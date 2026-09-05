import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import { type CommandDescriptor, type CommandMessage } from "../messaging/messages.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { CommandHandlerContext } from "./context.js"

/**
 * A registered command handler — pairs a command descriptor with its handler function.
 * When the descriptor has a result schema, the handler must return that type.
 */
export type CommandHandler<
  P extends StandardSchemaV1 = StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = undefined,
  C extends CommandHandlerContext = CommandHandlerContext,
> = {
  readonly kind: "command-handler"
  readonly descriptor: CommandDescriptor<P, R>
  /**
   * `C` is the context this handler REQUIRES. It defaults to the framework's
   * {@link CommandHandlerContext}; an adapter's handler wrapper (`drizzleHandler(handler, db)`)
   * takes a handler FUNCTION asking for its own richer context and returns one
   * asking only for the base, having supplied the difference. The host spreads
   * the entry — `{ ...h, handler: drizzleHandler(h.handler, db) }` — which is what lets
   * a slice write `ctx: CommandHandlerContext & DrizzleCapability` and still compose into `kronos`.
   */
  readonly handler: (
    message: CommandMessage<InferOutput<P>>,
    context: C,
  ) => R extends StandardSchemaV1 ? Promise<InferOutput<R>> | InferOutput<R> : Promise<void> | void
  readonly appendCondition?: (
    message: CommandMessage<InferOutput<P>>,
    sourcedQuery: EventQuery,
  ) => EventQuery
}

/**
 * Defines a command handler.
 *
 * The handler receives the command message and a {@link CommandHandlerContext} — the
 * typed front door to the active UnitOfWork (`load`, `append`, `send`,
 * `emitUpdate`, `transaction`). Prefer the context over the module-level
 * helpers: it only exists inside a handler, so misuse is a compile error
 * rather than a runtime NoActiveUnitOfWork.
 *
 * Void command (no result on descriptor):
 * ```
 * commandHandler(ChangeCourseCapacity, async ({ payload, metadata }, ctx) => {
 *   const course = await ctx.load(Course, { courseId: payload.courseId })
 *   ctx.append(CourseCapacityChanged, { courseId: payload.courseId, capacity: payload.capacity })
 * })
 * ```
 *
 * Typed result command (result on descriptor):
 * ```
 * const CreateCourse = command({
 *   name: qn("university", "CreateCourse"),
 *   payload: z.object({ courseId: z.string(), name: z.string() }),
 *   result: z.object({ courseId: z.string() }),
 * })
 *
 * commandHandler(CreateCourse, async ({ payload, metadata }, ctx) => {
 *   ctx.append(CourseCreated, { ... })
 *   return { courseId: payload.courseId }  // ← must match descriptor's result schema
 * })
 * ```
 *
 * With append condition override:
 * ```
 * commandHandler(CreateCourse, {
 *   handler: async ({ payload, metadata }) => { ... },
 *   appendCondition: (command, query) => query,
 * })
 * ```
 */
export function commandHandler<P extends StandardSchemaV1, C extends CommandHandlerContext = CommandHandlerContext>(
  descriptor: CommandDescriptor<P, undefined>,
  handler: (message: CommandMessage<InferOutput<P>>, context: C) => Promise<void> | void,
): CommandHandler<P, undefined, C>

export function commandHandler<
  P extends StandardSchemaV1,
  R extends StandardSchemaV1,
  C extends CommandHandlerContext = CommandHandlerContext,
>(
  descriptor: CommandDescriptor<P, R>,
  handler: (message: CommandMessage<InferOutput<P>>, context: C) => Promise<InferOutput<R>> | InferOutput<R>,
): CommandHandler<P, R, C>

export function commandHandler<P extends StandardSchemaV1, C extends CommandHandlerContext = CommandHandlerContext>(
  descriptor: CommandDescriptor<P, undefined>,
  options: {
    handler: (message: CommandMessage<InferOutput<P>>, context: C) => Promise<void> | void
    appendCondition?: (
      message: CommandMessage<InferOutput<P>>,
      sourcedQuery: EventQuery,
    ) => EventQuery
  },
): CommandHandler<P, undefined, C>

export function commandHandler<
  P extends StandardSchemaV1,
  R extends StandardSchemaV1,
  C extends CommandHandlerContext = CommandHandlerContext,
>(
  descriptor: CommandDescriptor<P, R>,
  options: {
    handler: (message: CommandMessage<InferOutput<P>>, context: C) => Promise<InferOutput<R>> | InferOutput<R>
    appendCondition?: (
      message: CommandMessage<InferOutput<P>>,
      sourcedQuery: EventQuery,
    ) => EventQuery
  },
): CommandHandler<P, R, C>

export function commandHandler(
  descriptor: CommandDescriptor,
  handlerOrOptions: any,
): CommandHandler {
  if (typeof handlerOrOptions === "function") {
    return { kind: "command-handler", descriptor, handler: handlerOrOptions }
  }
  return {
    kind: "command-handler",
    descriptor,
    handler: handlerOrOptions.handler,
    appendCondition: handlerOrOptions.appendCondition,
  }
}
