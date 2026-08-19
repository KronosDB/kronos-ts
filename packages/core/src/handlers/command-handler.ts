import type { z } from "zod"
import type { CommandDescriptor } from "../messages/descriptor.js"
import type { EventQuery } from "../query/event-query.js"
import type { CommandMessage } from "../messages/message.js"
import type { HandlerContext } from "./handler-context.js"

/**
 * A registered command handler — pairs a command descriptor with its handler function.
 * When the descriptor has a result schema, the handler must return that type.
 */
export interface CommandHandlerDefinition<
  P extends z.ZodType = z.ZodType,
  R extends z.ZodType | undefined = undefined,
  C extends HandlerContext = HandlerContext,
> {
  readonly kind: "command-handler"
  readonly descriptor: CommandDescriptor<P, R>
  /**
   * `C` is the context this handler REQUIRES. It defaults to the framework's
   * {@link HandlerContext}; an adapter's handler wrapper (`drizzleHandler(handler, db)`)
   * takes a handler FUNCTION asking for its own richer context and returns one
   * asking only for the base, having supplied the difference. The host spreads
   * the entry — `{ ...h, handler: drizzleHandler(h.handler, db) }` — which is what lets
   * a slice write `ctx: DrizzleContext` and still compose into `kronos`.
   */
  readonly handler: (
    message: CommandMessage<z.infer<P>>,
    context: C,
  ) => R extends z.ZodType ? Promise<z.infer<R>> | z.infer<R> : Promise<void> | void
  readonly appendCondition?: (
    message: CommandMessage<z.infer<P>>,
    sourcedQuery: EventQuery,
  ) => EventQuery
}

/**
 * Defines a command handler.
 *
 * The handler receives the command message and a {@link HandlerContext} — the
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
export function commandHandler<P extends z.ZodType, C extends HandlerContext = HandlerContext>(
  descriptor: CommandDescriptor<P, undefined>,
  handler: (message: CommandMessage<z.infer<P>>, context: C) => Promise<void> | void,
): CommandHandlerDefinition<P, undefined, C>

export function commandHandler<
  P extends z.ZodType,
  R extends z.ZodType,
  C extends HandlerContext = HandlerContext,
>(
  descriptor: CommandDescriptor<P, R>,
  handler: (message: CommandMessage<z.infer<P>>, context: C) => Promise<z.infer<R>> | z.infer<R>,
): CommandHandlerDefinition<P, R, C>

export function commandHandler<P extends z.ZodType, C extends HandlerContext = HandlerContext>(
  descriptor: CommandDescriptor<P, undefined>,
  options: {
    handler: (message: CommandMessage<z.infer<P>>, context: C) => Promise<void> | void
    appendCondition?: (
      message: CommandMessage<z.infer<P>>,
      sourcedQuery: EventQuery,
    ) => EventQuery
  },
): CommandHandlerDefinition<P, undefined, C>

export function commandHandler<
  P extends z.ZodType,
  R extends z.ZodType,
  C extends HandlerContext = HandlerContext,
>(
  descriptor: CommandDescriptor<P, R>,
  options: {
    handler: (message: CommandMessage<z.infer<P>>, context: C) => Promise<z.infer<R>> | z.infer<R>
    appendCondition?: (
      message: CommandMessage<z.infer<P>>,
      sourcedQuery: EventQuery,
    ) => EventQuery
  },
): CommandHandlerDefinition<P, R, C>

export function commandHandler(
  descriptor: CommandDescriptor,
  handlerOrOptions: any,
): CommandHandlerDefinition {
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
