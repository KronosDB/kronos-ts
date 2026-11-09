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
   * {@link CommandHandlerContext}; an adapter's handler wrapper (`postgresHandler(handler, pg)`)
   * takes a handler FUNCTION asking for its own richer context and returns one
   * asking only for the base, having supplied the difference. The host spreads
   * the entry — `{ ...h, handler: postgresHandler(h.handler, pg) }` — which is what lets
   * a slice write `ctx: CommandHandlerContext & PostgresCapability` and still compose into `kronos`.
   */
  readonly handler: (message: CommandMessage<InferOutput<P>>, context: C) => HandlerReturn<R>
  readonly appendCondition?: (
    message: CommandMessage<InferOutput<P>>,
    sourcedQuery: EventQuery,
  ) => EventQuery
}

/**
 * What a handler returns: the result schema's output, or nothing when the
 * descriptor declares no result. An array in the output may come back
 * readonly, because {@link commandHandler} reads the returned literals in a
 * const context (see there): `{ items: [id] }` is `readonly [string]`, and it is
 * still a `string[]` at runtime.
 */
type HandlerReturn<R extends StandardSchemaV1 | undefined> = [R] extends [StandardSchemaV1]
  ? Promise<Returned<InferOutput<NonNullable<R>>>> | Returned<InferOutput<NonNullable<R>>>
  : Promise<void> | void

/** An output as a handler may return it: the output itself, or it with its arrays readonly. */
type Returned<T> = T | ReadonlyArrays<T>

type ReadonlyArrays<T> = T extends readonly unknown[]
  ? { readonly [K in keyof T]: ReadonlyArrays<T[K]> }
  : T extends Date | ((...args: never[]) => unknown)
    ? T
    : T extends object
      ? { [K in keyof T]: ReadonlyArrays<T[K]> }
      : T

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
 * The returned literals are read against the result: `{ ok: true }` is
 * `{ ok: true }`, not `{ ok: boolean }`, with no `as const`. That is what `const R`
 * is for — without it TypeScript types the return before it knows the result,
 * and widens every literal in it. An array literal in the return comes back
 * readonly for the same reason, which the handler's return type allows.
 *
 * With append condition override:
 * ```
 * commandHandler(CreateCourse, {
 *   handler: async ({ payload, metadata }) => { ... },
 *   appendCondition: (command, query) => query,
 * })
 * ```
 */
export function commandHandler<
  P extends StandardSchemaV1,
  const R extends StandardSchemaV1 | undefined = undefined,
  C extends CommandHandlerContext = CommandHandlerContext,
>(
  descriptor: CommandDescriptor<P, R>,
  handler:
    | ((message: CommandMessage<InferOutput<P>>, context: C) => HandlerReturn<R>)
    | {
        handler: (message: CommandMessage<InferOutput<P>>, context: C) => HandlerReturn<R>
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
