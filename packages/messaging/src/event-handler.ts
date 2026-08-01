import type { z } from "zod"
import type { EventDescriptor } from "./descriptor.js"
import type { SequencedEventMessage } from "./message.js"
import type { EventHandlerContext } from "./handler-context.js"

// ---------------------------------------------------------------------------
// Singular factory — mirrors commandHandler / queryHandler.
// The processor builder consumes these via `.eventHandlers(...handlers)` varargs.
// ---------------------------------------------------------------------------

/**
 * A registered singular event handler — pairs an event descriptor with its handler
 * function. Mirrors {@link import("./command-handler.js").CommandHandlerDefinition}
 * structurally so all three handler shapes (command / event / query) share the same
 * pattern.
 */
export interface EventHandlerDefinition<P extends z.ZodType = z.ZodType> {
  readonly kind: "event-handler"
  readonly descriptor: EventDescriptor<P>
  readonly handler: (
    message: SequencedEventMessage<z.infer<P>>,
    context: EventHandlerContext,
  ) => Promise<void> | void
}

/**
 * Defines a singular event handler.
 *
 * The handler receives the sequenced event and an {@link EventHandlerContext}
 * (`load`, `send`, `emitUpdate`, `transaction` — no `append`: processor
 * UnitOfWorks flush no event buffer, so automations that produce events
 * dispatch a command via `ctx.send` instead).
 *
 * ```
 * const onCourseCreated = eventHandler(CourseCreated, async ({ payload, timestamp }, ctx) => {
 *   const tx = await ctx.transaction<Db>()
 *   await insertCourseRow(tx, { id: payload.courseId, name: payload.name, createdAt: timestamp })
 * })
 * ```
 *
 * Use with `trackingProcessor(...).eventHandlers(onCreated, onCapChanged).build()` or
 * `subscribingProcessor(...).eventHandlers(...).build()`. Symmetric to
 * {@link import("./command-handler.js").commandHandler} and
 * {@link import("./query-handler.js").queryHandler}.
 */
export function eventHandler<P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  handler: (message: SequencedEventMessage<z.infer<P>>, context: EventHandlerContext) => Promise<void> | void,
): EventHandlerDefinition<P> {
  return { kind: "event-handler", descriptor, handler }
}
