import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import { type EventDescriptor, type SequencedEventMessage } from "../messaging/messages.js"
import type { EventHandlerContext } from "./context.js"

// ---------------------------------------------------------------------------
// Singular factory — mirrors commandHandler / queryHandler.
// The processor builder consumes these via `.eventHandlers(...handlers)` varargs.
// ---------------------------------------------------------------------------

/**
 * A registered singular event handler — pairs an event descriptor with its handler
 * function. Mirrors {@link import("../command-handling/handler.js").CommandHandler}
 * structurally so all three handler shapes (command / event / query) share the same
 * pattern.
 */
export type EventHandler<
  P extends StandardSchemaV1 = StandardSchemaV1,
  C extends EventHandlerContext = EventHandlerContext,
> = {
  readonly kind: "event-handler"
  readonly descriptor: EventDescriptor<P>
  /**
   * `C` is the context this handler REQUIRES — see the note on
   * {@link import("../command-handling/handler.js").CommandHandler}. An
   * adapter's `drizzleHandler(handler, db)` supplies the difference between its own
   * context and the base one.
   */
  readonly handler: (
    message: SequencedEventMessage<InferOutput<P>>,
    context: C,
  ) => Promise<void> | void
}

/**
 * Defines a singular event handler.
 *
 * The handler receives the sequenced event and an {@link EventHandlerContext}
 * (`load`, `send`, `query`, `emitUpdate`, `unitOfWork` — no `append`: processor
 * UnitOfWorks flush no event buffer, so automations that produce events
 * dispatch a command via `ctx.send` instead).
 *
 * A projection writes through its adapter's accessor, handing it the unit of
 * work off the context. `activeDrizzleTransaction` OBSERVES — it returns the
 * batch's transaction when one is open and never provokes one — so the same
 * handler works whether or not the processor was given a transactional factory:
 *
 * ```
 * const onCourseCreated = eventHandler(CourseCreated, async ({ payload, timestamp }, ctx) => {
 *   const write = activeDrizzleTransaction(ctx.unitOfWork) ?? db
 *   await insertCourseRow(write, { id: payload.courseId, name: payload.name, createdAt: timestamp })
 * })
 * ```
 *
 * Use with `trackingProcessor(...).eventHandlers(onCreated, onCapChanged).build()` or
 * `subscribingProcessor(...).eventHandlers(...).build()`. Symmetric to
 * {@link import("../command-handling/handler.js").commandHandler} and
 * {@link import("../query-handling/handler.js").queryHandler}.
 */
export function eventHandler<
  P extends StandardSchemaV1,
  C extends EventHandlerContext = EventHandlerContext,
>(
  descriptor: EventDescriptor<P>,
  handler: (message: SequencedEventMessage<InferOutput<P>>, context: C) => Promise<void> | void,
): EventHandler<P, C> {
  return { kind: "event-handler", descriptor, handler }
}
