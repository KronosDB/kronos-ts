import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type { EventDescriptor } from "./descriptor.js"

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
    event: z.infer<P>,
    metadata: Metadata,
  ) => Promise<void> | void
}

/**
 * Defines a singular event handler.
 *
 * ```
 * const onCourseCreated = eventHandler(CourseCreated, async (event, metadata) => {
 *   await db.courses.insert({ id: event.courseId, name: event.name })
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
  handler: (event: z.infer<P>, metadata: Metadata) => Promise<void> | void,
): EventHandlerDefinition<P> {
  return { kind: "event-handler", descriptor, handler }
}
