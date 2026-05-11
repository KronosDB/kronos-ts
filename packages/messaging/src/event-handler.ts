import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type { EventDescriptor } from "./descriptor.js"
import type { EventHandlerRegistration } from "./handler.js"

/**
 * A named group of event handlers — can be a simple projection,
 * a process manager (if it uses state + commands), or anything in between.
 *
 * @deprecated Plan 11-04 deletes this in favour of the singular {@link eventHandler}
 * factory + processor-builder varargs. Kept transitionally in Plan 11-01 so the
 * tree stays green; do NOT add new callers.
 */
export interface EventHandlersDefinition {
  readonly kind: "event-handlers"
  readonly name: string
  readonly handlers: ReadonlyArray<EventHandlerRegistration<any>>
  readonly sequencedBy?: (event: unknown) => unknown
  readonly onReset?: () => Promise<void> | void
}

/**
 * Defines a group of event handlers.
 *
 * ```
 * eventHandlers({
 *   name: "course-projection",
 *   handlers: [
 *     on(CourseCreated, async (event) => {
 *       await db.courses.insert({ id: event.courseId, name: event.name })
 *     }),
 *   ],
 * })
 * ```
 *
 * @deprecated Plan 11-04 deletes this in favour of the singular {@link eventHandler}
 * factory + processor-builder varargs. Kept transitionally in Plan 11-01 so the
 * tree stays green; do NOT add new callers.
 */
export function eventHandlers(def: {
  name: string
  handlers: EventHandlerRegistration<any>[]
  sequencedBy?: (event: unknown) => unknown
  onReset?: () => Promise<void> | void
}): EventHandlersDefinition {
  return { kind: "event-handlers", ...def }
}

// ---------------------------------------------------------------------------
// Singular factory (Plan 11-01) — mirrors commandHandler / queryHandler.
// The processor builder consumes these via `.eventHandlers(...handlers)` varargs;
// see Plan 11-02 for the builder migration and Plan 11-04 for the grouped-form
// deletion that completes the symmetric API.
// ---------------------------------------------------------------------------

/**
 * A registered singular event handler — pairs an event descriptor with its handler
 * function. Mirrors {@link import("./command-handler.js").CommandHandlerDefinition}
 * structurally so all three handler shapes (command / event / query) share the same
 * pattern.
 *
 * Note: `kind: "event-handler"` overlaps with
 * {@link import("./handler.js").EventHandlerRegistration} on purpose — Plan 11-04
 * collapses the two once the grouped {@link EventHandlersDefinition} disappears.
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
