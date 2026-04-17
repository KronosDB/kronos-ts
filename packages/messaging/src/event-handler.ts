import type { EventHandlerRegistration } from "./handler.js"

/**
 * A named group of event handlers — can be a simple projection,
 * a process manager (if it uses state + commands), or anything in between.
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
 */
export function eventHandlers(def: {
  name: string
  handlers: EventHandlerRegistration<any>[]
  sequencedBy?: (event: unknown) => unknown
  onReset?: () => Promise<void> | void
}): EventHandlersDefinition {
  return { kind: "event-handlers", ...def }
}
