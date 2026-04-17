import type { QueryHandlerRegistration } from "./handler.js"

/**
 * A named group of query handlers.
 */
export interface QueryHandlersDefinition {
  readonly kind: "query-handlers"
  readonly name: string
  readonly handlers: ReadonlyArray<QueryHandlerRegistration<any, any>>
}

/**
 * Defines a group of query handlers.
 *
 * ```
 * queryHandlers({
 *   name: "course-queries",
 *   handlers: [
 *     on(GetCourse, async (query) => {
 *       return await db.courses.findById(query.courseId)
 *     }),
 *   ],
 * })
 * ```
 */
export function queryHandlers(def: {
  name: string
  handlers: QueryHandlerRegistration<any, any>[]
}): QueryHandlersDefinition {
  return { kind: "query-handlers", ...def }
}
