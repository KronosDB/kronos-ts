import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type { QueryDescriptor } from "./descriptor.js"
import type { QueryHandlerRegistration } from "./handler.js"

/**
 * A named group of query handlers.
 *
 * @deprecated Plan 11-04 deletes this in favour of the singular {@link queryHandler}
 * factory + `app.queries(...handlers)` varargs. Kept transitionally in Plan 11-01
 * so the tree stays green; do NOT add new callers.
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
 *
 * @deprecated Plan 11-04 deletes this in favour of the singular {@link queryHandler}
 * factory + `app.queries(...handlers)` varargs. Kept transitionally in Plan 11-01
 * so the tree stays green; do NOT add new callers.
 */
export function queryHandlers(def: {
  name: string
  handlers: QueryHandlerRegistration<any, any>[]
}): QueryHandlersDefinition {
  return { kind: "query-handlers", ...def }
}

// ---------------------------------------------------------------------------
// Singular factory (Plan 11-01) — mirrors commandHandler / eventHandler.
// The app consumes these via `app.queries(...handlers)` varargs; see Plan 11-03
// for the call-site migration and Plan 11-04 for the grouped-form deletion that
// completes the symmetric API.
// ---------------------------------------------------------------------------

/**
 * A registered singular query handler — pairs a query descriptor with its handler
 * function. Mirrors {@link import("./command-handler.js").CommandHandlerDefinition}
 * structurally so all three handler shapes (command / event / query) share the same
 * pattern.
 *
 * Note: queries do NOT carry a result schema on the descriptor (see `QueryDescriptor`
 * in `./descriptor.ts` — no `result` field used here). The result type `R` is
 * inferred from the handler's return type.
 *
 * Note: `kind: "query-handler"` overlaps with
 * {@link import("./handler.js").QueryHandlerRegistration} on purpose — Plan 11-04
 * collapses the two once the grouped {@link QueryHandlersDefinition} disappears.
 */
export interface QueryHandlerDefinition<
  Q extends z.ZodType = z.ZodType,
  R = unknown,
> {
  readonly kind: "query-handler"
  readonly descriptor: QueryDescriptor<Q>
  readonly handler: (query: z.infer<Q>, metadata: Metadata) => Promise<R> | R
}

/**
 * Defines a singular query handler.
 *
 * ```
 * const getCourseView = queryHandler(GetCourseView, async (q, metadata) => {
 *   const view = courseViews.get(q.courseId)
 *   if (!view) throw new Error("not found")
 *   return view
 * })
 * ```
 *
 * Use with `app.queries(getCourseView, getAllCourses)`. Symmetric to
 * {@link import("./command-handler.js").commandHandler} and
 * {@link import("./event-handler.js").eventHandler}.
 */
export function queryHandler<Q extends z.ZodType, R>(
  descriptor: QueryDescriptor<Q>,
  handler: (query: z.infer<Q>, metadata: Metadata) => Promise<R> | R,
): QueryHandlerDefinition<Q, R> {
  return { kind: "query-handler", descriptor, handler }
}
