import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type { QueryDescriptor } from "./descriptor.js"

// ---------------------------------------------------------------------------
// Singular factory — mirrors commandHandler / eventHandler.
// The app consumes these via `app.queries(...handlers)` varargs.
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
