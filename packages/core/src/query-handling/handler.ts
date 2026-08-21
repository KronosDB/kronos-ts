import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import { type QueryDescriptor, type QueryMessage } from "../messaging/messages.js"
import type { QueryHandlerContext } from "./context.js"

// ---------------------------------------------------------------------------
// Singular factory — mirrors commandHandler / eventHandler.
// The app consumes these via `app.queries(...handlers)` varargs.
// ---------------------------------------------------------------------------

/**
 * A registered singular query handler — pairs a query descriptor with its handler
 * function. Mirrors {@link import("../command-handling/handler.js").CommandHandler}
 * structurally so all three handler shapes (command / event / query) share the same
 * pattern.
 *
 * Note: queries do NOT carry a result schema on the descriptor (see `QueryDescriptor`
 * in `./descriptor.ts` — no `result` field used here). The result type `R` is
 * inferred from the handler's return type.
 */
export type QueryHandler<
  Q extends StandardSchemaV1 = StandardSchemaV1,
  R = unknown,
  C extends QueryHandlerContext = QueryHandlerContext,
> = {
  readonly kind: "query-handler"
  readonly descriptor: QueryDescriptor<Q, StandardSchemaV1 | undefined>
  /**
   * `C` is the context this handler REQUIRES — see the note on
   * {@link import("../command-handling/handler.js").CommandHandler}. An
   * adapter's `drizzleHandler(handler, db)` supplies the difference between its own
   * context and the base one.
   */
  readonly handler: (
    message: QueryMessage<InferOutput<Q>>,
    context: C,
  ) => Promise<R> | R
}

/**
 * Defines a singular query handler.
 *
 * ```
 * const getCourseView = queryHandler(GetCourseView, async ({ payload, metadata }) => {
 *   const view = courseViews.get(payload.courseId)
 *   if (!view) throw new Error("not found")
 *   return view
 * })
 * ```
 *
 * Use with `app.queries(getCourseView, getAllCourses)`. Symmetric to
 * {@link import("../command-handling/handler.js").commandHandler} and
 * {@link import("../event-processing/handler.js").eventHandler}.
 */
export function queryHandler<
  Q extends StandardSchemaV1,
  R,
  C extends QueryHandlerContext = QueryHandlerContext,
>(
  descriptor: QueryDescriptor<Q, StandardSchemaV1 | undefined>,
  handler: (message: QueryMessage<InferOutput<Q>>, context: C) => Promise<R> | R,
): QueryHandler<Q, R, C> {
  return { kind: "query-handler", descriptor, handler }
}
