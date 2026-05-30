import type { Message } from "./message.js"

/**
 * Intercepts a message before it reaches any handler.
 * Can transform, enrich, or reject messages.
 *
 * Dispatch interceptors run in order — each receives the (possibly transformed)
 * message from the previous interceptor.
 *
 * To reject a message, throw an error.
 * To transform, return a new message.
 * To pass through, return the message unchanged.
 *
 * UoW-scoped state (correlation data, tracing context, etc.) is read directly
 * from ALS via module-level accessors (`getResource` / `setResource`) — no
 * `ProcessingContext` parameter is threaded.
 *
 * **Known limitation:** Dispatch interceptors use a simple transform-or-throw
 * approach. They do not receive a `proceed()` function that can be skipped to
 * return an alternative result.
 * A dispatch interceptor cannot halt the chain and return an alternative result
 * — it can only transform the message or throw to reject it. This covers ~95%
 * of use cases. Full chain semantics (with `proceed()`) would require the
 * dispatch interceptor to also control the result type, which is a much larger
 * refactor. If you need to conditionally prevent dispatch, throw a typed error
 * and handle it at the call site.
 */
export interface DispatchInterceptor<M extends Message = Message> {
  (message: M): M | Promise<M>
}

/**
 * Intercepts a handler invocation. Wraps the actual handler call,
 * enabling before/after logic (transactions, tracing, metrics, etc.)
 *
 * UoW-scoped state is read/written via module-level ALS
 * accessors (`getResource` / `setResource`) — no `ProcessingContext`
 * parameter is threaded.
 *
 * The first argument is the full message object. Prefer keeping it as
 * `message` when transforming or inspecting broad message details:
 *
 * ```
 * app.handlerInterceptor(async (message, next) => {
 *   const { payload, metadata, timestamp } = message
 *   return next({
 *     ...message,
 *     metadata: { ...metadata, tenantId: "tenant-1" },
 *   })
 * })
 * ```
 *
 * The `next` function calls the next interceptor in the chain, or the
 * actual handler if this is the last interceptor. Call `next()` to proceed
 * with the current message, or `next(replacementMessage)` to proceed with a
 * transformed message.
 *
 * To skip handling entirely, don't call `next()` and return a result directly.
 */
export interface HandlerInterceptor<
  M extends Message = Message,
  R = unknown,
> {
  (message: M, next: (message?: M) => Promise<R>): Promise<R>
}
