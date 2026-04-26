import type { Metadata } from "@kronos-ts/common"
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
 * **Known limitation:** Unlike Java's dispatch interceptor which receives a
 * `proceed()` function and can choose not to call it (returning an alternative
 * result), this TypeScript model uses a simpler transform-or-throw approach.
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
 * Aligned with AF5's `MessageHandlerInterceptor` which receives
 * `(message, chain)`. UoW-scoped state is read/written via module-level ALS
 * accessors (`getResource` / `setResource`) — no `ProcessingContext`
 * parameter is threaded.
 *
 * The `next` function calls the next interceptor in the chain, or the
 * actual handler if this is the last interceptor.
 *
 * To skip handling entirely, don't call `next()` and return a result directly.
 */
export interface HandlerInterceptor<R = unknown> {
  (message: Message, next: () => Promise<R>): Promise<R>
}
