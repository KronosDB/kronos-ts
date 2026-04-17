import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { SubscriptionQueryResult } from "./subscription-query.js"
import type { DispatchInterceptor, HandlerInterceptor } from "./interceptor.js"

/**
 * A query bus decorator that adds dispatch and handler interceptor chains
 * to any {@link QueryBus} implementation.
 *
 * This follows Java's pattern of separating interceptor support from the
 * base bus. {@link createSimpleQueryBus} handles dispatch + subscribe only;
 * this decorator layers interceptor support on top.
 *
 * Aligned with AF5's `InterceptingQueryBus`.
 */
export function createInterceptingQueryBus(
  delegate: QueryBus,
): QueryBus & {
  /** Register a dispatch interceptor. Returns an unsubscribe function. */
  registerDispatchInterceptor(interceptor: DispatchInterceptor<QueryMessage>): () => void
  /** Register a handler interceptor. Returns an unsubscribe function. */
  registerHandlerInterceptor(interceptor: HandlerInterceptor): () => void
} {
  const dispatchInterceptors: Array<DispatchInterceptor<QueryMessage>> = []
  const handlerInterceptors: Array<HandlerInterceptor> = []

  return {
    async query(message: QueryMessage, context?: ProcessingContext): Promise<unknown> {
      let interceptedMessage = message
      for (const interceptor of dispatchInterceptors) {
        interceptedMessage = await interceptor(interceptedMessage, context)
      }

      return delegate.query(interceptedMessage, context)
    },

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, ctx: ProcessingContext) => Promise<unknown>,
    ) {
      // Wrap the handler with handler interceptors
      const wrappedHandler = (message: QueryMessage, ctx: ProcessingContext) => {
        if (handlerInterceptors.length === 0) {
          return handler(message, ctx)
        }

        let chain = () => handler(message, ctx)
        for (let i = handlerInterceptors.length - 1; i >= 0; i--) {
          const interceptor = handlerInterceptors[i]!
          const next = chain
          chain = () => interceptor(message, ctx, next)
        }

        return chain()
      }

      delegate.subscribe(queryName, wrappedHandler)
    },

    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      return delegate.subscriptionQuery(message, bufferSize)
    },

    subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      return delegate.subscribeToUpdates(message, bufferSize)
    },

    emitUpdate(
      queryName: string,
      filter: (queryPayload: unknown) => boolean,
      update: unknown,
      context?: ProcessingContext,
    ): Promise<void> {
      return delegate.emitUpdate(queryName, filter, update, context)
    },

    completeSubscription(
      queryName: string,
      filter?: (queryPayload: unknown) => boolean,
      context?: ProcessingContext,
    ): Promise<void> {
      return delegate.completeSubscription(queryName, filter, context)
    },

    completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: (queryPayload: unknown) => boolean,
      context?: ProcessingContext,
    ): Promise<void> {
      return delegate.completeSubscriptionExceptionally(queryName, error, filter, context)
    },

    registerDispatchInterceptor(interceptor) {
      dispatchInterceptors.push(interceptor)
      return () => {
        const idx = dispatchInterceptors.indexOf(interceptor)
        if (idx >= 0) dispatchInterceptors.splice(idx, 1)
      }
    },

    registerHandlerInterceptor(interceptor) {
      handlerInterceptors.push(interceptor)
      return () => {
        const idx = handlerInterceptors.indexOf(interceptor)
        if (idx >= 0) handlerInterceptors.splice(idx, 1)
      }
    },
  }
}
