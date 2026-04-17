import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { DispatchInterceptor, HandlerInterceptor } from "./interceptor.js"

/**
 * A command bus decorator that adds dispatch and handler interceptor chains
 * to any {@link CommandBus} implementation.
 *
 * This follows Java's pattern of separating interceptor support from the
 * base bus. {@link createSimpleCommandBus} handles dispatch + subscribe only;
 * this decorator layers interceptor support on top.
 *
 * Aligned with AF5's `InterceptingCommandBus`.
 */
export function createInterceptingCommandBus(
  delegate: CommandBus,
): CommandBus & {
  /** Register a dispatch interceptor. Returns an unsubscribe function. */
  registerDispatchInterceptor(interceptor: DispatchInterceptor<CommandMessage>): () => void
  /** Register a handler interceptor. Returns an unsubscribe function. */
  registerHandlerInterceptor(interceptor: HandlerInterceptor): () => void
} {
  const dispatchInterceptors: Array<DispatchInterceptor<CommandMessage>> = []
  const handlerInterceptors: Array<HandlerInterceptor> = []

  return {
    async dispatch(message: CommandMessage, context?: ProcessingContext): Promise<unknown> {
      // Run dispatch interceptors (can transform or reject)
      let interceptedMessage = message
      for (const interceptor of dispatchInterceptors) {
        interceptedMessage = await interceptor(interceptedMessage, context)
      }

      // If we have handler interceptors, wrap the delegate in a handler chain.
      // Handler interceptors are applied via a wrapping subscribe that the
      // delegate already called. Since the delegate creates UoW internally,
      // we wrap the dispatch to inject handler interceptors.
      if (handlerInterceptors.length > 0) {
        // We temporarily wrap the delegate's subscribe to inject interceptors.
        // This is done by subscribing a wrapping handler on the fly.
        // However, a cleaner approach: we store interceptors and the configurer
        // wires them on the delegate when it supports them.
        //
        // For now, delegate dispatch with intercepted message and rely on
        // the underlying bus to handle the handler interceptor chain if it
        // supports it. If the delegate has registerHandlerInterceptor, the
        // interceptors should have been registered there.
      }

      return delegate.dispatch(interceptedMessage, context)
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>,
    ) {
      // Wrap the handler with handler interceptors
      const wrappedHandler = (message: CommandMessage, ctx: ProcessingContext) => {
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

      delegate.subscribe(commandName, wrappedHandler)
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
