import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { DispatchInterceptor, HandlerInterceptor } from "./interceptor.js"

/**
 * A command bus decorator that adds dispatch and handler interceptor chains
 * to any {@link CommandBus} implementation.
 *
 * {@link createSimpleCommandBus} handles dispatch + subscribe only;
 * this decorator layers interceptor support on top.
 */
export function createInterceptingCommandBus(
  delegate: CommandBus,
): CommandBus & {
  /** Register a dispatch interceptor. Returns an unsubscribe function. */
  registerDispatchInterceptor(interceptor: DispatchInterceptor<CommandMessage>): () => void
  /** Register a handler interceptor. Returns an unsubscribe function. */
  registerHandlerInterceptor(interceptor: HandlerInterceptor<CommandMessage>): () => void
} {
  const dispatchInterceptors: Array<DispatchInterceptor<CommandMessage>> = []
  const handlerInterceptors: Array<HandlerInterceptor<CommandMessage>> = []

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      // Run dispatch interceptors (can transform or reject)
      let interceptedMessage = message
      for (const interceptor of dispatchInterceptors) {
        interceptedMessage = await interceptor(interceptedMessage)
      }

      return delegate.dispatch(interceptedMessage)
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage) => Promise<unknown>,
    ) {
      const wrappedHandler = (message: CommandMessage) => {
        if (handlerInterceptors.length === 0) {
          return handler(message)
        }

        let chain = (currentMessage: CommandMessage) => handler(currentMessage)
        for (let i = handlerInterceptors.length - 1; i >= 0; i--) {
          const interceptor = handlerInterceptors[i]!
          const next = chain
          chain = (currentMessage: CommandMessage) =>
            interceptor(currentMessage, (replacementMessage) =>
              next(replacementMessage ?? currentMessage))
        }

        return chain(message)
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
