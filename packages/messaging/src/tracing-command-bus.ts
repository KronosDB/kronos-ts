import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { SpanFactory } from "./span-factory.js"

/**
 * A {@link CommandBus} decorator that wraps dispatch and handler invocations
 * with tracing spans.
 *
 * Dispatch creates a "dispatch" span and propagates trace context into the
 * message metadata. Subscribe wraps each handler with a "handle" span.
 *
 * Aligned with AF5's `TracingCommandBus`.
 *
 * @param delegate  The underlying command bus to decorate.
 * @param spanFactory  The span factory for creating tracing spans.
 * @returns A decorated command bus with tracing instrumentation.
 */
export function createTracingCommandBus(
  delegate: CommandBus,
  spanFactory: SpanFactory,
): CommandBus {
  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const span = spanFactory.createDispatchSpan(`dispatch(${String(message.name)})`, message).start()
      try {
        const propagated = spanFactory.propagateContext(message)
        const result = await delegate.dispatch(propagated)
        span.end()
        return result
      } catch (err) {
        span.recordException(err instanceof Error ? err : new Error(String(err)))
        throw err
      }
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage) => Promise<unknown>,
    ): void {
      delegate.subscribe(commandName, async (msg: CommandMessage) => {
        const span = spanFactory.createHandlerSpan(`handle(${commandName})`, msg).start()
        try {
          const result = await handler(msg)
          span.end()
          return result
        } catch (err) {
          span.recordException(err instanceof Error ? err : new Error(String(err)))
          throw err
        }
      })
    },
  }
}
