import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { SpanFactory, Span } from "./span-factory.js"

/** Run `fn` with `span` active, falling back to a plain call when the span lacks runActive. */
function runActive<R>(span: Span, fn: () => R): R {
  return span.runActive ? span.runActive(fn) : fn()
}

/**
 * A {@link CommandBus} decorator that traces command dispatch (the producer
 * side).
 *
 * Dispatch creates a "dispatch" span and propagates trace context into the
 * message metadata, so the handler links back to it across the bus boundary.
 * The handler ("handle") span is created by {@link tracingHandlerEnhancerDefinition},
 * the single authority for handler-side spans across command/query/event
 * handlers — so this decorator does not wrap subscribe, avoiding a duplicate
 * command handle span.
 *
 * @param delegate  The underlying command bus to decorate.
 * @param spanFactory  The span factory for creating tracing spans.
 * @returns A decorated command bus with dispatch tracing.
 */
export function tracingCommandBus(
  delegate: CommandBus,
  spanFactory: SpanFactory,
): CommandBus {
  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const span = spanFactory.createDispatchSpan(`dispatch(${String(message.name)})`, message).start()
      try {
        // Propagate inside the active dispatch span so the outgoing message
        // carries this span's trace context and the handler links to it.
        const result = await runActive(span, () => {
          const propagated = spanFactory.propagateContext(message)
          return delegate.dispatch(propagated)
        })
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
      // Handler-side spans are owned by tracingHandlerEnhancerDefinition; pass
      // the handler through untouched so commands get exactly one handle span.
      delegate.subscribe(commandName, handler)
    },
  }
}
