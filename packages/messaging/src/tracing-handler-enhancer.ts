import type { HandlerEnhancerDefinition, HandlerMetadata } from "./handler-enhancer.js"
import type { SpanFactory } from "./span-factory.js"

/**
 * Handler enhancer that wraps message handler invocations with tracing spans.
 *
 * Creates an internal span per handler invocation, recording the handler
 * name and message type as context. Errors are recorded on the span.
 *
 */
export function tracingHandlerEnhancerDefinition(
  spanFactory: SpanFactory,
): HandlerEnhancerDefinition {
  return {
    wrapHandler<T extends (...args: any[]) => any>(
      handler: T,
      metadata: HandlerMetadata,
    ): T {
      const spanName = `${metadata.handlerGroup}.${metadata.messageName}`

      return (async (...args: any[]) => {
        const span = spanFactory.createInternalSpan(spanName).start()
        try {
          const result = await handler(...args)
          span.end()
          return result
        } catch (error) {
          span.recordException(error instanceof Error ? error : new Error(String(error)))
          throw error
        }
      }) as unknown as T
    },
  }
}
