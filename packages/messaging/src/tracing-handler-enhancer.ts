import type { HandlerEnhancerDefinition, HandlerMetadata } from "./handler-enhancer.js"
import type { SpanFactory, Span } from "./span-factory.js"
import type { Message } from "./message.js"
import { contributeCorrelationData } from "./correlation-data.js"

/**
 * Handler enhancer that wraps message handler invocations with tracing spans.
 *
 * The span is created from the message being handled (extracting any trace
 * context from its metadata) so the handler re-parents onto the dispatcher's
 * trace across the message boundary. Event handlers start a new trace linked to
 * the triggering event; command/query handlers continue the current trace.
 *
 * The handler runs inside the span's active context, and the active trace
 * context is captured onto the UnitOfWork (via contributeCorrelationData) so
 * appended and dispatched messages carry it — including events published at
 * commit time, after the span has ended.
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
        const message = args[0]
        const span = createSpan(spanFactory, spanName, message, metadata).start()
        const runActive: <R>(fn: () => R) => R = span.runActive
          ? span.runActive.bind(span)
          : (fn) => fn()

        try {
          const result = await runActive(() => {
            // Store the active trace context on the UnitOfWork so outgoing and
            // appended messages carry it. Best-effort: tracing must never break
            // handling, and there may be no active UnitOfWork.
            try {
              const traceContext = spanFactory.currentTraceContext?.()
              if (traceContext && Object.keys(traceContext).length > 0) {
                contributeCorrelationData(traceContext)
              }
            } catch {
              // no active UnitOfWork or no tracing context — skip
            }
            return handler(...args)
          })
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

function isMessage(value: unknown): value is Message {
  return (
    typeof value === "object" &&
    value !== null &&
    "metadata" in value &&
    "identifier" in value
  )
}

function createSpan(
  spanFactory: SpanFactory,
  spanName: string,
  message: unknown,
  metadata: HandlerMetadata,
): Span {
  if (!isMessage(message)) {
    // No message to re-parent from (defensive — wired handlers always receive one).
    return spanFactory.createInternalSpan(spanName)
  }
  if (metadata.messageType === "event" && spanFactory.createLinkedHandlerSpan) {
    return spanFactory.createLinkedHandlerSpan(spanName, message)
  }
  return spanFactory.createHandlerSpan(spanName, message)
}
