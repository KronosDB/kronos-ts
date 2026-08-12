import type { DeadLetterListener, SpanFactory } from "@kronos-ts/messaging"
import {
  openTelemetrySpanFactory,
  type OpenTelemetrySpanFactoryOptions,
} from "./opentelemetry-span-factory.js"

/**
 * An OpenTelemetry {@link DeadLetterListener} that emits a short internal span
 * for each dead-letter lifecycle transition, recording the cause as a span
 * exception on failures and overflow (backpressure).
 *
 * Attach it to a processor via the builder:
 * ```typescript
 * trackingProcessor("balances")
 *   .deadLetterQueue(dlq)
 *   .deadLetterListener(openTelemetryDeadLetterListener())
 * ```
 *
 * Unlike {@link openTelemetry} (which decorates the command bus and enhances
 * handlers app-wide), the DLQ listener is attached per processor, so this is a
 * standalone factory. Pass a shared `spanFactory` to correlate with the rest of
 * your tracing, or let it build one from `options`.
 */
export function openTelemetryDeadLetterListener(
  options: OpenTelemetrySpanFactoryOptions = {},
  spanFactory: SpanFactory = openTelemetrySpanFactory(options),
): DeadLetterListener {
  const emit = (name: string, error?: Error) => {
    const span = spanFactory.createInternalSpan(name).start()
    // recordException ends the span; otherwise end it normally.
    if (error) span.recordException(error)
    else span.end()
  }
  return {
    onEnqueued(letter, info) {
      if (info.blocked) emit("dlq.blocked")
      else emit("dlq.enqueue", letter.cause)
    },
    onEvicted() {
      emit("dlq.evict")
    },
    onRequeued() {
      emit("dlq.requeue")
    },
    onReprocessSuccess() {
      emit("dlq.reprocess.success")
    },
    onReprocessFailure(_letter, cause) {
      emit("dlq.reprocess.failure", cause)
    },
    onOverflow(_sequenceIdentifier, cause) {
      emit("dlq.overflow", cause)
    },
  }
}
