import type { Message } from "./message.js"

/**
 * A span representing a unit of tracing work.
 * Start the span, do work, then end it.
 *
 */
export interface Span {
  /** Start the span. Returns the span for chaining. */
  start(): Span
  /** End the span normally. */
  end(): void
  /** Record an error on the span and end it. */
  recordException(error: Error): void
}

/**
 * Provides custom attributes to add to spans.
 *
 */
export interface SpanAttributesProvider {
  provideAttributes(message: Message): Record<string, string>
}

/**
 * Factory for creating tracing spans around message processing.
 *
 * The SpanFactory is the core tracing abstraction. Implementations
 * (e.g., OpenTelemetry) provide the actual span creation and context
 * propagation. The framework calls these methods during dispatch and
 * handling.
 *
 */
export interface SpanFactory {
  /** Create a root trace span (no parent). */
  createRootTrace(operationName: string): Span

  /**
   * Create a span for handling a message (consumer side).
   * Extracts trace context from the parent message's metadata.
   */
  createHandlerSpan(operationName: string, parentMessage: Message): Span

  /**
   * Create a span for dispatching a message (producer side).
   * Links to the parent message's trace context.
   */
  createDispatchSpan(operationName: string, parentMessage: Message): Span

  /** Create a span for internal framework operations. */
  createInternalSpan(operationName: string): Span

  /**
   * Inject trace context into a message's metadata so it propagates
   * across message boundaries.
   */
  propagateContext<M extends Message>(message: M): M

  /** Register a custom span attribute provider. */
  registerSpanAttributeProvider(provider: SpanAttributesProvider): void
}

/**
 * A no-op span factory. Default when no tracing is configured.
 */
export function noOpSpanFactory(): SpanFactory {
  const noOpSpan: Span = {
    start() { return this },
    end() {},
    recordException() {},
  }

  return {
    createRootTrace() { return noOpSpan },
    createHandlerSpan() { return noOpSpan },
    createDispatchSpan() { return noOpSpan },
    createInternalSpan() { return noOpSpan },
    propagateContext<M extends Message>(message: M) { return message },
    registerSpanAttributeProvider() {},
  }
}
