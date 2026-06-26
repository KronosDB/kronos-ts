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
  /**
   * Run `fn` with this span set as the active trace context, returning `fn`'s
   * result. Spans created — and trace context read (`propagateContext` /
   * `currentTraceContext`) — inside `fn` are parented to this span. When `fn`
   * is async the span stays active across its awaits.
   *
   * Optional: callers must fall back to invoking `fn` directly when a Span
   * implementation doesn't provide it.
   */
  runActive?<T>(fn: () => T): T
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
   * Extracts trace context from the parent message's metadata and continues
   * that trace (the new span is a child in the same trace).
   */
  createHandlerSpan(operationName: string, parentMessage: Message): Span

  /**
   * Like {@link createHandlerSpan}, but starts a NEW trace linked to the parent
   * message's trace context instead of continuing it. Use for asynchronously
   * handled messages (e.g. streaming/tracking event processors) where joining a
   * possibly long-finished originating trace would be misleading — the link
   * preserves correlation without false nesting.
   *
   * Optional: callers fall back to {@link createHandlerSpan} when absent.
   */
  createLinkedHandlerSpan?(operationName: string, parentMessage: Message): Span

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

  /**
   * Returns the currently active trace context as propagation headers (e.g. the
   * W3C `traceparent`), or an empty object when no span is active. Used to store
   * the handler's trace context on the UnitOfWork (via `contributeCorrelationData`)
   * so it rides along on appended and dispatched messages — including those
   * published at commit time, after the handler span has ended.
   *
   * Optional: callers treat absence as "no trace context".
   */
  currentTraceContext?(): Record<string, string>

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
    runActive<T>(fn: () => T): T { return fn() },
  }

  return {
    createRootTrace() { return noOpSpan },
    createHandlerSpan() { return noOpSpan },
    createLinkedHandlerSpan() { return noOpSpan },
    createDispatchSpan() { return noOpSpan },
    createInternalSpan() { return noOpSpan },
    propagateContext<M extends Message>(message: M) { return message },
    currentTraceContext() { return {} },
    registerSpanAttributeProvider() {},
  }
}
