import {
  trace,
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  type Tracer,
  type TextMapGetter,
  type TextMapSetter,
  type Context as OtelContext,
} from "@opentelemetry/api"
import type { Metadata } from "@kronos-ts/common"
import type { Message } from "@kronos-ts/messaging"
import type { Span, SpanFactory, SpanAttributesProvider } from "@kronos-ts/messaging"

// ---------------------------------------------------------------------------
// Metadata context propagation — inject/extract OTel context via message metadata
// ---------------------------------------------------------------------------

const metadataGetter: TextMapGetter<Message> = {
  keys(carrier) {
    return Object.keys(carrier.metadata)
  },
  get(carrier, key) {
    const value = carrier.metadata[key]
    return value != null ? String(value) : undefined
  },
}

const metadataSetter: TextMapSetter<Record<string, unknown>> = {
  set(carrier, key, value) {
    carrier[key] = value
  },
}

// ---------------------------------------------------------------------------
// OpenTelemetry Span wrapper
// ---------------------------------------------------------------------------

function wrapOtelSpan(otelSpan: import("@opentelemetry/api").Span, _parentContext: OtelContext): Span {
  return {
    start() {
      // OTel spans are started at creation — nothing to do
      return this
    },
    end() {
      otelSpan.setStatus({ code: SpanStatusCode.OK })
      otelSpan.end()
    },
    recordException(error: Error) {
      otelSpan.recordException(error)
      otelSpan.setStatus({ code: SpanStatusCode.ERROR, message: error.message })
      otelSpan.end()
    },
    runActive<T>(fn: () => T): T {
      // Make this span the active context for the duration of `fn`. With the
      // AsyncLocalStorage context manager the span stays active across awaits in
      // an async `fn`, so propagateContext / currentTraceContext and any child
      // spans created during handling parent to it.
      return context.with(trace.setSpan(context.active(), otelSpan), fn)
    },
  }
}

// ---------------------------------------------------------------------------
// OpenTelemetrySpanFactory
// ---------------------------------------------------------------------------

export interface OpenTelemetrySpanFactoryOptions {
  /** Custom tracer. Defaults to `trace.getTracer("kronos-framework")`. */
  tracer?: Tracer
  /** Custom span attribute providers. */
  spanAttributeProviders?: SpanAttributesProvider[]
}

/**
 * SpanFactory implementation using OpenTelemetry.
 *
 * Creates spans for message dispatch (PRODUCER), handling (CONSUMER),
 * and internal operations (INTERNAL). Propagates trace context through
 * message metadata using W3C Trace Context standard.
 *
 * ```typescript
 * await kronos().use(openTelemetry()).start()
 * ```
 */
export function openTelemetrySpanFactory(
  options: OpenTelemetrySpanFactoryOptions = {},
): SpanFactory {
  const tracer = options.tracer ?? trace.getTracer("kronos-framework")
  const attributeProviders: SpanAttributesProvider[] = [...(options.spanAttributeProviders ?? [])]

  function addMessageAttributes(
    otelSpan: import("@opentelemetry/api").Span,
    message: Message,
  ) {
    // Built-in attributes
    otelSpan.setAttribute("kronos.message.name", String(message.name))
    otelSpan.setAttribute("kronos.message.id", message.identifier)

    // Custom attribute providers
    for (const provider of attributeProviders) {
      const attrs = provider.provideAttributes(message)
      for (const [key, value] of Object.entries(attrs)) {
        otelSpan.setAttribute(key, value)
      }
    }
  }

  function extractContext(parentMessage: Message): OtelContext {
    return propagation.extract(context.active(), parentMessage, metadataGetter)
  }

  return {
    createRootTrace(operationName: string): Span {
      const otelSpan = tracer.startSpan(operationName, {
        kind: SpanKind.INTERNAL,
      })
      return wrapOtelSpan(otelSpan, context.active())
    },

    createHandlerSpan(operationName: string, parentMessage: Message): Span {
      const parentContext = extractContext(parentMessage)
      const otelSpan = tracer.startSpan(
        operationName,
        { kind: SpanKind.CONSUMER },
        parentContext,
      )
      addMessageAttributes(otelSpan, parentMessage)
      return wrapOtelSpan(otelSpan, parentContext)
    },

    createLinkedHandlerSpan(operationName: string, parentMessage: Message): Span {
      // New trace (root), linked to the parent message's span rather than
      // parented to it — the originating trace may be long finished by the time
      // an asynchronous processor handles the event.
      const parentContext = extractContext(parentMessage)
      const parentSpanContext = trace.getSpanContext(parentContext)
      const otelSpan = tracer.startSpan(operationName, {
        kind: SpanKind.CONSUMER,
        root: true,
        links: parentSpanContext ? [{ context: parentSpanContext }] : [],
      })
      addMessageAttributes(otelSpan, parentMessage)
      return wrapOtelSpan(otelSpan, parentContext)
    },

    createDispatchSpan(operationName: string, parentMessage: Message): Span {
      const parentContext = extractContext(parentMessage)
      const otelSpan = tracer.startSpan(
        operationName,
        { kind: SpanKind.PRODUCER },
        parentContext,
      )
      addMessageAttributes(otelSpan, parentMessage)
      return wrapOtelSpan(otelSpan, parentContext)
    },

    createInternalSpan(operationName: string): Span {
      const otelSpan = tracer.startSpan(operationName, {
        kind: SpanKind.INTERNAL,
      })
      return wrapOtelSpan(otelSpan, context.active())
    },

    propagateContext<M extends Message>(message: M): M {
      const additionalMetadata: Record<string, unknown> = {}
      propagation.inject(context.active(), additionalMetadata, metadataSetter)

      if (Object.keys(additionalMetadata).length === 0) {
        return message
      }

      return {
        ...message,
        metadata: { ...message.metadata, ...additionalMetadata },
      }
    },

    currentTraceContext(): Record<string, string> {
      const carrier: Record<string, string> = {}
      propagation.inject(
        context.active(),
        carrier,
        metadataSetter as TextMapSetter<Record<string, string>>,
      )
      return carrier
    },

    registerSpanAttributeProvider(provider: SpanAttributesProvider) {
      attributeProviders.push(provider)
    },
  }
}
