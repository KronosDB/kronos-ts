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

function wrapOtelSpan(otelSpan: import("@opentelemetry/api").Span, parentContext: OtelContext): Span {
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
 * Aligned with Kronos Framework's `OpenTelemetrySpanFactory`.
 *
 * ```typescript
 * await kronos().use(openTelemetry()).start()
 * ```
 */
export function createOpenTelemetrySpanFactory(
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

    registerSpanAttributeProvider(provider: SpanAttributesProvider) {
      attributeProviders.push(provider)
    },
  }
}
