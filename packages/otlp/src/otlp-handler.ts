import type { Message, MessageKind } from "@kronos-ts/core"
import { qualifiedNameToString } from "@kronos-ts/core"
import { SpanKind, type Attributes, type OtlpExporter, type SpanKindValue } from "./otlp-exporter.js"
import { traceparentOf } from "./traceparent.js"

// ---------------------------------------------------------------------------
// The CONSUMER side.
//
// A wrapper over the handler FUNCTION, in the shape the persistence packages
// use (`postgresHandler(handler, pg)`): take a handler, return a handler of the
// same shape. It reads NOTHING from the entry it was taken off — the span's name,
// its kind and whether it parents or links all come from the message being
// handled, which is where they honestly live. That is what makes it
// pre-appliable: `otlpHandler(h.handler, exporter)` needs no arrow reaching back
// into `h`.
// ---------------------------------------------------------------------------

/** The attributes a span over a message carries. Shared with the bus wrappers. */
export function messageAttributes(message: Message): Attributes {
  return {
    "kronos.message.name": qualifiedNameToString(message.name),
    "kronos.message.id": message.identifier,
    "kronos.message.kind": message.kind,
  }
}

/**
 * How a message names the span that handles it when the wrap site does not say
 * otherwise — its own qualified name, matching `dispatch(...)`/`query(...)` on
 * the producer side.
 */
export function messageName(message: Message): string {
  return qualifiedNameToString(message.name)
}

function handlerSpanKind(kind: MessageKind): SpanKindValue {
  // A query is answered synchronously to a waiting caller — SERVER, opposite
  // the CLIENT span `otlpQueryBus` opens. Commands and events are consumed off
  // a bus — CONSUMER, opposite PRODUCER.
  return kind === "query" ? SpanKind.SERVER : SpanKind.CONSUMER
}

/**
 * Wrap a handler function so each invocation is a span, joined to the trace the
 * handled message arrived carrying.
 *
 * How it joins is the whole point, and it differs by leg — read off
 * `message.kind`, because the message knows what it is:
 *
 * - COMMAND and QUERY messages PARENT onto the remote context. The dispatcher
 *   is still on the stack waiting for the result, so nesting is honest: the
 *   handler's duration really is part of the caller's.
 *
 * - EVENT messages LINK to it instead, and run as the root of their OWN trace.
 *   An event processor may handle an event long after the producing trace
 *   finished — a projection catching up over a batch of month-old events would
 *   otherwise be swallowed into whatever produced them, reporting a month-long
 *   span. The link keeps the correlation without the lie.
 *
 * `label` ABSENT names the span after the message's qualified name. Pass one to
 * name it otherwise; it is a function OF THE MESSAGE, never a per-handler
 * string closed over at wiring time.
 *
 * ```ts
 * kronos({
 *   commandHandlers: commands.map((h) => ({ ...h, handler: otlpHandler(h.handler, exporter) })),
 *   eventHandlers: projections.map((h) => ({ ...h, handler: otlpHandler(h.handler, exporter) })),
 * })
 * ```
 */
export function otlpHandler<M extends Message, C, R>(
  next: (message: M, context: C) => R,
  exporter: OtlpExporter,
  label?: (message: Message) => string,
): (message: M, context: C) => Promise<Awaited<R>> {
  return async (message, context): Promise<Awaited<R>> => {
    const remote = traceparentOf(message.metadata)
    const linked = message.kind === "event"
    const span = exporter.startSpan({
      name: label ? label(message) : messageName(message),
      kind: handlerSpanKind(message.kind),
      parent: linked ? undefined : remote,
      links: linked && remote ? [remote] : undefined,
      attributes: messageAttributes(message),
    })

    try {
      const result = await next(message, context)
      span.end()
      return result
    } catch (error) {
      span.fail(error)
      throw error
    }
  }
}
