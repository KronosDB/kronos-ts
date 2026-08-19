import type { Metadata } from "@kronos-ts/core"
import type { TraceContext } from "./otlp-exporter.js"

// ---------------------------------------------------------------------------
// W3C Trace Context, the only part of it a message bus needs.
//
// Message metadata IS the carrier: it already crosses every boundary this
// framework has (in-process bus, RabbitMQ, KronosDB, the event store), so
// propagation is one string on a record nobody has to teach a transport about.
// ---------------------------------------------------------------------------

/** The metadata key trace context travels under. Lowercase, as W3C spells it. */
export const TRACEPARENT = "traceparent"

const TRACEPARENT_PATTERN = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/

const INVALID_TRACE_ID = "0".repeat(32)
const INVALID_SPAN_ID = "0".repeat(16)

/** `00-<traceid>-<spanid>-01` — sampled, because we already decided to export it. */
export function formatTraceparent(context: TraceContext): string {
  return `00-${context.traceId}-${context.spanId}-01`
}

/**
 * The trace context a message arrived with, or `undefined` when it carries
 * none / an unparseable one. A malformed header is treated as absent: a new
 * root trace is a better answer than a corrupted one.
 */
export function traceparentOf(metadata: Metadata): TraceContext | undefined {
  const raw = metadata[TRACEPARENT]
  if (typeof raw !== "string") return undefined
  const match = TRACEPARENT_PATTERN.exec(raw)
  if (!match) return undefined
  const [, version, trace, span] = match
  if (version === "ff") return undefined
  if (!trace || !span) return undefined
  if (trace === INVALID_TRACE_ID || span === INVALID_SPAN_ID) return undefined
  return { traceId: trace, spanId: span }
}

/** The same metadata, carrying `context` as its traceparent. */
export function withTraceparent(metadata: Metadata, context: TraceContext): Metadata {
  return { ...metadata, [TRACEPARENT]: formatTraceparent(context) }
}
