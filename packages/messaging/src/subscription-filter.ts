/**
 * A predicate over a query payload, used by `emitUpdate` to decide which
 * subscribers should receive an update.
 *
 * Two forms are supported:
 *
 * 1. **Function** — `(payload) => boolean`. Easy to write but cannot cross a
 *    network boundary because functions are not serializable. Use for
 *    in-process / single-segment query buses.
 *
 * 2. **Structured `payloadEquals`** — `{ payloadEquals: Partial<P> }`. Every
 *    key in the partial must deep-equal the same key in the subscriber's
 *    query payload. Serializable, so it ships across distributed query bus
 *    transports (e.g. RabbitMQ broadcasts).
 *
 * Both forms work locally; only the structured form crosses processes when a
 * distributed query bus is in use.
 */
export type SubscriptionFilter<P = unknown> =
  | ((payload: P) => boolean)
  | { readonly payloadEquals: Partial<P> }

/**
 * Helper that builds a structured filter from a partial payload.
 *
 * ```ts
 * emitUpdate(GetCourseView, payloadEquals({ courseId: e.courseId }), view)
 * ```
 *
 * Prefer this over a function filter when you want updates to fan out across
 * a distributed query bus.
 */
export function payloadEquals<P>(partial: Partial<P>): { payloadEquals: Partial<P> } {
  return { payloadEquals: partial }
}

/** Evaluate a {@link SubscriptionFilter} against a payload. */
export function applySubscriptionFilter<P>(filter: SubscriptionFilter<P>, payload: P): boolean {
  if (typeof filter === "function") return filter(payload)
  return matchesPayloadEquals(payload, filter.payloadEquals)
}

/** Extract the structured form, if any, for serialization across a transport. */
export function extractStructuredFilter<P>(
  filter: SubscriptionFilter<P> | undefined,
): { payloadEquals: Partial<P> } | undefined {
  if (!filter) return undefined
  if (typeof filter === "function") return undefined
  return filter
}

/** Deep equality on the keys defined in `expected`. */
export function matchesPayloadEquals<P>(payload: P, expected: Partial<P>): boolean {
  if (payload === null || typeof payload !== "object") {
    return Object.keys(expected).length === 0
  }
  for (const key of Object.keys(expected) as Array<keyof P>) {
    if (!deepEqual((payload as P)[key], expected[key])) return false
  }
  return true
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || b === null) return false
  if (typeof a !== typeof b) return false
  if (typeof a !== "object") return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false
    for (let i = 0; i < a.length; i++) {
      if (!deepEqual(a[i], b[i])) return false
    }
    return true
  }
  const ao = a as Record<string, unknown>
  const bo = b as Record<string, unknown>
  const aKeys = Object.keys(ao)
  const bKeys = Object.keys(bo)
  if (aKeys.length !== bKeys.length) return false
  for (const key of aKeys) {
    if (!deepEqual(ao[key], bo[key])) return false
  }
  return true
}
