import type { Message, Metadata } from "../messaging/messages.js"

/**
 * The standard cargo: what a handling carries from the message it handles
 * onto everything it gives birth to. Axon calls the same three lines its
 * `MessageOriginProvider`.
 *
 * - `correlationId` is the CHAIN — inherited when the parent has one, seeded
 *   from the parent otherwise.
 * - `causationId` is the PARENT, unconditionally — never the parent's own
 *   causationId, which would name the grandparent and collapse the chain.
 * - `traceparent` rides along when the parent carries one. It is the W3C
 *   trace-context key, not a tracing package's private name: a tracing
 *   wrapper stamps its span onto the handled message, and this is what puts
 *   that span on every message the handling produces and on every event it
 *   appends. Without a tracing wrapper the key is simply absent.
 *
 * `correlatingHandler` uses it when given no cargo function. A host that
 * carries more spreads it:
 *
 * ```ts
 * correlatingHandler(h.handler, (m) => ({ ...messageOrigin(m), actor: String(m.metadata.actor ?? "") }))
 * ```
 */
export const messageOrigin = (parent: Message): Metadata => {
  const traceparent = parent.metadata.traceparent
  return {
    correlationId: String(parent.metadata.correlationId ?? parent.identifier),
    causationId: String(parent.identifier),
    ...(typeof traceparent === "string" ? { traceparent } : {}),
  }
}
