import type { Message } from "../messaging/messages.js"

/**
 * The correlation rule, written out. Both fields SEED and neither clobbers:
 *
 * - `correlationId` is preserved when the message already has one, and
 *   otherwise starts a new chain at this message.
 * - `causationId` is preserved when the message already has one, and otherwise
 *   starts at this message's own identifier.
 *
 * The `??` on `causationId` is the whole point. A message that arrives with a
 * cause already on it was CAUSED by something — `correlatingHandler` overlays
 * the handled message's pair onto everything a handler emits, which is where a
 * real causationId comes from. An unconditional `causationId: message.identifier`
 * at the bus edge overwrote that with the emitted message's own identifier, so
 * every message in a multi-hop chain claimed to have caused itself and the
 * causal graph collapsed to a set of self-loops. `correlation` seeds ROOTS —
 * messages born at an edge with no cause — and the handler wrapper re-stamps
 * per hop.
 *
 * Applying it twice is a no-op on both fields, which is what lets a transport
 * bus wrap a local segment that is itself intercepting.
 *
 * It is an {@link import("../interception/intercepting-bus.js").Intercept}, so
 * it goes on a bus rather than on a handler:
 *
 * ```ts
 * interceptingCommandBus(localCommandBus(uow), correlation)
 * ```
 */
export const correlation = <M extends Message>(message: M): M => ({
  ...message,
  metadata: {
    ...message.metadata,
    correlationId: String(message.metadata.correlationId ?? message.identifier),
    causationId: String(message.metadata.causationId ?? message.identifier),
  },
})
