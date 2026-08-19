import type { QualifiedName } from "../primitives/qualified-name.js"
import type { Metadata } from "../primitives/metadata.js"
import type { Clock } from "../primitives/clock.js"

/**
 * Discriminates a message by its dispatch category.
 *
 * TypeScript erases interfaces at runtime, so `CommandMessage`, `EventMessage`,
 * and `QueryMessage` — which are otherwise shape-identical — cannot be told
 * apart with `instanceof` the way Axon Framework's nominal interfaces can.
 * This field is the structural-typing equivalent of that `instanceof` check:
 * it lets a reusable handler interceptor branch on message category without
 * being pinned to a single bus.
 */
export type MessageKind = "command" | "event" | "query"

/**
 * A message carrying a payload and metadata, identified by a unique ID
 * and routed by its qualified name.
 *
 * `kind` is derived at construction/reconstruction time, never persisted —
 * each bus and event-store reconstruction site sets it from context.
 */
export interface Message<P = unknown> {
  readonly kind: MessageKind
  readonly identifier: string
  readonly name: QualifiedName
  readonly payload: P
  readonly metadata: Metadata
  readonly timestamp: number
}

/**
 * A command message — dispatched to exactly one handler, may return a result.
 */
export interface CommandMessage<P = unknown> extends Message<P> {
  readonly kind: "command"
}

/**
 * A command result message — the response from handling a command.
 */
export interface CommandResultMessage<R = unknown> {
  readonly identifier: string
  readonly payload: R | undefined
  readonly metadata: Metadata
  readonly error?: Error
}

/**
 * An event message — published to all interested handlers.
 */
export interface EventMessage<P = unknown> extends Message<P> {
  readonly kind: "event"
  readonly version: string
  readonly tags: ReadonlyArray<{ readonly key: string; readonly value: string }>
}

export interface SequencedEventMessage<P = unknown> extends EventMessage<P> {
  /** Stream position when the source has one; absent for push-only delivery. */
  readonly sequence?: bigint
}

/**
 * A query message — dispatched to handler(s) that can answer it.
 */
export interface QueryMessage<P = unknown> extends Message<P> {
  readonly kind: "query"
}

/**
 * A message that has not been stamped with its birth instant yet.
 *
 * The edge verbs (`send`, `query`, `subscriptionQuery`) build a message from a
 * descriptor and a payload and hand it straight to a bus. They do NOT stamp
 * `timestamp`, because the instant belongs to the TASK that handles the message
 * and the task does not exist yet: the bus mints the unit of work, and a unit of
 * work carries the {@link Clock}. So the verb builds a message with everything
 * it can know, and the bus fills in the one field only the task can supply.
 *
 * Every bus therefore ACCEPTS this shape and never produces it — a handler,
 * an interceptor's output and an event in the store are all fully stamped. A
 * `Message` is assignable here, so a caller that already has a real instant
 * (a transport reconstructing an inbound message, a test) passes it unchanged
 * and {@link stamped} leaves it alone.
 */
export type Unstamped<M extends Message> = Omit<M, "timestamp"> & {
  readonly timestamp?: number
}

/**
 * Stamp a message's birth instant, if whoever built it could not.
 *
 * Idempotent by construction: a message that already carries a `timestamp` is
 * returned as-is, so wrapping buses and transports can each call this without
 * any of them having to know whether another already did.
 */
export function stamped<M extends Message>(message: Unstamped<M>, clock: Clock): M {
  return (message.timestamp === undefined
    ? { ...message, timestamp: clock() }
    : message) as M
}
