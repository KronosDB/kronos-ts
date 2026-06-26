import type { QualifiedName, Metadata } from "@kronos-ts/common"

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
