import type { QualifiedName, Metadata } from "@kronos-ts/common"

/**
 * A message carrying a payload and metadata, identified by a unique ID
 * and routed by its qualified name.
 */
export interface Message<P = unknown> {
  readonly identifier: string
  readonly name: QualifiedName
  readonly payload: P
  readonly metadata: Metadata
  readonly timestamp: number
}

/**
 * A command message — dispatched to exactly one handler, may return a result.
 */
export interface CommandMessage<P = unknown> extends Message<P> {}

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
  readonly version: string
  readonly tags: ReadonlyArray<{ readonly key: string; readonly value: string }>
}

/**
 * A query message — dispatched to handler(s) that can answer it.
 */
export interface QueryMessage<P = unknown> extends Message<P> {}
