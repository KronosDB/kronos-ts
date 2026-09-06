import type { EventMessage } from "../messaging/messages.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { TrackingToken } from "./tracking-token.js"

/**
 * An event with its global sequence position.
 */
export type SequencedEvent = {
  readonly sequence: bigint
  readonly event: EventMessage
  /**
   * The cursor token positioned immediately AFTER this event — i.e. resuming a
   * stream from this token reads events strictly following this one. Supplied
   * by gap-free engines (e.g. Postgres) so the processor persists the engine's
   * own `(commit-order-key, sequence)` cursor verbatim instead of synthesising
   * a position-only token, which would lose the commit-order key and skip
   * events on reopen. Engines with a dense global sequence (in-memory, Axon
   * Server) omit it; the processor then falls back to a position+1 token.
   */
  readonly token?: TrackingToken
}

/**
 * Condition for opening a streaming event source.
 * Defines the starting position and an optional event-query filter.
 */
export type StreamingCondition = {
  /** Position to start streaming from. */
  readonly position: bigint
  /**
   * The resume token. When it carries an engine-specific cursor (e.g. a
   * {@link GapAwareToken} with a commit-order key), the engine resumes exactly
   * from that cursor; otherwise it falls back to {@link position}. Optional so
   * engines that only understand a position can ignore it.
   */
  readonly token?: TrackingToken
  /** Optional query to filter events. When omitted, all events are delivered. */
  readonly query?: EventQuery
}

/**
 * What a processor pulls events from: a buffered, non-blocking cursor with a
 * wake-up callback. Stores build one in `open()`.
 *
 * SEVEN MEMBERS, AND NOTHING COMPOSES THEM. This used to carry `map`, `filter`,
 * `reduce`, `concatWith` and `onErrorContinue` — another framework's stream
 * algebra, transcribed, with no caller. A wrapper that needs to transform items
 * (see `upcastingEventStore`) writes the seven delegations itself.
 */
export type MessageStream<M> = {
  /** Pull the next available item (non-blocking). */
  next(): M | undefined
  /** Peek at the next item without consuming it. */
  peek(): M | undefined
  /** Check if there are items ready to be pulled. */
  hasNextAvailable(): boolean
  /** Whether the stream has been completed. */
  isCompleted(): boolean
  /** The error that caused the stream to fail, if any. */
  error(): Error | undefined
  /** Register a callback for when items become available. */
  setCallback(callback: () => void): void
  /** Close the stream and release resources. */
  close(): void
}

// ---------------------------------------------------------------------------
// StreamableEventSource
// ---------------------------------------------------------------------------

/**
 * A source of events that can be opened as an infinite stream.
 */
export type StreamableEventSource = {
  /**
   * Open an infinite event stream starting from the given condition.
   */
  open(condition: StreamingCondition): MessageStream<SequencedEvent>

  /**
   * Get the token representing the beginning of the event stream.
   * A processor starting from this token will read all events.
   */
  firstToken(): Promise<TrackingToken>

  /**
   * Get the token representing the current tail of the event stream.
   * A processor starting from this token will only see new events.
   */
  latestToken(): Promise<TrackingToken>

  /**
   * Get the current head position — the sequence of the next event
   * to be appended. Convenience method equivalent to
   * {@code (await latestToken()).position()}.
   */
  getHeadPosition(): Promise<bigint>
}
