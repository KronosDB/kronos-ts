/**
 * Common control + status surface for all event processors — the kronos analog
 * of AF5's `EventProcessor` interface. This is the contract a host (or an admin
 * UI) drives: enumerate processors, read status, start/stop, reset.
 *
 * AF5 splits a base `EventProcessor` (run/error/lifecycle) from
 * `StreamingEventProcessor` (status, reset, segments). kronos mirrors that: the
 * base is here; the streaming-specific operations (`status`, `resetTokens`,
 * `reprocessDeadLetters`) live on {@link TrackingEventProcessor}.
 */

/**
 * A point-in-time snapshot of a processor's progress — the kronos analog of
 * AF5's `EventTrackerStatus`. kronos processors are single-segment, so this is
 * one snapshot per processor rather than a per-segment map.
 */
export interface EventProcessorStatus {
  /** Whether the processor's polling/streaming loop is active. */
  readonly running: boolean
  /** The most recent unrecovered processing error, if any. Cleared on the next
   *  successful batch. A non-undefined value is the kronos `isErrorState`. */
  readonly error?: Error
  /** Current committed position in the event stream. */
  readonly position: bigint
  /** Whether the processor has consumed all currently-available events. */
  readonly caughtUp: boolean
  /** Whether the processor is currently replaying (reset) the stream. */
  readonly replaying: boolean
}

/**
 * Common interface for all event processors. Both tracking and subscribing
 * processors satisfy it, so a host can enumerate and operate them uniformly.
 */
export interface EventProcessor {
  readonly name: string
  readonly running: boolean
  /** Subscribing processors start synchronously; streaming ones return a promise. */
  start(): Promise<void> | void
  stop(): void
}
