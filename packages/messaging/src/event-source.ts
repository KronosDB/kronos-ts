import type { EventMessage } from "./message.js"
import type { EventCriteria } from "./event-criteria.js"
import type { TrackingToken } from "./tracking-token.js"

/**
 * An event with its global sequence position.
 */
export interface SequencedEvent {
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
 * Defines the starting position and optional event criteria filter.
 */
export interface StreamingCondition {
  /** Position to start streaming from. */
  readonly position: bigint
  /**
   * The resume token. When it carries an engine-specific cursor (e.g. a
   * {@link GapAwareToken} with a commit-order key), the engine resumes exactly
   * from that cursor; otherwise it falls back to {@link position}. Optional so
   * engines that only understand a position can ignore it.
   */
  readonly token?: TrackingToken
  /** Optional criteria to filter events. When omitted, all events are delivered. */
  readonly criteria?: EventCriteria
}

/**
 * A push-based message stream. Events are buffered internally and
 * pulled via {@link next}. The stream notifies when events become
 * available via {@link setCallback}.
 */
export interface MessageStream<M> {
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
  /** Transform each item. */
  map<R>(mapper: (item: M) => R): MessageStream<R>
  /** Filter items by predicate. */
  filter(predicate: (item: M) => boolean): MessageStream<M>
  /** Recover from stream errors by providing a continuation stream. */
  onErrorContinue(recovery: (error: Error) => MessageStream<M>): MessageStream<M>
  /** Reduce all items to a single value. Resolves when the stream completes. */
  reduce<R>(identity: R, accumulator: (acc: R, item: M) => R): Promise<R>
  /** Concatenate another stream after this one completes. */
  concatWith(other: MessageStream<M>): MessageStream<M>
}

/**
 * Creates a MessageStream that wraps a source with transformation support.
 * Used by event store implementations to provide stream instances.
 */
export function createMessageStream<M>(source: {
  next(): M | undefined
  peek(): M | undefined
  hasNextAvailable(): boolean
  isCompleted(): boolean
  error(): Error | undefined
  setCallback(callback: () => void): void
  close(): void
}): MessageStream<M> {
  const stream: MessageStream<M> = {
    ...source,
    map<R>(mapper: (item: M) => R): MessageStream<R> {
      return createMappedStream(stream, mapper)
    },
    filter(predicate: (item: M) => boolean): MessageStream<M> {
      return createFilteredStream(stream, predicate)
    },
    onErrorContinue(recovery: (error: Error) => MessageStream<M>): MessageStream<M> {
      return createErrorRecoveryStream(stream, recovery)
    },
    reduce<R>(identity: R, accumulator: (acc: R, item: M) => R): Promise<R> {
      return reduceStream(stream, identity, accumulator)
    },
    concatWith(other: MessageStream<M>): MessageStream<M> {
      return createConcatStream(stream, other)
    },
  }
  return stream
}

/**
 * Creates a completed empty MessageStream.
 */
export function emptyMessageStream<M>(): MessageStream<M> {
  return createMessageStream<M>({
    next: () => undefined,
    peek: () => undefined,
    hasNextAvailable: () => false,
    isCompleted: () => true,
    error: () => undefined,
    setCallback: () => {},
    close: () => {},
  })
}

/**
 * Creates a MessageStream that immediately fails with the given error.
 */
export function failedMessageStream<M>(error: Error): MessageStream<M> {
  return createMessageStream<M>({
    next: () => undefined,
    peek: () => undefined,
    hasNextAvailable: () => false,
    isCompleted: () => true,
    error: () => error,
    setCallback: () => {},
    close: () => {},
  })
}

// ---------------------------------------------------------------------------
// Stream transformations
// ---------------------------------------------------------------------------

function createMappedStream<M, R>(source: MessageStream<M>, mapper: (item: M) => R): MessageStream<R> {
  return createMessageStream<R>({
    next() {
      const item = source.next()
      return item !== undefined ? mapper(item) : undefined
    },
    peek() {
      const item = source.peek()
      return item !== undefined ? mapper(item) : undefined
    },
    hasNextAvailable: () => source.hasNextAvailable(),
    isCompleted: () => source.isCompleted(),
    error: () => source.error(),
    setCallback: (cb) => source.setCallback(cb),
    close: () => source.close(),
  })
}

function createFilteredStream<M>(source: MessageStream<M>, predicate: (item: M) => boolean): MessageStream<M> {
  let buffered: M | undefined

  function advance(): M | undefined {
    while (true) {
      const item = source.next()
      if (item === undefined) return undefined
      if (predicate(item)) return item
    }
  }

  return createMessageStream<M>({
    next() {
      if (buffered !== undefined) {
        const item = buffered
        buffered = undefined
        return item
      }
      return advance()
    },
    peek() {
      if (buffered !== undefined) return buffered
      buffered = advance()
      return buffered
    },
    hasNextAvailable() {
      if (buffered !== undefined) return true
      buffered = advance()
      return buffered !== undefined
    },
    isCompleted: () => source.isCompleted() && buffered === undefined,
    error: () => source.error(),
    setCallback: (cb) => source.setCallback(cb),
    close: () => source.close(),
  })
}

function createErrorRecoveryStream<M>(
  source: MessageStream<M>,
  recovery: (error: Error) => MessageStream<M>,
): MessageStream<M> {
  let current: MessageStream<M> = source
  let recovered = false

  function checkRecovery(): void {
    if (!recovered && current.error()) {
      current = recovery(current.error()!)
      recovered = true
    }
  }

  return createMessageStream<M>({
    next() { checkRecovery(); return current.next() },
    peek() { checkRecovery(); return current.peek() },
    hasNextAvailable() { checkRecovery(); return current.hasNextAvailable() },
    isCompleted() { checkRecovery(); return current.isCompleted() },
    error() { return recovered ? current.error() : undefined },
    setCallback(cb) { current.setCallback(cb) },
    close() { current.close() },
  })
}

function createConcatStream<M>(first: MessageStream<M>, second: MessageStream<M>): MessageStream<M> {
  let usingFirst = true

  function current(): MessageStream<M> {
    if (usingFirst && first.isCompleted() && !first.hasNextAvailable()) {
      usingFirst = false
    }
    return usingFirst ? first : second
  }

  return createMessageStream<M>({
    next() { return current().next() },
    peek() { return current().peek() },
    hasNextAvailable() { return current().hasNextAvailable() },
    isCompleted() { return current().isCompleted() },
    error() { return current().error() },
    setCallback(cb) {
      if (usingFirst) {
        first.setCallback(() => {
          if (first.isCompleted() && !first.hasNextAvailable()) {
            usingFirst = false
            second.setCallback(cb)
            cb()
          } else {
            cb()
          }
        })
      } else {
        second.setCallback(cb)
      }
    },
    close() { first.close(); second.close() },
  })
}

async function reduceStream<M, R>(
  stream: MessageStream<M>,
  identity: R,
  accumulator: (acc: R, item: M) => R,
): Promise<R> {
  let result = identity
  return new Promise<R>((resolve, reject) => {
    function drain() {
      while (true) {
        const item = stream.next()
        if (item !== undefined) {
          result = accumulator(result, item)
          continue
        }
        if (stream.error()) {
          reject(stream.error())
          return
        }
        if (stream.isCompleted()) {
          resolve(result)
          return
        }
        // Wait for more items
        stream.setCallback(drain)
        return
      }
    }
    drain()
  })
}

// ---------------------------------------------------------------------------
// StreamableEventSource
// ---------------------------------------------------------------------------

/**
 * A source of events that can be opened as an infinite stream.
 */
export interface StreamableEventSource {
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
