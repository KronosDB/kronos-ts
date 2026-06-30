/**
 * A tracking token represents a processor's position in an event stream.
 *
 * This interface is extensible — event store implementations can define
 * their own token types (e.g., a PostgreSQL extension might track
 * commit timestamps alongside sequence positions).
 *
 * The framework ships two built-in implementations:
 * - `GlobalSequenceToken` — for sequential event stores (in-memory, Axon Server)
 * - `ReplayToken` — wraps any token to mark replay-in-progress state
 *
 * Tokens are immutable. All operations return new instances.
 */
export interface TrackingToken {
  /**
   * Discriminant for type narrowing and serialization.
   * Each token implementation has a unique kind string.
   */
  readonly kind: string

  /**
   * The effective read position in the event stream.
   * Used by the event source to know where to start reading.
   */
  position(): bigint

  /**
   * Does this token's position include all events covered by the other?
   * Returns true if every event that `other` has seen, this token has also seen.
   *
   * Used for replay completion detection and segment merging.
   */
  covers(other: TrackingToken): boolean

  /**
   * Create a token representing the lower bound of this and another token.
   * When opening a shared stream for multiple segments, use the lower bound
   * to ensure no events are missed by any segment.
   */
  lowerBound(other: TrackingToken): TrackingToken

  /**
   * Create a token representing the upper bound of this and another token.
   * Represents the furthest position — used for segment merging.
   */
  upperBound(other: TrackingToken): TrackingToken

  /**
   * Check if this token represents the same position as the other.
   * Default: `this.covers(other) && other.covers(this)`.
   */
  samePositionAs(other: TrackingToken): boolean
}

// ---------------------------------------------------------------------------
// GlobalSequenceToken
// ---------------------------------------------------------------------------

export interface GlobalSequenceToken extends TrackingToken {
  readonly kind: "global-sequence"
  readonly sequence: bigint
}

/**
 * Creates a token for a global-sequence event store (monotonically increasing positions).
 * This is the default token type for in-memory stores and Axon Server.
 */
export function globalSequenceToken(sequence: bigint): GlobalSequenceToken {
  return {
    kind: "global-sequence",
    sequence,
    position: () => sequence,
    covers: (other) => sequence >= other.position(),
    lowerBound: (other) => globalSequenceToken(sequence < other.position() ? sequence : other.position()),
    upperBound: (other) => globalSequenceToken(sequence > other.position() ? sequence : other.position()),
    samePositionAs: (other) => sequence === other.position(),
  }
}

// ---------------------------------------------------------------------------
// GapAwareToken
// ---------------------------------------------------------------------------

export interface GapAwareToken extends TrackingToken {
  readonly kind: "gap-aware"
  /** The `sequence_position` of the last consumed event — the `position()`. */
  readonly sequence: bigint
  /**
   * An opaque, store-defined commit-order key that, paired with `sequence`,
   * forms a gap-free tailing cursor. For the Postgres engine this is the
   * event's `transaction_id` (xid8): the durable token MUST carry it because
   * gap-free tailing orders by `(transaction_id, sequence_position)` and only
   * `transaction_id` has a commit watermark (`pg_snapshot_xmin`). A position
   * alone cannot resume the stream without permanently skipping events whose
   * `sequence_position` is lower but whose `transaction_id` is higher (the
   * xid/seq inversion that happens when a transaction writes other rows —
   * stamping its xid — before appending its event).
   */
  readonly gapKey: string
}

/**
 * Creates a token for a gap-free tailing engine: a `sequence` position paired
 * with an opaque `gapKey` (the store's commit-order key, e.g. Postgres xid8).
 * `position()` returns the sequence so replay/`covers` semantics are unchanged;
 * the `gapKey` rides along so the engine can resume the `(gapKey, sequence)`
 * cursor exactly on reopen instead of falling back to a lossy position filter.
 */
export function gapAwareToken(sequence: bigint, gapKey: string): GapAwareToken {
  return {
    kind: "gap-aware",
    sequence,
    gapKey,
    position: () => sequence,
    covers: (other) => sequence >= other.position(),
    lowerBound: (other) =>
      sequence <= other.position() ? gapAwareToken(sequence, gapKey) : globalSequenceToken(other.position()),
    upperBound: (other) =>
      sequence >= other.position() ? gapAwareToken(sequence, gapKey) : globalSequenceToken(other.position()),
    samePositionAs: (other) => sequence === other.position(),
  }
}

/**
 * Sentinel token representing the beginning of the event stream.
 * A processor starting with FIRST_TOKEN will read from position 0.
 */
export const FIRST_TOKEN: TrackingToken = globalSequenceToken(0n)

/**
 * Sentinel token representing the tail of the event stream.
 * A processor starting with LATEST_TOKEN will skip all existing events
 * and only process new events appended after startup.
 */
export const LATEST_TOKEN: TrackingToken = {
  kind: "latest",
  position: () => BigInt(Number.MAX_SAFE_INTEGER),
  covers: () => true,
  lowerBound: (other) => other,
  upperBound: () => LATEST_TOKEN,
  samePositionAs: (other) => other === LATEST_TOKEN || (other.kind === "latest"),
}

// ---------------------------------------------------------------------------
// ReplayToken
// ---------------------------------------------------------------------------

export interface ReplayToken extends TrackingToken {
  readonly kind: "replay"
  /** The wrapped token representing current progress during replay. */
  readonly currentToken: TrackingToken
  /** The token representing the position when reset was triggered. Events before this are replayed. */
  readonly tokenAtReset: TrackingToken
  /** Optional user-provided context (e.g., reason for the reset). */
  readonly resetContext?: unknown
}

/**
 * Creates a replay token that wraps another token to mark replay-in-progress.
 *
 * During replay, events are re-delivered from `currentToken` up to `tokenAtReset`.
 * Once `currentToken.covers(tokenAtReset)`, the replay is complete and the
 * token unwraps to the current position.
 *
 * @param tokenAtReset The head position when reset was triggered
 * @param currentToken Where replay is currently reading from
 * @param resetContext Optional user-provided context
 */
export function replayToken(
  tokenAtReset: TrackingToken,
  currentToken: TrackingToken,
  resetContext?: unknown,
): ReplayToken {
  return {
    kind: "replay",
    currentToken,
    tokenAtReset,
    resetContext,

    position: () => currentToken.position(),

    covers: (other) => currentToken.covers(other),

    lowerBound: (other) => {
      const inner = currentToken.lowerBound(other)
      // If the lower bound is still within replay range, keep the replay wrapper
      if (!inner.covers(tokenAtReset)) {
        return replayToken(tokenAtReset, inner, resetContext)
      }
      return inner
    },

    upperBound: (other) => {
      const inner = currentToken.upperBound(other)
      // If the upper bound has passed the reset point, replay is done
      if (inner.covers(tokenAtReset)) {
        return inner
      }
      return replayToken(tokenAtReset, inner, resetContext)
    },

    samePositionAs: (other) => currentToken.samePositionAs(other),
  }
}

// ---------------------------------------------------------------------------
// Type guards
// ---------------------------------------------------------------------------

export function isReplayToken(token: TrackingToken): token is ReplayToken {
  return token.kind === "replay"
}

export function isGlobalSequenceToken(token: TrackingToken): token is GlobalSequenceToken {
  return token.kind === "global-sequence"
}

export function isGapAwareToken(token: TrackingToken): token is GapAwareToken {
  return token.kind === "gap-aware"
}

// ---------------------------------------------------------------------------
// Token operations
// ---------------------------------------------------------------------------

/**
 * Advance a token to the position represented by `next`, preserving replay
 * wrapping. Generalises {@link advanceToken} to any TrackingToken — used when
 * the event source supplies its own cursor token (e.g. a {@link GapAwareToken}
 * carrying a commit-order key) that must be persisted verbatim rather than
 * collapsed to a bare position.
 */
export function advanceTokenTo(token: TrackingToken, next: TrackingToken): TrackingToken {
  if (!isReplayToken(token)) {
    return next
  }

  // Check if replay is complete
  if (next.covers(token.tokenAtReset)) {
    return next
  }

  // Still replaying — wrap the advanced token
  return replayToken(token.tokenAtReset, next, token.resetContext)
}

/**
 * Advance a token to a new position. If the token is a ReplayToken and
 * the new position covers the reset point, unwraps to a plain token.
 */
export function advanceToken(token: TrackingToken, newPosition: bigint): TrackingToken {
  return advanceTokenTo(token, globalSequenceToken(newPosition))
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

/** Wire shape for a persisted token: a `kind` discriminant + a JSON body. */
export interface SerializedToken {
  readonly type: string
  readonly data: string
}

/**
 * Serialize a token for durable storage. The JSON body always carries
 * `position`; when the token (or, for a ReplayToken, its innermost token) is a
 * {@link GapAwareToken}, the `gapKey` is preserved so the cursor resumes
 * exactly on reload. ReplayTokens still flatten to their current position on
 * the wire (replay-in-progress state does not survive a restart, as before),
 * but the gapKey survives so live tailing resumes without skipping events.
 */
export function serializeToken(token: TrackingToken): SerializedToken {
  const inner = unwrapToken(token)
  const payload: { position: string; gapKey?: string } = {
    position: token.position().toString(),
  }
  if (isGapAwareToken(inner)) {
    payload.gapKey = inner.gapKey
  }
  return { type: token.kind, data: JSON.stringify(payload) }
}

/**
 * Reconstruct a token from its persisted form. A body carrying a `gapKey`
 * rehydrates as a {@link GapAwareToken}; otherwise a {@link GlobalSequenceToken}.
 * Returns undefined when there is no stored token.
 */
export function deserializeToken(
  type: string | null | undefined,
  data: string | null | undefined,
): TrackingToken | undefined {
  if (!data) return undefined
  const parsed = JSON.parse(data) as { position: string; gapKey?: string }
  if (parsed.gapKey !== undefined) {
    return gapAwareToken(BigInt(parsed.position), parsed.gapKey)
  }
  return globalSequenceToken(BigInt(parsed.position))
}

/**
 * Check whether the given token represents a replay in progress.
 * Returns true if the token is a ReplayToken AND the current position
 * has not yet passed the reset position.
 *
 * A ReplayToken that has been fully replayed (current covers reset)
 * would have been unwrapped by advanceToken — so any ReplayToken
 * that still exists is mid-replay.
 */
export function isReplaying(token: TrackingToken): boolean {
  return isReplayToken(token)
}

/**
 * Unwrap a token to its innermost non-replay token.
 * If the token is not a ReplayToken, returns it unchanged.
 */
export function unwrapToken(token: TrackingToken): TrackingToken {
  if (isReplayToken(token)) {
    return unwrapToken(token.currentToken)
  }
  return token
}

/**
 * Check if an event at the given position was already processed before
 * the reset (i.e., it's a replayed event, not a new one).
 *
 * Uses the token's `covers()` semantics: if the reset token covers
 * the event position, then the event existed before the reset.
 */
export function wasProcessedBeforeReset(token: ReplayToken, eventPosition: bigint): boolean {
  return token.tokenAtReset.covers(globalSequenceToken(eventPosition))
}
