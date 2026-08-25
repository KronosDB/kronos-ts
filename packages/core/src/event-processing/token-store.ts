import type { TrackingToken } from "./tracking-token.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Stores tracking tokens for event processors and manages segment claims.
 *
 * Each processor has a name and one or more segments. The token
 * represents the processor's position in the event stream.
 *
 * Claim management enables distributed processing: multiple instances
 * of the same processor each claim different segments, preventing
 * double-processing.
 *
 * Every member takes the unit of work as a TRAILING parameter. Persistent
 * implementations read the adapter transaction off it
 * (`activeDrizzleTransaction(uow) ?? db`, and the equivalent per adapter) so
 * token updates and projection updates commit atomically. It is optional
 * because the lifecycle calls — `initializeSegments` and the startup `get`,
 * plus `resetTokens` — legitimately run outside any unit of work; those get
 * the plain writer.
 *
 * ── `U` IS WHAT THIS STORE DEMANDS OF THE TASK ─────────────────────────────
 *
 * A store that writes through a family's transaction says so by narrowing `U`
 * to that family's brand — `TokenStore<UnitOfWork & DrizzleUnitOfWork>` — and the
 * processor that wires it is where the compiler compares that demand against
 * the unit-of-work factory the host actually built. Mixing families is a build
 * error naming the factory to call. See `unit-of-work/persistence-family.ts`.
 *
 * THE MEMBERS ARE FUNCTION-TYPED FIELDS, NOT METHOD SHORTHAND, and that is
 * load-bearing rather than stylistic. TypeScript checks method-shorthand
 * parameters BIVARIANTLY, so a store demanding a branded task would have been
 * silently assignable to a slot expecting a bare one and the whole demand
 * would have been decoration. Written as fields, the parameter is checked
 * contravariantly and the refusal is real. It is also what the surface rules
 * already asked for: a shape is a type alias of function-signature fields.
 *
 * A BARE STORE DEMANDS NOTHING. `inMemoryTokenStore()` is a
 * `TokenStore<UnitOfWork>`, and contravariance makes that assignable into ANY
 * family's slot — which is exactly right, because a store with no transaction
 * to share cannot be in the wrong family.
 */
export type TokenStore<U extends UnitOfWork = UnitOfWork> = {
  /**
   * Store the token for a processor segment.
   * Called during PREPARE_COMMIT phase (same transaction as handler work).
   * The caller must own the claim for this segment.
   */
  store: (processorName: string, segment: number, token: TrackingToken, uow?: U) => Promise<void>

  /**
   * Get the current token for a processor segment.
   * Returns undefined if no token has been stored (start from beginning).
   */
  get: (processorName: string, segment: number, uow?: U) => Promise<TrackingToken | undefined>

  /**
   * Initialize token segments for a processor if they don't exist yet.
   * Called once during processor startup.
   */
  initializeSegments: (processorName: string, segmentCount: number, uow?: U) => Promise<void>

  /**
   * Claim a segment for processing. Returns the current token for
   * that segment. Throws `UnableToClaimTokenError` if the segment
   * is already claimed by another instance.
   */
  claimToken: (
    processorName: string,
    segment: number,
    ownerId: string,
    uow?: U,
  ) => Promise<TrackingToken | undefined>

  /**
   * Extend the claim lease for a segment without processing.
   * Called periodically to prevent claim expiry during long batches.
   */
  extendClaim: (processorName: string, segment: number, ownerId: string, uow?: U) => Promise<void>

  /**
   * Release a segment claim, allowing other instances to pick it up.
   */
  releaseClaim: (processorName: string, segment: number, ownerId: string, uow?: U) => Promise<void>

  /**
   * Fetch all segment IDs that have tokens for a processor.
   * Used during startup to discover available segments.
   */
  fetchSegments: (processorName: string, uow?: U) => Promise<number[]>

  /**
   * Fetch segment IDs that are not currently claimed by any instance.
   * Used to discover segments available for claiming.
   */
  fetchAvailableSegments: (processorName: string, uow?: U) => Promise<number[]>

  /**
   * Delete the token for a segment (e.g., after merging).
   */
  deleteToken: (processorName: string, segment: number, uow?: U) => Promise<void>
}

/**
 * Thrown when a segment claim cannot be acquired (already owned by another instance).
 */
export class UnableToClaimTokenError extends Error {
  constructor(processorName: string, segment: number) {
    super(`Unable to claim token for processor "${processorName}" segment ${segment}: already claimed`)
    this.name = "UnableToClaimTokenError"
  }
}

/**
 * In-memory token store with claim management.
 * Tokens and claims are lost on restart.
 *
 * @param claimTimeoutMs How long a claim lasts before it can be stolen (default: 10000ms)
 */
export function inMemoryTokenStore(claimTimeoutMs: number = 10000): TokenStore {
  type TokenEntry = {
    token: TrackingToken | undefined
    ownerId: string | null
    claimedAt: number
  }

  const entries = new Map<string, TokenEntry>()

  function key(processorName: string, segment: number): string {
    return `${processorName}:${segment}`
  }

  function getEntry(processorName: string, segment: number): TokenEntry | undefined {
    return entries.get(key(processorName, segment))
  }

  function isClaimExpired(entry: TokenEntry): boolean {
    if (!entry.ownerId) return true
    return Date.now() - entry.claimedAt > claimTimeoutMs
  }

  return {
    async store(processorName, segment, token) {
      const k = key(processorName, segment)
      const entry = entries.get(k)
      if (entry) {
        entry.token = token
        entry.claimedAt = Date.now() // Refresh claim on store
      } else {
        entries.set(k, { token, ownerId: null, claimedAt: Date.now() })
      }
    },

    async get(processorName, segment) {
      return getEntry(processorName, segment)?.token
    },

    async initializeSegments(processorName, segmentCount) {
      for (let i = 0; i < segmentCount; i++) {
        const k = key(processorName, i)
        if (!entries.has(k)) {
          entries.set(k, { token: undefined, ownerId: null, claimedAt: 0 })
        }
      }
    },

    async claimToken(processorName, segment, ownerId) {
      const k = key(processorName, segment)
      const entry = entries.get(k)

      if (!entry) {
        // Segment doesn't exist yet — create and claim
        const newEntry: TokenEntry = { token: undefined, ownerId, claimedAt: Date.now() }
        entries.set(k, newEntry)
        return undefined
      }

      if (entry.ownerId === ownerId) {
        // Already owned by this instance — refresh claim
        entry.claimedAt = Date.now()
        return entry.token
      }

      if (isClaimExpired(entry)) {
        // Claim expired — steal it
        entry.ownerId = ownerId
        entry.claimedAt = Date.now()
        return entry.token
      }

      throw new UnableToClaimTokenError(processorName, segment)
    },

    async extendClaim(processorName, segment, ownerId) {
      const entry = getEntry(processorName, segment)
      if (entry && entry.ownerId === ownerId) {
        entry.claimedAt = Date.now()
      }
    },

    async releaseClaim(processorName, segment, ownerId) {
      const entry = getEntry(processorName, segment)
      if (entry && entry.ownerId === ownerId) {
        entry.ownerId = null
        entry.claimedAt = 0
      }
    },

    async fetchSegments(processorName) {
      const segments: number[] = []
      const prefix = `${processorName}:`
      for (const k of entries.keys()) {
        if (k.startsWith(prefix)) {
          segments.push(parseInt(k.slice(prefix.length), 10))
        }
      }
      return segments.sort((a, b) => a - b)
    },

    async fetchAvailableSegments(processorName) {
      const segments: number[] = []
      const prefix = `${processorName}:`
      for (const [k, entry] of entries) {
        if (k.startsWith(prefix) && (!entry.ownerId || isClaimExpired(entry))) {
          segments.push(parseInt(k.slice(prefix.length), 10))
        }
      }
      return segments.sort((a, b) => a - b)
    },

    async deleteToken(processorName, segment) {
      entries.delete(key(processorName, segment))
    },
  }
}
