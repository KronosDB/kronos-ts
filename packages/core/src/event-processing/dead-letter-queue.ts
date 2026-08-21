import type { EventMessage } from "../messaging/messages.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * A dead letter — an event that failed processing and was parked
 * for later retry or manual intervention.
 */
export type DeadLetter = {
  /** The original event message that failed. */
  readonly message: EventMessage
  /** The error that caused the failure. */
  readonly cause: Error
  /** When this letter was first enqueued. */
  readonly enqueuedAt: number
  /** When this letter was last touched (enqueued or requeued). */
  readonly lastTouched: number
  /** Additional diagnostics metadata about the failure. */
  readonly diagnostics: Record<string, unknown>
  /** The sequence identifier — the lane — this letter belongs to. */
  readonly sequenceIdentifier: string
}

/**
 * The outcome of one reprocess attempt: keep the letter parked, or drop it.
 * `shouldEnqueue: true` requeues (still failing); `false` evicts (done).
 */
export type EnqueueDecision = {
  /** Whether to keep the letter parked. */
  readonly shouldEnqueue: boolean
  /** Updated cause, when the retry failed differently. */
  readonly cause?: Error
  /** Additional diagnostics to attach. */
  readonly diagnostics?: Record<string, unknown>
}

/**
 * A sequenced dead letter queue that maintains ordering within lanes.
 *
 * Events in the same lane (identified by `sequenceIdentifier`) are ordered by
 * insertion. When a lane has dead letters, subsequent events in that lane are
 * also parked, so ordering survives the failure.
 *
 * Every method takes the PROCESSING GROUP first and the unit of work last.
 *
 * The group is per CALL, not per construction. One queue object is one table
 * (or one map), and a table is shared by every processor pointed at it — so
 * which partition a call reads is a property of the CALLER, exactly as a token
 * store's `processorName` is. Baking it into the constructor made a host build
 * one queue object per processor over the same table, and made a single
 * `clear()` mean two different things depending on which object you held.
 *
 * The unit of work is optional because admin and drain paths (`clear`,
 * `sequenceIdentifiers`) legitimately run outside any unit of work; database
 * backed implementations read the adapter transaction off it
 * (`activeDrizzleTransaction(uow) ?? db`, and the equivalent per adapter) so
 * enqueue/evict/requeue commit in the same transaction as the batch that
 * produced them.
 *
 * `U` IS WHAT THIS QUEUE DEMANDS OF THE TASK, exactly as it is on `TokenStore`
 * — and for exactly the same reason, since a queue that parks a letter outside
 * the batch's transaction is the same silent split-brain a token store outside
 * it would be. The members are function-typed FIELDS rather than method
 * shorthand so the demand is checked contravariantly and therefore real; see
 * the note on `TokenStore` for why that distinction decides whether any of this
 * works at all.
 */
export type SequencedDeadLetterQueue<U extends UnitOfWork = UnitOfWork> = {
  /** Park a dead letter in its lane. */
  enqueue: (processingGroup: string, letter: DeadLetter, uow?: U) => Promise<void>

  /**
   * Park a letter ONLY if the lane already has parked letters — how a lane
   * stays ordered through a failure. The supplier is called only when the lane
   * exists. Returns true when the letter was parked.
   */
  enqueueIfPresent: (
    processingGroup: string,
    sequenceIdentifier: string,
    letterSupplier: () => DeadLetter,
    uow?: U,
  ) => Promise<boolean>

  /** Remove a dead letter from the queue (successfully reprocessed). */
  evict: (
    processingGroup: string,
    sequenceIdentifier: string,
    letter: DeadLetter,
    uow?: U,
  ) => Promise<void>

  /** Re-insert a dead letter at the front of its lane with updated properties. */
  requeue: (
    processingGroup: string,
    letter: DeadLetter,
    update?: Partial<Pick<DeadLetter, "cause" | "diagnostics">>,
    uow?: U,
  ) => Promise<void>

  /** Whether a lane has any dead letters. */
  contains: (processingGroup: string, sequenceIdentifier: string, uow?: U) => Promise<boolean>

  /** All dead letters in a lane, in insertion order. */
  deadLetterSequence: (
    processingGroup: string,
    sequenceIdentifier: string,
    uow?: U,
  ) => Promise<DeadLetter[]>

  /** All lane identifiers that have dead letters. */
  sequenceIdentifiers: (processingGroup: string, uow?: U) => Promise<string[]>

  /**
   * Process the oldest parked lane matching the filter.
   * For each letter in the lane:
   * - `{ shouldEnqueue: false }`: letter is evicted, continue
   * - `{ shouldEnqueue: true }`: letter is requeued, stop
   *
   * Returns true if a lane was processed.
   */
  process: (
    processingGroup: string,
    sequenceFilter: (sequenceId: string) => boolean,
    processingTask: (letter: DeadLetter) => Promise<EnqueueDecision>,
    uow?: U,
  ) => Promise<boolean>

  /** Total number of dead letters in the group. */
  size: (processingGroup: string, uow?: U) => Promise<number>

  /** Number of lanes with dead letters in the group. */
  amountOfSequences: (processingGroup: string, uow?: U) => Promise<number>

  /** Clear all dead letters in the group. */
  clear: (processingGroup: string, uow?: U) => Promise<void>

  /**
   * Whether the queue is full for the given lane — max lanes or max lane size
   * reached. Async so persistent backends can answer with a count query.
   */
  isFull: (processingGroup: string, sequenceIdentifier: string, uow?: U) => Promise<boolean>
}

/**
 * Creates an in-memory dead letter queue.
 *
 * @param options.maxSequences Maximum number of lanes per group (default: 1024)
 * @param options.maxSequenceSize Maximum letters per lane (default: 1024)
 */
export function inMemoryDeadLetterQueue(options?: {
  maxSequences?: number
  maxSequenceSize?: number
}): SequencedDeadLetterQueue {
  const maxSequences = options?.maxSequences ?? 1024
  const maxSequenceSize = options?.maxSequenceSize ?? 1024

  const groups = new Map<string, Map<string, DeadLetter[]>>()
  const processing = new Set<string>()

  function lanes(group: string): Map<string, DeadLetter[]> {
    let m = groups.get(group)
    if (!m) {
      m = new Map<string, DeadLetter[]>()
      groups.set(group, m)
    }
    return m
  }

  const queue: SequencedDeadLetterQueue = {
    async enqueue(group, letter) {
      const sequences = lanes(group)
      const seq = sequences.get(letter.sequenceIdentifier)
      if (seq) {
        if (seq.length >= maxSequenceSize) {
          throw new DeadLetterQueueOverflowError(
            `sequence "${letter.sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
          )
        }
        seq.push(letter)
      } else {
        if (sequences.size >= maxSequences) {
          throw new DeadLetterQueueOverflowError(`max sequences ${maxSequences} reached`)
        }
        sequences.set(letter.sequenceIdentifier, [letter])
      }
    },

    async enqueueIfPresent(group, sequenceIdentifier, letterSupplier) {
      const seq = lanes(group).get(sequenceIdentifier)
      if (!seq) return false
      if (seq.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      seq.push(letterSupplier())
      return true
    },

    async evict(group, sequenceIdentifier, letter) {
      const sequences = lanes(group)
      const seq = sequences.get(sequenceIdentifier)
      if (!seq) return
      const idx = seq.indexOf(letter)
      if (idx >= 0) seq.splice(idx, 1)
      if (seq.length === 0) sequences.delete(sequenceIdentifier)
    },

    async requeue(group, letter, update?) {
      const seq = lanes(group).get(letter.sequenceIdentifier)
      if (!seq) return

      const updated: DeadLetter = {
        ...letter,
        lastTouched: Date.now(),
        cause: update?.cause ?? letter.cause,
        diagnostics: update?.diagnostics
          ? { ...letter.diagnostics, ...update.diagnostics }
          : letter.diagnostics,
      }

      const idx = seq.indexOf(letter)
      if (idx >= 0) seq.splice(idx, 1)
      seq.unshift(updated)
    },

    async contains(group, sequenceIdentifier) {
      const seq = lanes(group).get(sequenceIdentifier)
      return seq !== undefined && seq.length > 0
    },

    async deadLetterSequence(group, sequenceIdentifier) {
      return lanes(group).get(sequenceIdentifier) ?? []
    },

    async sequenceIdentifiers(group) {
      return [...lanes(group).keys()]
    },

    async process(group, sequenceFilter, processingTask, uow) {
      const sequences = lanes(group)
      // Find oldest untaken lane matching filter
      let oldestId: string | undefined
      let oldestTime = Infinity

      for (const [id, letters] of sequences) {
        if (processing.has(`${group}:${id}`)) continue
        if (letters.length === 0) continue
        if (!sequenceFilter(id)) continue

        const firstTouched = letters[0]!.lastTouched
        if (firstTouched < oldestTime) {
          oldestTime = firstTouched
          oldestId = id
        }
      }

      if (!oldestId) return false

      processing.add(`${group}:${oldestId}`)
      try {
        const letters = sequences.get(oldestId)
        if (!letters) return false

        // Process letters in order — take a snapshot of current letters
        const snapshot = [...letters]
        for (const letter of snapshot) {
          const decision = await processingTask(letter)
          if (decision.shouldEnqueue) {
            // Requeue and stop — the lane is still blocked
            await queue.requeue(
              group,
              letter,
              { cause: decision.cause, diagnostics: decision.diagnostics },
              uow,
            )
            return true
          }
          await queue.evict(group, oldestId, letter, uow)
        }
        return true
      } finally {
        processing.delete(`${group}:${oldestId}`)
      }
    },

    async size(group) {
      let total = 0
      for (const letters of lanes(group).values()) total += letters.length
      return total
    },

    async amountOfSequences(group) {
      return lanes(group).size
    },

    async clear(group) {
      groups.delete(group)
      for (const key of [...processing]) {
        if (key.startsWith(`${group}:`)) processing.delete(key)
      }
    },

    async isFull(group, sequenceIdentifier) {
      const sequences = lanes(group)
      const seq = sequences.get(sequenceIdentifier)
      if (seq) return seq.length >= maxSequenceSize
      return sequences.size >= maxSequences
    },
  }

  return queue
}

/**
 * Thrown when the dead letter queue is full.
 */
export class DeadLetterQueueOverflowError extends Error {
  constructor(message: string) {
    super(`Dead letter queue overflow: ${message}`)
    this.name = "DeadLetterQueueOverflowError"
  }
}

/**
 * Creates a DeadLetter from a failed event.
 */
export function deadLetter(
  message: EventMessage,
  cause: Error,
  sequenceIdentifier: string,
  diagnostics?: Record<string, unknown>,
): DeadLetter {
  const now = Date.now()
  return {
    message,
    cause,
    enqueuedAt: now,
    lastTouched: now,
    diagnostics: diagnostics ?? {},
    sequenceIdentifier,
  }
}
