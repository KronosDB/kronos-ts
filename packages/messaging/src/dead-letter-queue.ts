import type { EventMessage } from "./message.js"

/**
 * A dead letter — an event that failed processing and was parked
 * for later retry or manual intervention.
 */
export interface DeadLetter {
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
  /** The sequence identifier this letter belongs to. */
  readonly sequenceIdentifier: string
}

/**
 * Decision on whether to enqueue a failed event as a dead letter.
 */
export interface EnqueueDecision {
  /** Whether to enqueue the failed event. */
  readonly shouldEnqueue: boolean
  /** Optional updated cause (may differ from original). */
  readonly cause?: Error
  /** Additional diagnostics to attach. */
  readonly diagnostics?: Record<string, unknown>
}

/**
 * Policy that determines whether a failed event should be dead-lettered.
 */
export interface EnqueuePolicy {
  /**
   * Decide whether to enqueue a failed event.
   * @param letter The dead letter candidate
   * @param cause The error that caused the failure
   */
  decide(letter: DeadLetter, cause: Error): EnqueueDecision
}

/**
 * A sequenced dead letter queue that maintains ordering within sequences.
 *
 * Events in the same sequence (identified by `sequenceIdentifier`) are
 * ordered by insertion. When a sequence has dead letters, subsequent
 * events in that sequence are also dead-lettered to preserve ordering.
 *
 * Typical sequence identifier: aggregate ID or correlation ID.
 *
 * Database-backed implementations participate in the active UnitOfWork via
 * the ALS-managed transaction (read through `getActiveTransaction()` /
 * `getResource(TRANSACTION_KEY)`); no `ProcessingContext` parameter is
 * threaded through the public surface.
 */
export interface SequencedDeadLetterQueue {
  /**
   * Enqueue a dead letter into the given sequence.
   */
  enqueue(letter: DeadLetter): Promise<void>

  /**
   * Enqueue a dead letter only if the sequence already has dead letters.
   * Used to block subsequent events in a failed sequence.
   * The supplier is only called if the sequence exists (avoids creating
   * unnecessary objects).
   * Returns true if the letter was enqueued (sequence existed).
   */
  enqueueIfPresent(sequenceIdentifier: string, letterSupplier: () => DeadLetter): Promise<boolean>

  /**
   * Remove a dead letter from the queue (successfully reprocessed).
   */
  evict(sequenceIdentifier: string, letter: DeadLetter): Promise<void>

  /**
   * Re-insert a dead letter at the front of its sequence with updated properties.
   */
  requeue(
    letter: DeadLetter,
    update?: Partial<Pick<DeadLetter, "cause" | "diagnostics">>,
  ): Promise<void>

  /**
   * Check if a sequence has any dead letters.
   */
  contains(sequenceIdentifier: string): Promise<boolean>

  /**
   * Get all dead letters in a sequence, in insertion order.
   */
  deadLetterSequence(sequenceIdentifier: string): Promise<DeadLetter[]>

  /**
   * Get all sequence identifiers that have dead letters.
   */
  sequenceIdentifiers(): Promise<string[]>

  /**
   * Process the oldest dead letter sequence matching the filter.
   * For each letter in the sequence:
   * - If processingTask returns `{ shouldEnqueue: false }`: letter is evicted, continue
   * - If processingTask returns `{ shouldEnqueue: true }`: letter is requeued, stop
   *
   * Returns true if a sequence was processed.
   */
  process(
    sequenceFilter: (sequenceId: string) => boolean,
    processingTask: (letter: DeadLetter) => Promise<EnqueueDecision>,
  ): Promise<boolean>

  /** Total number of dead letters across all sequences. */
  size(): Promise<number>

  /** Number of sequences with dead letters. */
  amountOfSequences(): Promise<number>

  /** Clear all dead letters. */
  clear(): Promise<void>

  /**
   * Check if the queue is full for the given sequence.
   * Returns true if max sequences or max sequence size is reached.
   *
   * Async so persistent backends can answer with a count query.
   */
  isFull(sequenceIdentifier: string): Promise<boolean>
}

/**
 * Creates an in-memory dead letter queue.
 *
 * @param options.maxSequences Maximum number of sequences (default: 1024)
 * @param options.maxSequenceSize Maximum letters per sequence (default: 1024)
 */
export function createInMemoryDeadLetterQueue(options?: {
  maxSequences?: number
  maxSequenceSize?: number
}): SequencedDeadLetterQueue {
  const maxSequences = options?.maxSequences ?? 1024
  const maxSequenceSize = options?.maxSequenceSize ?? 1024

  const sequences = new Map<string, DeadLetter[]>()
  const processing = new Set<string>()

  return {
    async enqueue(letter) {
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
          throw new DeadLetterQueueOverflowError(
            `max sequences ${maxSequences} reached`,
          )
        }
        sequences.set(letter.sequenceIdentifier, [letter])
      }
    },

    async enqueueIfPresent(sequenceIdentifier, letterSupplier) {
      const seq = sequences.get(sequenceIdentifier)
      if (!seq) return false
      if (seq.length >= maxSequenceSize) {
        throw new DeadLetterQueueOverflowError(
          `sequence "${sequenceIdentifier}" has reached max size ${maxSequenceSize}`,
        )
      }
      seq.push(letterSupplier())
      return true
    },

    async evict(sequenceIdentifier, letter) {
      const seq = sequences.get(sequenceIdentifier)
      if (!seq) return
      const idx = seq.indexOf(letter)
      if (idx >= 0) seq.splice(idx, 1)
      if (seq.length === 0) sequences.delete(sequenceIdentifier)
    },

    async requeue(letter, update?) {
      const seq = sequences.get(letter.sequenceIdentifier)
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

    async contains(sequenceIdentifier) {
      const seq = sequences.get(sequenceIdentifier)
      return seq !== undefined && seq.length > 0
    },

    async deadLetterSequence(sequenceIdentifier) {
      return sequences.get(sequenceIdentifier) ?? []
    },

    async sequenceIdentifiers() {
      return [...sequences.keys()]
    },

    async process(sequenceFilter, processingTask) {
      // Find oldest untaken sequence matching filter
      let oldestId: string | undefined
      let oldestTime = Infinity

      for (const [id, letters] of sequences) {
        if (processing.has(id)) continue
        if (letters.length === 0) continue
        if (!sequenceFilter(id)) continue

        const firstTouched = letters[0]!.lastTouched
        if (firstTouched < oldestTime) {
          oldestTime = firstTouched
          oldestId = id
        }
      }

      if (!oldestId) return false

      processing.add(oldestId)
      try {
        const letters = sequences.get(oldestId)
        if (!letters) return false

        // Process letters in order — take a snapshot of current letters
        const snapshot = [...letters]
        for (const letter of snapshot) {
          const decision = await processingTask(letter)
          if (decision.shouldEnqueue) {
            // Requeue and stop — sequence is still blocked
            await this.requeue(letter, {
              cause: decision.cause,
              diagnostics: decision.diagnostics,
            })
            return true
          }
          // Evict — successfully reprocessed
          await this.evict(oldestId!, letter)
        }
        return true
      } finally {
        processing.delete(oldestId)
      }
    },

    async size() {
      let total = 0
      for (const letters of sequences.values()) {
        total += letters.length
      }
      return total
    },

    async amountOfSequences() {
      return sequences.size
    },

    async clear() {
      sequences.clear()
      processing.clear()
    },

    async isFull(sequenceIdentifier) {
      const seq = sequences.get(sequenceIdentifier)
      if (seq) {
        return seq.length >= maxSequenceSize
      }
      return sequences.size >= maxSequences
    },
  }
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
export function createDeadLetter(
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
