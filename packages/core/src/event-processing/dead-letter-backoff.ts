import type { DeadLetter, EnqueueDecision, SequencedDeadLetterQueue } from "./dead-letter-queue.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * What a policy is told about a failure that just happened.
 *
 * `attempts` counts the failures of THIS letter so far, including this one: 1
 * when a live failure first parks it, 2 after the first replay that fails
 * again, and so on. `letter` carries the failure's own `cause` — the error
 * that was just thrown, not the one it replaced — and the letter's diagnostics.
 *
 * Test the cause by `cause.name`, not `instanceof`. A cause read back from a
 * persistent queue is rebuilt as a plain `Error` carrying the original name and
 * message, so a class check that holds for a live failure fails for the same
 * letter after a restart.
 */
export type DeadLetterBackoffFailure = {
  readonly attempts: number
  readonly letter: DeadLetter
}

/**
 * Decides when a failed letter may be replayed again: the number of
 * milliseconds from now, or `"hold"` to keep the lane parked until
 * {@link deadLetterBackoff}'s `retryNow` releases it.
 */
export type DeadLetterBackoffPolicy = (failure: DeadLetterBackoffFailure) => number | "hold"

/** The backoff state a wrapped queue keeps on a letter, under `diagnostics.backoff`. */
type BackoffState = {
  readonly attempts: number
  /** Epoch ms from which the letter is due, `"hold"` until released, `null` when due. */
  readonly retryAt: number | "hold" | null
}

/** The state on a letter, or undefined when it has none (never failed since it parked). */
function backoffOf(letter: DeadLetter): BackoffState | undefined {
  const state = letter.diagnostics["backoff"]
  if (typeof state !== "object" || state === null) return undefined
  const { attempts, retryAt } = state as { attempts?: unknown; retryAt?: unknown }
  return {
    attempts: typeof attempts === "number" ? attempts : 0,
    retryAt: typeof retryAt === "number" || retryAt === "hold" ? retryAt : null,
  }
}

/** Due: no state, a `null` retry time, or a retry time that has passed. Never while held. */
function isDue(letter: DeadLetter): boolean {
  const retryAt = backoffOf(letter)?.retryAt
  if (retryAt === undefined || retryAt === null) return true
  if (retryAt === "hold") return false
  return retryAt <= Date.now()
}

/**
 * Wrap a {@link SequencedDeadLetterQueue} so a lane that keeps failing is not
 * replayed again straight away: after each failure the lane's head letter is
 * not due until the policy's delay has passed, and `process` skips lanes whose
 * head is not due.
 *
 * This DELAYS and nothing else. It is not a retry budget — no letter is ever
 * evicted, dropped or given up on because it failed too often, however many
 * attempts it has. That is the stance `deadLetterReprocessor` takes: a budget
 * is something an operator spends against a parked letter, not a rule to guess
 * before the failure exists.
 *
 * The state is ONE key on the letter's diagnostics,
 * `backoff: { attempts, retryAt }`, so it persists wherever diagnostics do and
 * needs no schema of its own. `attempts` counts this letter's failures
 * including the latest; `retryAt` is the epoch ms it becomes due, `"hold"`, or
 * `null` (due). A letter with no `backoff` key is due. Counts are per letter:
 * when a head is evicted, the letter behind it starts again at no attempts.
 *
 * - `enqueue` — the live failure that parks a lane — stamps `attempts: 1` and
 *   the policy's first delay on the letter.
 * - `enqueueIfPresent` is untouched. Those letters were never attempted, so
 *   they are due as soon as they become the head.
 * - `process` skips a lane whose head is not due, and stamps the next attempt
 *   on a letter whose replay fails again. The stamp rides on the decision's
 *   `diagnostics`, which every queue implementation applies on requeue; it is
 *   not done by wrapping `requeue`, which an implementation calls on itself and
 *   a wrapper never sees.
 * - `retryNow` makes a lane's head due at once. Everything else passes through.
 *
 * `"hold"` keeps a lane parked until `retryNow`. While a lane is held, the
 * events that arrive for it keep parking behind its head, so a long hold can
 * fill the lane to the queue's `maxSequenceSize`. The next `enqueueIfPresent`
 * then throws `DeadLetterQueueOverflowError`, and `deadLetteringDelivery` does
 * not catch it: it propagates out of the batch, which rolls back, the
 * processor's token does not advance, and the same batch is redelivered after
 * the poll delay (twice the polling interval) for as long as the lane stays
 * full. That stalls the whole processor, not just the held lane, with nothing
 * dropped — the same as having no queue. `retryNow` plus a reprocess, or
 * clearing the lane, is what lets it move again.
 *
 * Time is `Date.now()`, read on whichever node does the work, so with a shared
 * persistent queue the nodes' clocks should agree to well within the delays.
 *
 * @example
 * ```ts
 * const exponential = exponentialBackoff({ initialDelayMs: 30_000 })
 * const queue = deadLetterBackoff(postgresDeadLetterQueue(pg), (failure) =>
 *   failure.letter.cause.name === "ValidationError" ? "hold" : exponential(failure),
 * )
 *
 * // later, from an operator action:
 * await queue.retryNow("order-projection", lane)
 * ```
 */
export function deadLetterBackoff<U extends UnitOfWork>(
  queue: SequencedDeadLetterQueue<U>,
  policy: DeadLetterBackoffPolicy,
): SequencedDeadLetterQueue<U> & {
  /**
   * Make the lane's head due now. Returns false when the lane has no parked
   * letters.
   *
   * The head's `attempts` are kept, so a policy that reads them still sees the
   * history. It does not replay anything, and it does update the letter's
   * `lastTouched` — `requeue` does — which puts the lane at the back of the
   * oldest-first order `process` picks from. To replay the lane immediately,
   * call this and then `processor.reprocessDeadLetters((id) => id === lane)`.
   */
  retryNow(processingGroup: string, sequenceIdentifier: string, uow?: U): Promise<boolean>
} {
  /** The state to stamp for a failure: the policy's answer, resolved to a retry time. */
  function stamp(failure: DeadLetterBackoffFailure): BackoffState {
    const delay = policy(failure)
    return {
      attempts: failure.attempts,
      retryAt: delay === "hold" ? "hold" : Date.now() + Math.max(0, delay),
    }
  }

  return {
    ...queue,

    async enqueue(group, letter, uow) {
      const backoff = stamp({ attempts: 1, letter })
      await queue.enqueue(
        group,
        { ...letter, diagnostics: { ...letter.diagnostics, backoff } },
        uow,
      )
    },

    async process(group, sequenceFilter, processingTask, unitOfWork) {
      return queue.process(
        group,
        (sequenceId, head) => sequenceFilter(sequenceId, head) && isDue(head),
        async (letter, uow) => {
          const decision = await processingTask(letter, uow)
          if (!decision.shouldEnqueue) return decision
          const attempts = (backoffOf(letter)?.attempts ?? 0) + 1
          const backoff = stamp({
            attempts,
            letter: { ...letter, cause: decision.cause ?? letter.cause },
          })
          return {
            ...decision,
            diagnostics: { ...decision.diagnostics, backoff },
          } satisfies EnqueueDecision
        },
        unitOfWork,
      )
    },

    async retryNow(group, sequenceIdentifier, uow) {
      const head = (await queue.deadLetterSequence(group, sequenceIdentifier, uow))[0]
      if (head === undefined) return false
      const attempts = backoffOf(head)?.attempts ?? 0
      await queue.requeue(
        group,
        head,
        { diagnostics: { backoff: { attempts, retryAt: null } } },
        uow,
      )
      return true
    },
  }
}

/**
 * Exponential backoff for {@link deadLetterBackoff}: the delay before attempt
 * `n + 1` is `initialDelayMs × multiplier^(n − 1)`, capped at `maxDelayMs`,
 * then spread by ±25% so lanes that failed together do not all come due
 * together.
 *
 * The jitter is applied after the cap, so a capped delay can land up to 25%
 * either side of `maxDelayMs`.
 *
 * @param options.initialDelayMs Delay after the first failure (default: 10 000)
 * @param options.maxDelayMs Cap before jitter (default: 3 600 000, one hour)
 * @param options.multiplier Growth per failure (default: 2)
 */
export function exponentialBackoff(options?: {
  initialDelayMs?: number
  maxDelayMs?: number
  multiplier?: number
}): DeadLetterBackoffPolicy {
  const initialDelayMs = options?.initialDelayMs ?? 10_000
  const maxDelayMs = options?.maxDelayMs ?? 3_600_000
  const multiplier = options?.multiplier ?? 2
  return ({ attempts }) => {
    const delay = Math.min(initialDelayMs * multiplier ** (attempts - 1), maxDelayMs)
    return delay * (0.75 + Math.random() * 0.5)
  }
}
