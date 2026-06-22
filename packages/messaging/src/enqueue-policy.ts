import type { DeadLetter, EnqueueDecision, EnqueuePolicy } from "./dead-letter-queue.js"

/**
 * Diagnostics key under which retry policies track how many times a letter has
 * been processed (initial failure counts as attempt 1).
 */
export const ATTEMPTS_DIAGNOSTIC = "attempts"

/**
 * Factory for the standard {@link EnqueueDecision}s, mirroring Axon's
 * `Decisions`. The two lifecycle moments share one decision type:
 *
 * - On an **initial** handler failure, `shouldEnqueue === true` means "park the
 *   event" and `false` means "drop it" (the error is swallowed, the token
 *   advances anyway).
 * - During **reprocessing**, `shouldEnqueue === true` means "requeue (keep it,
 *   still failing)" and `false` means "evict (done — succeeded or gave up)".
 *
 * Hence the deliberate aliases: `evict` ≡ `doNotEnqueue`, `requeue` ≡ `enqueue`.
 * They exist purely to read correctly at each call site.
 */
export const Decisions = {
  /** Park / keep the letter, recording the given cause and diagnostics. */
  enqueue(cause?: Error, diagnostics?: Record<string, unknown>): EnqueueDecision {
    return { shouldEnqueue: true, cause, diagnostics }
  },
  /** Alias of {@link enqueue}, read at reprocessing time ("still failing, keep it"). */
  requeue(cause?: Error, diagnostics?: Record<string, unknown>): EnqueueDecision {
    return { shouldEnqueue: true, cause, diagnostics }
  },
  /** Drop the letter without parking it (initial failure path). */
  doNotEnqueue(): EnqueueDecision {
    return { shouldEnqueue: false }
  },
  /** Alias of {@link doNotEnqueue}, read at reprocessing time ("done — remove it"). */
  evict(): EnqueueDecision {
    return { shouldEnqueue: false }
  },
  /**
   * Keep the letter in place without recording a new cause — used when a
   * reprocess attempt should neither evict nor overwrite the existing failure.
   */
  ignore(): EnqueueDecision {
    return { shouldEnqueue: true }
  },
} as const

/**
 * Default policy: always park a failed event, preserving the original cause.
 * Matches Axon's default `(letter, cause) -> Decisions.enqueue(cause)`.
 *
 * With no retry cap, letters stay until reprocessed successfully or evicted by
 * an operator — appropriate when every failure deserves manual inspection.
 */
export function alwaysEnqueuePolicy(): EnqueuePolicy {
  return {
    decide(_letter, cause) {
      return Decisions.enqueue(cause)
    },
  }
}

function attemptsOf(letter: DeadLetter): number {
  const raw = letter.diagnostics[ATTEMPTS_DIAGNOSTIC]
  return typeof raw === "number" ? raw : 0
}

/**
 * Options for {@link retryThenEvictPolicy}.
 */
export interface RetryThenEvictOptions {
  /**
   * Total processing attempts (including the first failure) before the letter
   * is evicted/dropped. `maxAttempts: 1` means "never retry".
   */
  maxAttempts: number
  /**
   * Classifies a cause as non-transient. Non-transient failures are parked once
   * for manual inspection (marked `retryable: false` in diagnostics) and never
   * counted against the retry budget — retrying a deterministic failure is
   * pointless. Optional; when omitted, every failure is treated as transient.
   */
  nonTransient?: (cause: Error) => boolean
}

/**
 * Caps automatic retries by counting attempts in the letter's diagnostics,
 * then evicting once the budget is exhausted. This is the Axon idiom: the
 * framework holds no retry counter — the policy authors it in diagnostics.
 *
 * - Initial failure → park with `attempts: 1` (unless `maxAttempts <= 1`, which
 *   evicts/drops immediately).
 * - Each reprocess failure → increment `attempts`; once `attempts >= maxAttempts`
 *   the letter is evicted (given up on).
 * - A `nonTransient` cause is parked once, never retried, and flagged for an
 *   operator.
 *
 * ```typescript
 * trackingProcessor("balances")
 *   .deadLetterQueue(dlq)
 *   .enqueuePolicy(retryThenEvictPolicy({ maxAttempts: 5 }))
 * ```
 */
export function retryThenEvictPolicy(options: RetryThenEvictOptions): EnqueuePolicy {
  const { maxAttempts, nonTransient } = options
  return {
    decide(letter, cause) {
      if (nonTransient?.(cause)) {
        return Decisions.enqueue(cause, { retryable: false })
      }
      const attempts = attemptsOf(letter) + 1
      if (attempts >= maxAttempts) {
        return Decisions.evict()
      }
      return Decisions.enqueue(cause, { [ATTEMPTS_DIAGNOSTIC]: attempts })
    },
  }
}
