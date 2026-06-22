import { qualifiedNameToString } from "@kronos-ts/common"
import type { DeadLetter } from "./dead-letter-queue.js"

/**
 * Observability hook for dead-letter lifecycle events. Implementations receive
 * a callback at each transition so operators can emit metrics, logs, or traces
 * (queue depth, dead-letter rate, reprocessing outcomes, overflow/backpressure).
 *
 * All callbacks are best-effort and synchronous from the queue's perspective —
 * implementations must not throw and should not block.
 */
export interface DeadLetterListener {
  /** A failed event was parked. `blocked` is true when parked only because an
   * earlier letter in its sequence failed (ordering gate), not a fresh failure. */
  onEnqueued(letter: DeadLetter, info: { blocked: boolean }): void
  /** A letter was removed after successful reprocessing or being given up on. */
  onEvicted(letter: DeadLetter): void
  /** A letter was re-parked after a reprocess attempt left it still failing. */
  onRequeued(letter: DeadLetter): void
  /** A reprocess attempt succeeded. */
  onReprocessSuccess(letter: DeadLetter): void
  /** A reprocess attempt failed; `decision` is what the policy chose next. */
  onReprocessFailure(letter: DeadLetter, cause: Error): void
  /** The queue rejected an enqueue because it was full (Axon backpressure). */
  onOverflow(sequenceIdentifier: string, cause: Error): void
}

/** A listener that does nothing. Default. */
export function noOpDeadLetterListener(): DeadLetterListener {
  return {
    onEnqueued() {},
    onEvicted() {},
    onRequeued() {},
    onReprocessSuccess() {},
    onReprocessFailure() {},
    onOverflow() {},
  }
}

function describe(letter: DeadLetter): string {
  return `${qualifiedNameToString(letter.message.name)} (seq "${letter.sequenceIdentifier}")`
}

/**
 * Logs each dead-letter transition. A reasonable default for getting visibility
 * without wiring a metrics backend.
 */
export function loggingDeadLetterListener(processorName: string): DeadLetterListener {
  const tag = `Dead letter queue "${processorName}":`
  return {
    onEnqueued(letter, info) {
      if (info.blocked) {
        console.warn(`${tag} blocked ${describe(letter)} — earlier letter in sequence failed`)
      } else {
        console.warn(`${tag} parked ${describe(letter)}:`, letter.cause)
      }
    },
    onEvicted(letter) {
      console.info(`${tag} evicted ${describe(letter)}`)
    },
    onRequeued(letter) {
      console.warn(`${tag} requeued ${describe(letter)} — still failing`)
    },
    onReprocessSuccess(letter) {
      console.info(`${tag} reprocessed ${describe(letter)}`)
    },
    onReprocessFailure(letter, cause) {
      console.warn(`${tag} reprocess failed for ${describe(letter)}:`, cause)
    },
    onOverflow(sequenceIdentifier, cause) {
      console.error(`${tag} overflow on sequence "${sequenceIdentifier}" — applying backpressure:`, cause)
    },
  }
}

/** Fan a single notification out to several listeners. */
export function multiDeadLetterListener(
  listeners: ReadonlyArray<DeadLetterListener>,
): DeadLetterListener {
  if (listeners.length === 0) return noOpDeadLetterListener()
  if (listeners.length === 1) return listeners[0]!
  const fanOut =
    <K extends keyof DeadLetterListener>(method: K) =>
    (...args: Parameters<DeadLetterListener[K]>) => {
      for (const l of listeners) {
        ;(l[method] as (...a: unknown[]) => void)(...args)
      }
    }
  return {
    onEnqueued: fanOut("onEnqueued"),
    onEvicted: fanOut("onEvicted"),
    onRequeued: fanOut("onRequeued"),
    onReprocessSuccess: fanOut("onReprocessSuccess"),
    onReprocessFailure: fanOut("onReprocessFailure"),
    onOverflow: fanOut("onOverflow"),
  }
}
