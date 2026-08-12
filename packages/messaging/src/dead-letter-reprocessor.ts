import { emptyMetadata } from "@kronos-ts/common"
import type { DeadLetter, SequencedDeadLetterQueue, EnqueuePolicy } from "./dead-letter-queue.js"
import { Decisions, alwaysEnqueuePolicy } from "./enqueue-policy.js"
import { type DeadLetterListener, noOpDeadLetterListener } from "./dead-letter-listener.js"
import type { UoWRunner } from "./unit-of-work.js"
import { runInNewUoW } from "./unit-of-work.js"

/**
 * Replays a single dead letter through its handlers. Resolves on success and
 * rejects with the handler's error on failure. Supplied by the owning
 * processor, which re-establishes the same ALS resources (state manager,
 * command/query bus) it uses for live delivery.
 */
export type DeadLetterReplay = (letter: DeadLetter) => Promise<void>

export interface DeadLetterReprocessorOptions {
  queue: SequencedDeadLetterQueue
  replay: DeadLetterReplay
  /** Decides requeue-vs-evict after a failed retry. Default: always requeue. */
  policy?: EnqueuePolicy
  /** Runs each reprocess inside a UnitOfWork so the claim/evict/requeue and any
   * handler side effects commit in one transaction. Default: a fresh UoW. */
  unitOfWorkRunner?: UoWRunner
  listener?: DeadLetterListener
}

export interface DeadLetterReprocessor {
  /**
   * Reprocess the oldest dead-letter sequence matching the filter, once.
   * Walks the sequence head-to-tail: each letter that replays successfully is
   * evicted; the first that fails is requeued (per policy) and stops the walk,
   * keeping the sequence ordered. Returns true if a sequence was processed.
   */
  reprocess(filter?: (sequenceId: string) => boolean): Promise<boolean>
  /**
   * One drain pass over every currently-parked sequence matching the filter,
   * each attempted at most once (a still-failing sequence is left requeued, not
   * retried in a hot loop). Returns the number of sequences attempted.
   */
  reprocessAll(filter?: (sequenceId: string) => boolean): Promise<number>
}

/**
 * Binds a {@link SequencedDeadLetterQueue} to a replay function, producing the
 * Axon `SequencedDeadLetterProcessor` capability: drive parked events back
 * through their handlers, evicting on success and requeuing on continued
 * failure. Trigger it manually or on a schedule (see the processor's
 * `dlqRetryIntervalMs`).
 */
export function deadLetterReprocessor(
  options: DeadLetterReprocessorOptions,
): DeadLetterReprocessor {
  const {
    queue,
    replay,
    policy = alwaysEnqueuePolicy(),
    unitOfWorkRunner = runInNewUoW,
    listener = noOpDeadLetterListener(),
  } = options

  async function reprocess(filter: (sequenceId: string) => boolean = () => true): Promise<boolean> {
    return unitOfWorkRunner(emptyMetadata(), async () =>
      queue.process(filter, async (letter) => {
        try {
          await replay(letter)
          listener.onReprocessSuccess(letter)
          listener.onEvicted(letter)
          return Decisions.evict()
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          listener.onReprocessFailure(letter, error)
          const decision = policy.decide(letter, error)
          if (decision.shouldEnqueue) {
            listener.onRequeued(letter)
          } else {
            listener.onEvicted(letter)
          }
          return decision
        }
      }),
    )
  }

  async function reprocessAll(filter: (sequenceId: string) => boolean = () => true): Promise<number> {
    const ids = (await queue.sequenceIdentifiers()).filter(filter)
    let attempted = 0
    for (const id of ids) {
      const processed = await reprocess((s) => s === id)
      if (processed) attempted++
    }
    return attempted
  }

  return { reprocess, reprocessAll }
}
