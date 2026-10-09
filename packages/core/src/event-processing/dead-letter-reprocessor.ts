import type { DeadLetter, SequencedDeadLetterQueue } from "./dead-letter-queue.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Replays a single dead letter through its handlers. Resolves on success and
 * rejects with the handler's error on failure. Supplied by the owning
 * processor, which builds a handler context from the reprocess unit of work
 * exactly as it does for live delivery.
 */
export type DeadLetterReplay<U extends UnitOfWork = UnitOfWork> = (letter: DeadLetter, uow: U) => Promise<void>

export type DeadLetterReprocessorOptions<U extends UnitOfWork = UnitOfWork> = {
  queue: SequencedDeadLetterQueue<U>
  /** The queue partition this reprocessor drains — the processor's name. */
  processingGroup: string
  replay: DeadLetterReplay<U>
  /**
   * Mints the unit of work each LETTER is replayed in. The queue runs one per
   * letter: the handlers' writes and the letter's eviction commit together, and
   * a letter that fails rolls its unit of work back before it is requeued.
   */
  unitOfWork: () => U
}

export type DeadLetterReprocessor = {
  /**
   * Reprocess the oldest parked lane matching the filter, once. Walks the lane
   * head-to-tail, each letter in a unit of work of its own: a letter that
   * replays successfully is evicted, committing with the handlers' writes; the
   * first that fails has its unit of work rolled back, is requeued and stops the
   * walk, keeping the lane ordered. Returns true if a lane was processed.
   */
  reprocess(filter?: (sequenceId: string) => boolean): Promise<boolean>
  /**
   * One drain pass over every currently-parked lane matching the filter, each
   * attempted at most once (a still-failing lane is left requeued, not retried
   * in a hot loop). Returns the number of lanes attempted.
   */
  reprocessAll(filter?: (sequenceId: string) => boolean): Promise<number>
}

/**
 * Bind a {@link SequencedDeadLetterQueue} to a replay function: drive parked
 * events back through their handlers, evicting on success and requeuing on
 * continued failure.
 *
 * Evict-on-success / requeue-on-failure is the whole rule, and it is not
 * configurable. A retry BUDGET is a thing an operator spends against a parked
 * letter, not a rule the framework can guess before the failure exists — a
 * budget configured up front turns "we kept the evidence" into "we deleted it
 * on the fifth try".
 */
export function deadLetterReprocessor<U extends UnitOfWork = UnitOfWork>(
  options: DeadLetterReprocessorOptions<U>,
): DeadLetterReprocessor {
  const { queue, processingGroup, replay, unitOfWork } = options

  async function reprocess(filter: (sequenceId: string) => boolean = () => true): Promise<boolean> {
    return queue.process(
      processingGroup,
      (sequenceId) => filter(sequenceId),
      // `uow` is the letter's own unit of work, minted by the queue from the
      // factory below, so the replay is typed against whatever it produces.
      async (letter, uow) => {
        try {
          await replay(letter, uow)
          return { shouldEnqueue: false }
        } catch (err) {
          // A decision, not a rethrow: the queue turns it into a rollback of
          // this letter's unit of work, then requeues the letter with the cause.
          const cause = err instanceof Error ? err : new Error(String(err))
          return { shouldEnqueue: true, cause }
        }
      },
      unitOfWork,
    )
  }

  async function reprocessAll(
    filter: (sequenceId: string) => boolean = () => true,
  ): Promise<number> {
    const ids = (await queue.sequenceIdentifiers(processingGroup)).filter(filter)
    let attempted = 0
    for (const id of ids) {
      const processed = await reprocess((s) => s === id)
      if (processed) attempted++
    }
    return attempted
  }

  return { reprocess, reprocessAll }
}
