import { qualifiedNameToString } from "@kronos-ts/common"
import type { EventHandlerRegistration } from "./handler.js"
import type { SequencedEvent } from "./event-source.js"
import { type SequencingPolicy, defaultSequencingPolicy } from "./sequencing-policy.js"
import {
  type SequencedDeadLetterQueue,
  type EnqueuePolicy,
  deadLetter,
  DeadLetterQueueOverflowError,
} from "./dead-letter-queue.js"
import { alwaysEnqueuePolicy } from "./enqueue-policy.js"
import {
  type DeadLetterListener,
  noOpDeadLetterListener,
} from "./dead-letter-listener.js"
import { EVENT_HANDLER_CONTEXT } from "./handler-context.js"

/**
 * Options for dead-lettering event handler wrapper.
 */
export interface DeadLetteringOptions {
  /** The dead letter queue to use. */
  queue: SequencedDeadLetterQueue
  /** Policy deciding whether to dead-letter a failed event. Default: always. */
  policy?: EnqueuePolicy
  /**
   * Decides which ordered sequence an event belongs to. Events in the same
   * sequence are ordered — if one fails, subsequent ones are blocked.
   * Default: {@link defaultSequencingPolicy} (first tag value, else event name).
   */
  sequencingPolicy?: SequencingPolicy
  /** Observability hook for dead-letter lifecycle events. Default: no-op. */
  listener?: DeadLetterListener
}

/**
 * Wraps event delivery with dead-letter support.
 *
 * When a handler fails:
 * 1. Creates a DeadLetter and consults the EnqueuePolicy
 * 2. If policy says enqueue: adds to DLQ, continues with next event
 * 3. If policy says don't enqueue: error is swallowed
 *
 * When processing an event whose sequence already has dead letters:
 * - The event is automatically dead-lettered (sequence is blocked)
 * - This preserves ordering within the sequence
 */
export function deadLetteringDelivery(options: DeadLetteringOptions) {
  const {
    queue,
    policy = alwaysEnqueuePolicy(),
    sequencingPolicy = defaultSequencingPolicy,
    listener = noOpDeadLetterListener(),
  } = options

  return {
    /**
     * Deliver an event to handlers, with dead-letter support.
     *
     * The DLQ participates in any active transaction via ALS — both the
     * caller and the DLQ implementation read transactional state from the
     * UnitOfWork ALS store, no explicit ProcessingContext is threaded.
     */
    async deliver(
      sequencedEvent: SequencedEvent,
      handlers: Array<EventHandlerRegistration<any>>,
    ): Promise<void> {
      const event = sequencedEvent.event
      const seqId = sequencingPolicy(event)

      // If this sequence already has dead letters, block this event too —
      // preserving per-sequence ordering. A full-queue rejection here
      // propagates as backpressure (see enqueue path below).
      let blockedLetter: ReturnType<typeof deadLetter> | undefined
      const blocked = await withOverflowReported(seqId, () =>
        queue.enqueueIfPresent(seqId, () => {
          blockedLetter = deadLetter(
            event,
            new Error("Blocked: previous event in sequence failed"),
            seqId,
            { blocked: true, position: Number(sequencedEvent.sequence) },
          )
          return blockedLetter
        }),
      )
      if (blocked) {
        if (blockedLetter) listener.onEnqueued(blockedLetter, { blocked: true })
        return
      }

      // Try to deliver to all handlers
      for (const reg of handlers) {
        try {
          await reg.handler({ ...event, sequence: sequencedEvent.sequence }, EVENT_HANDLER_CONTEXT)
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          const letter = deadLetter(event, error, seqId, {
            position: Number(sequencedEvent.sequence),
            handlerName: qualifiedNameToString(reg.descriptor.name),
          })

          const decision = policy.decide(letter, error)
          if (decision.shouldEnqueue) {
            const enqueued = {
              ...letter,
              cause: decision.cause ?? letter.cause,
              diagnostics: decision.diagnostics
                ? { ...letter.diagnostics, ...decision.diagnostics }
                : letter.diagnostics,
            }
            // A full queue throws DeadLetterQueueOverflowError, which propagates
            // to stall and redeliver the batch (Axon backpressure) — surfaced
            // via the listener rather than silently looping.
            await withOverflowReported(seqId, () => queue.enqueue(enqueued))
            listener.onEnqueued(enqueued, { blocked: false })
          }
          // Error is consumed by DLQ (parked or dropped) — don't propagate.
          return
        }
      }
    },
  }

  async function withOverflowReported<T>(seqId: string, op: () => Promise<T>): Promise<T> {
    try {
      return await op()
    } catch (err) {
      if (err instanceof DeadLetterQueueOverflowError) {
        listener.onOverflow(seqId, err)
      }
      throw err
    }
  }
}
