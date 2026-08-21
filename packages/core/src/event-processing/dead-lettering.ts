import { qualifiedNameToString } from "../messaging/messages.js"
import type { EventHandler } from "./handler.js"
import type { SequencedEvent } from "./source.js"
import type { Sequence } from "./sequence.js"
import { type SequencedDeadLetterQueue, deadLetter } from "./dead-letter-queue.js"
import type { EventHandlerContext } from "./context.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/** One handler and the context built for it, for this event. */
export type DeadLetteringTarget = {
  readonly definition: EventHandler<any, any>
  readonly context: EventHandlerContext
}

/**
 * Event delivery with lane-preserving parking.
 *
 * When a handler fails, the event is parked in its lane and the batch stays
 * committable, so the token advances past the poison pill instead of
 * redelivering it forever. When a lane ALREADY has parked letters, the next
 * event in that lane is parked without being delivered — which is the whole
 * point of a lane: order is preserved through the failure.
 *
 * There is no enqueue policy. Parking is what a queue is for; a policy that
 * could decide not to park is a policy that silently drops events, and the
 * honest way to say "do not park" is to configure no queue. Giving up on a
 * letter is an operator decision, taken against the parked letter, not a rule
 * configured before the failure happened.
 */
export function deadLetteringDelivery<U extends UnitOfWork = UnitOfWork>(options: {
  queue: SequencedDeadLetterQueue<U>
  /** The queue partition this delivery parks into — the processor's name. */
  processingGroup: string
  /** Which lane each event belongs to. Required — parking is a lane operation. */
  sequence: Sequence
}) {
  const { queue, processingGroup, sequence } = options

  return {
    /**
     * Deliver an event to its handlers, parking on failure.
     *
     * `uow` is the batch's unit of work, handed to every queue call so the
     * enqueue commits in the same transaction as the token update.
     */
    async deliver(
      sequencedEvent: SequencedEvent,
      targets: ReadonlyArray<DeadLetteringTarget>,
      uow?: U,
    ): Promise<void> {
      const event = sequencedEvent.event
      const lane = sequence(event)

      // If this lane already has parked letters, park this event too rather
      // than delivering it out of order. A full-queue rejection propagates as
      // backpressure: the batch fails and is redelivered.
      const blocked = await queue.enqueueIfPresent(
        processingGroup,
        lane,
        () =>
          deadLetter(event, new Error("Blocked: previous event in sequence failed"), lane, {
            blocked: true,
            position: Number(sequencedEvent.sequence),
          }),
        uow,
      )
      if (blocked) return

      for (const target of targets) {
        try {
          await target.definition.handler(
            { ...event, sequence: sequencedEvent.sequence },
            target.context,
          )
        } catch (err) {
          const cause = err instanceof Error ? err : new Error(String(err))
          await queue.enqueue(
            processingGroup,
            deadLetter(event, cause, lane, {
              position: Number(sequencedEvent.sequence),
              handlerName: qualifiedNameToString(target.definition.descriptor.name),
            }),
            uow,
          )
          // The error is consumed by the queue — the lane is parked, and the
          // batch stays committable so the token can advance past it.
          return
        }
      }
    },
  }
}
