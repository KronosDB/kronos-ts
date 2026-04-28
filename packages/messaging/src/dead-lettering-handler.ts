import { qualifiedNameToString } from "@kronos-ts/common"
import type { EventMessage } from "./message.js"
import type { EventHandlerRegistration } from "./handler.js"
import type { SequencedEvent } from "./event-source.js"
import {
  type SequencedDeadLetterQueue,
  type EnqueuePolicy,
  alwaysEnqueuePolicy,
  createDeadLetter,
} from "./dead-letter-queue.js"

/**
 * Options for dead-lettering event handler wrapper.
 */
export interface DeadLetteringOptions {
  /** The dead letter queue to use. */
  queue: SequencedDeadLetterQueue
  /** Policy deciding whether to dead-letter a failed event. Default: always. */
  policy?: EnqueuePolicy
  /**
   * Extract a sequence identifier from an event. Events in the same
   * sequence are ordered — if one fails, subsequent ones are blocked.
   * Default: uses the first tag value or event name.
   */
  sequenceIdentifier?: (event: EventMessage) => string
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
export function createDeadLetteringDelivery(options: DeadLetteringOptions) {
  const {
    queue,
    policy = alwaysEnqueuePolicy(),
    sequenceIdentifier = defaultSequenceIdentifier,
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
      const seqId = sequenceIdentifier(event)

      // If this sequence already has dead letters, block this event too
      const blocked = await queue.enqueueIfPresent(
        seqId,
        () => createDeadLetter(
          event,
          new Error("Blocked: previous event in sequence failed"),
          seqId,
          { blocked: true, position: Number(sequencedEvent.sequence) },
        ),
      )
      if (blocked) return

      // Try to deliver to all handlers
      for (const reg of handlers) {
        try {
          await reg.handler(event.payload, event.metadata)
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          const letter = createDeadLetter(event, error, seqId, {
            position: Number(sequencedEvent.sequence),
            handlerName: qualifiedNameToString(reg.descriptor.name),
          })

          const decision = policy.decide(letter, error)
          if (decision.shouldEnqueue) {
            await queue.enqueue({
              ...letter,
              cause: decision.cause ?? letter.cause,
              diagnostics: decision.diagnostics
                ? { ...letter.diagnostics, ...decision.diagnostics }
                : letter.diagnostics,
            })
          }
          // Error is consumed by DLQ — don't propagate
          return
        }
      }
    },
  }
}

function defaultSequenceIdentifier(event: EventMessage): string {
  // Use first tag value if available, otherwise event name
  if (event.tags && event.tags.length > 0) {
    return event.tags[0]!.value
  }
  return qualifiedNameToString(event.name)
}
