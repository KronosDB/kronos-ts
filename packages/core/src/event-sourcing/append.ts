import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  emptyMetadata,
  type Metadata,
  type EventDescriptor,
  type EventMessage, tagsOf } from "../messaging/messages.js"
import { generateIdentifier } from "../messaging/identifier.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
/**
 * A list of `[descriptor, payload]` (optionally `[descriptor, payload, metadata]`)
 * pairs. The descriptors are inferred as a tuple and each payload is mapped from
 * ITS OWN element, so a payload that does not match the descriptor beside it is
 * a compile error — no pairing helper needed.
 */
export type EventList<T extends readonly EventDescriptor<any>[]> = {
  [K in keyof T]:
    | readonly [T[K], InferOutput<T[K] extends EventDescriptor<infer P> ? P : never>]
    | readonly [T[K], InferOutput<T[K] extends EventDescriptor<infer P> ? P : never>, Metadata]
}

/**
 * Append events to the unit of work, buffered until commit.
 *
 * Single:  `ctx.append(TicketOpened, { ticketId })`
 * Batch:   `ctx.append([[TicketOpened, { ticketId }], [MessageSent, { messageId }]])`
 *
 * Both forms are equivalent — every append in a UnitOfWork already flushes as
 * one atomic write at PREPARE_COMMIT, so the batch form is ergonomics, not a
 * different transaction boundary.
 */
export type AppendFunction = {
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>): void
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>, metadata: Metadata): void
  <T extends readonly EventDescriptor<any>[]>(events: EventList<T>): void
}

/**
 * Build the `append` capability for ONE invocation, closed over that
 * invocation's unit of work.
 *
 * Internal — exported only via the "./append" subpath for the CommandHandlerContext.
 * Handlers reach the result as `ctx.append`.
 *
 * Throws NoActiveUnitOfWork once the unit of work has closed; throws
 * WrongUoWPhase outside the INVOCATION phase.
 *
 * Buffers events onto `uow.events.buffered`, which a later `ctx.load` folds
 * into the states they belong to.
 *
 * An appended event's metadata is EXACTLY what the caller passed. Nothing is
 * carried over from the command being handled — carrying is a host policy, and
 * `correlatingHandler(next, from)` implements it by wrapping this verb and
 * overlaying the task's correlation map through the same `metadata` parameter.
 */
export function appendFunction(deps: { uow: UnitOfWork }): AppendFunction {
  const append = ((
    eventDescriptorOrList:
      | EventDescriptor<any>
      | ReadonlyArray<readonly [EventDescriptor<any>, unknown, Metadata?]>,
    eventPayload?: unknown,
    eventMetadata?: Metadata,
  ) => {
    // Batch form: fan out to the single form so buffering and tag derivation
    // stay in one place.
    if (Array.isArray(eventDescriptorOrList)) {
      for (const [descriptor, payload, metadata] of eventDescriptorOrList) {
        append(descriptor, payload, metadata)
      }
      return
    }
    const eventDescriptor = eventDescriptorOrList as EventDescriptor<any>
    const uow = requireInvocation(deps.uow)
    const metadata = eventMetadata ?? emptyMetadata()
    const event: EventMessage = {
      kind: "event",
      identifier: generateIdentifier(),
      name: eventDescriptor.name,
      version: eventDescriptor.version,
      payload: eventPayload,
      metadata,
      timestamp: uow.now(),
    }
    uow.events.buffered.push({ event, tags: tagsOf(eventDescriptor, eventPayload, metadata) })
  }) as ((
    d: EventDescriptor<any> | ReadonlyArray<readonly [EventDescriptor<any>, unknown, Metadata?]>,
    p?: unknown,
    m?: Metadata,
  ) => void)

  return append as AppendFunction
}
