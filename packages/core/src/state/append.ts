import { qualifiedNameToString } from "../primitives/qualified-name.js"
import { emptyMetadata, mergeMetadata, type Metadata } from "../primitives/metadata.js"
import { generateIdentifier } from "../primitives/identifier.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { z } from "zod"
import type { EventDescriptor } from "../messages/descriptor.js"
import type { EventMessage, Message } from "../messages/message.js"

/**
 * A list of `[descriptor, payload]` (optionally `[descriptor, payload, metadata]`)
 * pairs. The descriptors are inferred as a tuple and each payload is mapped from
 * ITS OWN element, so a payload that does not match the descriptor beside it is
 * a compile error — no pairing helper needed.
 */
export type EventList<T extends readonly EventDescriptor<any>[]> = {
  [K in keyof T]:
    | readonly [T[K], z.infer<T[K] extends EventDescriptor<infer P> ? P : never>]
    | readonly [T[K], z.infer<T[K] extends EventDescriptor<infer P> ? P : never>, Metadata]
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
export interface AppendFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>): void
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, metadata: Metadata): void
  <T extends readonly EventDescriptor<any>[]>(events: EventList<T>): void
}

/**
 * Build the `append` capability for ONE invocation, closed over that
 * invocation's unit of work.
 *
 * Internal — exported only via the "./append" subpath for the HandlerContext.
 * Handlers reach the result as `ctx.append`.
 *
 * Throws NoActiveUnitOfWork once the unit of work has closed; throws
 * WrongUoWPhase outside the INVOCATION phase.
 *
 * Buffers events onto `uow.events.buffered`; updates cached state via matching
 * evolvers so a handler that appends then loads sees its own writes.
 */
export function appendFunction(deps: { uow: UnitOfWork; message?: Message }): AppendFunction {
  const append = ((
    eventDescriptorOrList:
      | EventDescriptor<any>
      | ReadonlyArray<readonly [EventDescriptor<any>, unknown, Metadata?]>,
    eventPayload?: unknown,
    eventMetadata?: Metadata,
  ) => {
    // Batch form: fan out to the single form so buffering, tag derivation,
    // correlation stamping and cached-state evolution stay in ONE place.
    if (Array.isArray(eventDescriptorOrList)) {
      for (const [descriptor, payload, metadata] of eventDescriptorOrList) {
        append(descriptor, payload, metadata)
      }
      return
    }
    const eventDescriptor = eventDescriptorOrList as EventDescriptor<any>
    const uow = requireInvocation(deps.uow)
    const tags = eventDescriptor.tags ? eventDescriptor.tags(eventPayload) : []

    // Apply the unit of work's correlation data to the appended event so it
    // carries the correct correlationId/causationId of the message currently
    // being handled. Merges over the base metadata — the explicit argument
    // when given, otherwise the metadata of the message THIS invocation is
    // handling (closed over by the binding; the unit of work is task-scoped and
    // holds no message). No-op when no correlation data is set — keeps events
    // untouched for apps that don't configure correlation providers.
    const baseMetadata = eventMetadata ?? deps.message?.metadata ?? emptyMetadata()
    const correlationData = uow.correlationData()
    const metadata =
      Object.keys(correlationData).length > 0
        ? mergeMetadata(baseMetadata, correlationData)
        : baseMetadata

    const eventMessage: EventMessage = {
      kind: "event",
      identifier: generateIdentifier(),
      name: eventDescriptor.name,
      version: eventDescriptor.version,
      payload: eventPayload,
      metadata,
      timestamp: uow.now(),
      tags,
    }
    uow.events.buffered.push(eventMessage)

    // Update cached state by applying matching evolvers.
    const { entries, modules } = uow.stateCache
    const eventType = qualifiedNameToString(eventDescriptor.name)
    for (const [cacheKey, { module }] of modules) {
      const cachedPromise = entries.get(cacheKey)
      if (!cachedPromise) continue
      const evolvers = (module as any).evolvers as
        | ReadonlyArray<readonly [{ name: any }, (...args: any[]) => any]>
        | undefined
      if (!evolvers) continue
      for (const [descriptor, evolve] of evolvers) {
        if (qualifiedNameToString(descriptor.name) === eventType) {
          entries.set(
            cacheKey,
            cachedPromise.then((result: any) => ({
              ...result,
              state: evolve(result.state, eventMessage),
            })),
          )
          break
        }
      }
    }
  }) as ((
    d: EventDescriptor<any> | ReadonlyArray<readonly [EventDescriptor<any>, unknown, Metadata?]>,
    p?: unknown,
    m?: Metadata,
  ) => void)

  return append as AppendFunction
}
