import {
  resourceKey,
  qualifiedNameToString,
  generateIdentifier,
  mergeMetadata,
  type Metadata,
  type ResourceKey,
} from "@kronos-ts/common"
import {
  getResource,
  computeIfAbsent,
  requireInvocationPhase,
} from "@kronos-ts/messaging/processing-state"
import { CORRELATION_DATA_KEY } from "@kronos-ts/messaging/correlation-data"
import type { z } from "zod"
import type { EventDescriptor, EventMessage, EventCriteria } from "@kronos-ts/messaging"

/**
 * A descriptor paired with a payload it validates. Build these with
 * {@link evt} so each pair is checked independently — an array of loose tuples
 * would widen the descriptor/payload relationship and stop catching mismatches.
 */
export interface PendingEvent {
  readonly descriptor: EventDescriptor<any>
  readonly payload: unknown
  readonly metadata?: Metadata
}

/** Pair an event descriptor with its payload, for the batch form of `append`. */
export function evt<P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  payload: z.infer<P>,
  metadata?: Metadata,
): PendingEvent {
  return metadata === undefined ? { descriptor, payload } : { descriptor, payload, metadata }
}

/**
 * Append events to the active unit of work, buffered until commit.
 *
 * Single:  `append(TicketOpened, { ticketId })`
 * Batch:   `append([evt(TicketOpened, { ticketId }), evt(MessageSent, { messageId })])`
 *
 * Both forms are equivalent — every append in a UnitOfWork already flushes as
 * one atomic write at PREPARE_COMMIT, so the batch form is ergonomics, not a
 * different transaction boundary.
 */
export interface AppendFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>): void
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, metadata: Metadata): void
  (events: ReadonlyArray<PendingEvent>): void
}

// ---------------------------------------------------------------------------
// Resource keys (owned by append — open-question #1 resolved: keys live with
// the helper that writes them)
// ---------------------------------------------------------------------------

/** Buffered events waiting to be flushed at PREPARE_COMMIT. */
export const BUFFERED_EVENTS_KEY: ResourceKey<EventMessage[]> = resourceKey("bufferedEvents")

/** Sourcing info from load() calls, used to build append condition. */
export const SOURCING_INFOS_KEY: ResourceKey<Array<{ criteria: EventCriteria; markerPosition: bigint }>> =
  resourceKey("sourcingInfos")

/** State cache: prevents duplicate load() calls within same UnitOfWork. */
export const STATE_CACHE_KEY: ResourceKey<Map<string, Promise<unknown>>> = resourceKey("stateCache")

/** State module references keyed by cache key, used to apply evolvers on append. */
export const STATE_MODULES_KEY: ResourceKey<Map<string, { module: any; id: unknown }>> =
  resourceKey("stateModules")

/**
 * Plan 04-01 (HDL-02 / D-42): module-level append.
 *
 * Internal — exported only via the "./append" subpath for the HandlerContext.
 * Handlers reach this as `ctx.append`.
 *
 * Throws NoActiveUnitOfWork outside a UoW (D-43 fail-fast on no-UoW).
 * Throws WrongUoWPhase outside INVOCATION phase (D-43 mutator guard).
 *
 * Buffers events in BUFFERED_EVENTS_KEY; updates cached state via
 * matching evolvers (same logic as command-handling-module.ts appendFn).
 */
export const append: AppendFunction = ((
  eventDescriptorOrList: EventDescriptor<any> | ReadonlyArray<PendingEvent>,
  eventPayload?: unknown,
  eventMetadata?: Metadata,
) => {
  // Batch form: fan out to the single form so buffering, tag derivation,
  // correlation stamping and cached-state evolution stay in ONE place.
  if (Array.isArray(eventDescriptorOrList)) {
    for (const pending of eventDescriptorOrList) {
      ;(append as (d: EventDescriptor<any>, p: unknown, m?: Metadata) => void)(
        pending.descriptor,
        pending.payload,
        pending.metadata,
      )
    }
    return
  }
  const eventDescriptor = eventDescriptorOrList as EventDescriptor<any>
  const state = requireInvocationPhase() // D-43 mutator guard
  const events = computeIfAbsent(BUFFERED_EVENTS_KEY, () => [])
  const tags = eventDescriptor.tags ? eventDescriptor.tags(eventPayload) : []

  // Apply the active UnitOfWork's correlation data to the appended event so it
  // carries the correct correlationId/causationId of the message currently
  // being handled. Merges over the base metadata (explicit arg, else UoW
  // metadata). No-op when no correlation data is set — keeps events untouched
  // for apps that don't configure correlation providers.
  const baseMetadata = eventMetadata ?? state.metadata // fallback to UoW metadata when caller omits
  const correlationData = getResource(CORRELATION_DATA_KEY)
  const metadata =
    correlationData && Object.keys(correlationData).length > 0
      ? mergeMetadata(baseMetadata, correlationData)
      : baseMetadata

  const eventMessage: EventMessage = {
    kind: "event",
    identifier: generateIdentifier(),
    name: eventDescriptor.name,
    version: eventDescriptor.version,
    payload: eventPayload,
    metadata,
    timestamp: Date.now(),
    tags,
  }
  events.push(eventMessage)

  // Update cached state by applying matching evolvers.
  // Verbatim copy of command-handling-module.ts:101-123 logic — moved here so
  // both command-handling-module (via delegation) and direct module-level callers
  // get identical behaviour.
  const cache = getResource(STATE_CACHE_KEY)
  const modules = getResource(STATE_MODULES_KEY)
  if (cache && modules) {
    const eventType = qualifiedNameToString(eventDescriptor.name)
    for (const [cacheKey, { module, id }] of modules) {
      const cachedPromise = cache.get(cacheKey)
      if (!cachedPromise) continue
      const evolvers = (module as any).evolvers as ReadonlyArray<{
        descriptor: { name: any }
        evolve: (...args: any[]) => any
      }> | undefined
      if (!evolvers) continue
      for (const evolver of evolvers) {
        if (qualifiedNameToString(evolver.descriptor.name) === eventType) {
          cache.set(
            cacheKey,
            cachedPromise.then((result: any) => ({
              ...result,
              state: evolver.evolve(result.state, eventMessage),
            })),
          )
          break
        }
      }
    }
  }
}) as AppendFunction
