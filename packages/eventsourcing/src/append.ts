import {
  resourceKey,
  qualifiedNameToString,
  generateIdentifier,
  type Metadata,
  type ResourceKey,
} from "@kronos-ts/common"
import {
  getResource,
  computeIfAbsent,
  requireInvocationPhase,
} from "@kronos-ts/messaging/processing-state"
import type { z } from "zod"
import type { EventDescriptor, EventMessage, EventCriteria } from "@kronos-ts/messaging"

/** Append events to the active unit of work, buffered until commit. */
export interface AppendFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>): void
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, metadata: Metadata): void
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
 * Throws NoActiveUnitOfWork outside a UoW (D-43 fail-fast on no-UoW).
 * Throws WrongUoWPhase outside INVOCATION phase (D-43 mutator guard).
 *
 * Buffers events in BUFFERED_EVENTS_KEY; updates cached state via
 * matching evolvers (same logic as command-handling-module.ts appendFn).
 */
export const append: AppendFunction = ((
  eventDescriptor: EventDescriptor<any>,
  eventPayload: unknown,
  eventMetadata?: Metadata,
) => {
  const state = requireInvocationPhase() // D-43 mutator guard
  const events = computeIfAbsent(BUFFERED_EVENTS_KEY, () => [])
  const tags = eventDescriptor.tags ? eventDescriptor.tags(eventPayload) : []
  const eventMessage: EventMessage = {
    identifier: generateIdentifier(),
    name: eventDescriptor.name,
    version: eventDescriptor.version,
    payload: eventPayload,
    metadata: eventMetadata ?? state.metadata, // fallback to UoW metadata when caller omits
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
