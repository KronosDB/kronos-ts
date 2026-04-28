import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import {
  processingStateStorage,
  computeIfAbsent,
  NoActiveUnitOfWork,
} from "@kronos-ts/messaging/processing-state"
import type { LoadFunction, EventCriteria } from "@kronos-ts/messaging"
import { ENTITY_CACHE_KEY, ENTITY_MODULES_KEY, SOURCING_INFOS_KEY } from "./append.js"

// ---------------------------------------------------------------------------
// State manager interface — minimal shape needed by load
// ---------------------------------------------------------------------------

type StateManagerLike = {
  load: (
    entity: any,
    id: any,
  ) => Promise<{
    state: any
    sourcingInfo: { criteria: EventCriteria; markerPosition: bigint }
  }>
}

/**
 * Resource key for the state manager component.
 * Written by handling modules + processors at handler-invocation entry (D-44).
 */
export const STATE_MANAGER_KEY: ResourceKey<StateManagerLike> = resourceKey("stateManager")

/**
 * Plan 04-01 (HDL-02 / D-42): module-level load.
 *
 * Read-only — NOT phase-guarded per D-43. Throws NoActiveUnitOfWork outside
 * a UoW. Caches entity state within the UoW (duplicate load() calls for the
 * same entity-id pair return the cached promise without re-querying the store).
 */
export const load: LoadFunction = (async <S>(entity: { name: string }, id: unknown): Promise<S> => {
  const state = processingStateStorage.getStore()
  if (state === undefined) throw new NoActiveUnitOfWork()
  const stateManager = state.resources.get(STATE_MANAGER_KEY.symbol) as StateManagerLike | undefined
  if (!stateManager) throw new Error("No state manager configured")

  const cache = computeIfAbsent(ENTITY_CACHE_KEY, () => new Map())
  const cacheKey = `${entity.name}:${String(id)}`
  if (!cache.has(cacheKey)) {
    cache.set(cacheKey, stateManager.load(entity, id))
    const modules = computeIfAbsent(ENTITY_MODULES_KEY, () => new Map())
    modules.set(cacheKey, { entity, id })
  }
  const result = await cache.get(cacheKey)!
  const loadResult = result as {
    state: any
    sourcingInfo: { criteria: EventCriteria; markerPosition: bigint }
  }
  const infos = computeIfAbsent(SOURCING_INFOS_KEY, () => [])
  infos.push(loadResult.sourcingInfo)
  return loadResult.state as S
}) as LoadFunction
