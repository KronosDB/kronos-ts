import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import {
  processingStateStorage,
  computeIfAbsent,
  NoActiveUnitOfWork,
} from "@kronos-ts/messaging/processing-state"
import type { EventCriteria } from "@kronos-ts/messaging"
import { STATE_CACHE_KEY, STATE_MODULES_KEY, SOURCING_INFOS_KEY } from "./append.js"

/**
 * Load event-sourced state for a module within the active unit of work.
 *
 * The first signature matches a `StateModule`-shaped object structurally
 * (without importing `@kronos-ts/modelling`, which would invert the
 * dependency direction) so both the id and state types are inferred.
 */
export interface LoadFunction {
  <Id, S>(module: { kind: "state-module"; name: string; create: (id: Id) => S }, id: Id): Promise<S>
  <S>(module: { name: string }, id: unknown): Promise<S>
}

// ---------------------------------------------------------------------------
// State manager interface — minimal shape needed by load
// ---------------------------------------------------------------------------

type StateManagerLike = {
  load: (
    module: any,
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
 * a UoW. Caches state within the UoW (duplicate load() calls for the
 * same module-id pair return the cached promise without re-querying the store).
 */
export const load: LoadFunction = (async <S>(module: { name: string }, id: unknown): Promise<S> => {
  const state = processingStateStorage.getStore()
  if (state === undefined) throw new NoActiveUnitOfWork()
  const stateManager = state.resources.get(STATE_MANAGER_KEY.symbol) as StateManagerLike | undefined
  if (!stateManager) throw new Error("No state manager configured")

  const cache = computeIfAbsent(STATE_CACHE_KEY, () => new Map())
  const cacheKey = `${module.name}:${String(id)}`
  if (!cache.has(cacheKey)) {
    cache.set(cacheKey, stateManager.load(module, id))
    const modules = computeIfAbsent(STATE_MODULES_KEY, () => new Map())
    modules.set(cacheKey, { module, id })
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
