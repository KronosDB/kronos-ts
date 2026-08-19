import { requireLive, type SourcingInfo, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Load event-sourced state for a module within the unit of work.
 *
 * The first signature matches a `StateModule`-shaped object structurally
 * (without importing the state model, which would invert the
 * dependency direction) so both the id and state types are inferred.
 */
export interface LoadFunction {
  <Id, S>(
    module: { kind: "state-module"; identity: string; create: (id: Id) => S },
    id: Id,
  ): Promise<S>
  <S>(module: { identity: string }, id: unknown): Promise<S>
}

// ---------------------------------------------------------------------------
// State manager interface — minimal shape needed by load
// ---------------------------------------------------------------------------

export type StateManagerLike = {
  load: (
    module: any,
    id: any,
  ) => Promise<{
    state: any
    sourcingInfo: SourcingInfo
  }>
}

/**
 * Stable cache key for a state id. State ids are typically objects
 * (e.g. `{ ticketId }`), and `String({...})` collapses every object to
 * `"[object Object]"` — which would make two different ids of the same module
 * share a cache entry within a UoW and return each other's state. Serialize
 * structurally instead, with sorted keys (so id construction order is
 * irrelevant) and bigint support. Primitive ids serialize to a unique string
 * too, so this is strictly safer than `String(id)`.
 */
function stableIdKey(id: unknown): string {
  return JSON.stringify(id, (_key, value) => {
    if (typeof value === "bigint") return `${value}n`
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {}
      for (const k of Object.keys(value).sort()) sorted[k] = (value as Record<string, unknown>)[k]
      return sorted
    }
    return value
  })
}

/**
 * Build the `load` capability for ONE invocation, closed over that
 * invocation's unit of work and the state manager of the item being invoked.
 *
 * Internal — exported only via the "./load" subpath for the HandlerContext.
 * Handlers reach the result as `ctx.load`.
 *
 * Read-only — NOT phase-guarded, but throws NoActiveUnitOfWork once the unit
 * of work has closed. Caches state on the unit of work (duplicate `load()`
 * calls for the same module-id pair return the cached promise without
 * re-querying the store) and records a sourcing info per call, which the
 * PREPARE_COMMIT flush turns into the DCB append condition.
 */
export function loadFunction(deps: {
  uow: UnitOfWork
  stateManager?: StateManagerLike
}): LoadFunction {
  return (async <S,>(module: { identity: string }, id: unknown): Promise<S> => {
    const uow = requireLive(deps.uow)
    const stateManager = deps.stateManager
    if (!stateManager) throw new Error("No state manager configured")

    // The module half of the key is the definition's `identity` — the handle
    // `state()` assigns per definition — not its `name`, which is optional and
    // means durable snapshot identity, not "which definition is this".
    const { entries, modules } = uow.stateCache
    const cacheKey = `${module.identity}:${stableIdKey(id)}`
    if (!entries.has(cacheKey)) {
      entries.set(cacheKey, stateManager.load(module, id))
      modules.set(cacheKey, { module, id })
    }
    const result = (await entries.get(cacheKey)!) as {
      state: any
      sourcingInfo: SourcingInfo
    }
    uow.events.sourcingInfos.push(result.sourcingInfo)
    return result.state as S
  }) as LoadFunction
}
