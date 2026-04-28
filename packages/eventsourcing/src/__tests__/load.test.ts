import { describe, it, expect, mock } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import { runInNewUoW, NoActiveUnitOfWork, onPrepareCommit } from "@kronos-ts/messaging"
import { setResource, processingStateStorage } from "@kronos-ts/messaging/processing-state"
import { load, STATE_MANAGER_KEY } from "../load.js"
import { ENTITY_CACHE_KEY, ENTITY_MODULES_KEY, SOURCING_INFOS_KEY } from "../append.js"

// ---------------------------------------------------------------------------
// Minimal mock state manager
// ---------------------------------------------------------------------------

function makeStateManager(stateResult: unknown = { name: "Intro" }) {
  const loadFn = mock((_entity: any, _id: any) =>
    Promise.resolve({
      state: stateResult,
      sourcingInfo: { criteria: { kind: "any" }, markerPosition: 0n },
    }),
  )
  return {
    load: loadFn,
    _loadFn: loadFn,
  }
}

const mockEntity = { name: "Course" } as any

describe("load", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", async () => {
    await expect(load(mockEntity, "c1")).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws 'No state manager configured' when STATE_MANAGER_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(load(mockEntity, "c1")).rejects.toThrow(/state manager/)
    })
  })

  it("delegates to state manager and returns entity state", async () => {
    const sm = makeStateManager({ name: "Intro to TS" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      const result = await load(mockEntity, "c1")
      expect(result).toEqual({ name: "Intro to TS" })
      expect(sm._loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("caches the entity promise — second load() call is a cache hit", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      const r1 = await load(mockEntity, "c1")
      const r2 = await load(mockEntity, "c1")
      expect(r1).toEqual({ name: "Intro" })
      expect(r2).toEqual({ name: "Intro" })
      expect(sm._loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("populates ENTITY_CACHE_KEY, ENTITY_MODULES_KEY and SOURCING_INFOS_KEY on first load", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      await load(mockEntity, "c1")

      const state = processingStateStorage.getStore()!
      const cache = state.resources.get(ENTITY_CACHE_KEY.symbol) as Map<string, Promise<unknown>>
      const modules = state.resources.get(ENTITY_MODULES_KEY.symbol) as Map<string, { entity: any; id: unknown }>
      const infos = state.resources.get(SOURCING_INFOS_KEY.symbol) as Array<{ criteria: any; markerPosition: bigint }>

      expect(cache).toBeDefined()
      expect(cache.has("Course:c1")).toBe(true)
      expect(modules).toBeDefined()
      expect(modules.has("Course:c1")).toBe(true)
      expect(infos).toBeDefined()
      expect(infos).toHaveLength(1)
      expect(infos[0]!.markerPosition).toBe(0n)
    })
  })

  it("SOURCING_INFOS_KEY accumulates one entry per load() call (per unique entity-id)", async () => {
    const sm = makeStateManager()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      await load(mockEntity, "c1")
      await load({ name: "Course" } as any, "c2")
      const state = processingStateStorage.getStore()!
      const infos = state.resources.get(SOURCING_INFOS_KEY.symbol) as any[]
      // Two distinct entity-id pairs → two sourcing infos
      expect(infos).toHaveLength(2)
    })
  })

  it("does NOT have a phase guard — load works from inside onPrepareCommit", async () => {
    const sm = makeStateManager({ name: "Intro" })
    let result: unknown = null
    let caughtError: unknown = null

    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      onPrepareCommit(async () => {
        try {
          result = await load(mockEntity, "c1")
        } catch (e) {
          caughtError = e
        }
      })
    })

    // Must NOT throw WrongUoWPhase — load is read-only per D-43
    expect(caughtError).toBeNull()
    expect(result).toEqual({ name: "Intro" })
  })
})
