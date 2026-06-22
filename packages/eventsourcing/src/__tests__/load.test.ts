import { describe, it, expect, mock } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import { runInNewUoW, NoActiveUnitOfWork, onPrepareCommit } from "@kronos-ts/messaging"
import { setResource, processingStateStorage } from "@kronos-ts/messaging/processing-state"
import { load, STATE_MANAGER_KEY } from "../load.js"
import { STATE_CACHE_KEY, STATE_MODULES_KEY, SOURCING_INFOS_KEY } from "../append.js"

// ---------------------------------------------------------------------------
// Minimal mock state manager
// ---------------------------------------------------------------------------

function makeStateManager(stateResult: unknown = { name: "Intro" }) {
  const loadFn = mock((_module: any, _id: any) =>
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

const mockModule = { name: "Course" } as any

describe("load", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", async () => {
    await expect(load(mockModule, "c1")).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws 'No state manager configured' when STATE_MANAGER_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(load(mockModule, "c1")).rejects.toThrow(/state manager/)
    })
  })

  it("delegates to state manager and returns state", async () => {
    const sm = makeStateManager({ name: "Intro to TS" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      const result = await load(mockModule, "c1")
      expect(result).toEqual({ name: "Intro to TS" })
      expect(sm._loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("caches the state promise — second load() call is a cache hit", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      const r1 = await load(mockModule, "c1")
      const r2 = await load(mockModule, "c1")
      expect(r1).toEqual({ name: "Intro" })
      expect(r2).toEqual({ name: "Intro" })
      expect(sm._loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("does NOT collide two different OBJECT ids of the same module within one UoW (gotcha #7)", async () => {
    // Regression: the cache key used String(id), so {ticketId:"A"} and
    // {ticketId:"B"} both stringified to "[object Object]" and shared one
    // entry — the second load returned the first ticket's state.
    const loadFn = mock((_module: any, id: any) =>
      Promise.resolve({
        state: { ticketId: id.ticketId },
        sourcingInfo: { criteria: { kind: "any" }, markerPosition: 0n },
      }),
    )
    const sm = { load: loadFn } as any

    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm)
      const a = await load<{ ticketId: string }>(mockModule, { ticketId: "A" })
      const b = await load<{ ticketId: string }>(mockModule, { ticketId: "B" })

      expect(a.ticketId).toBe("A")
      expect(b.ticketId).toBe("B") // would be "A" under the old String(id) key
      expect(loadFn).toHaveBeenCalledTimes(2) // distinct cache keys → two real loads
    })
  })

  it("key order in an object id does not change the cache key", async () => {
    const loadFn = mock((_module: any, _id: any) =>
      Promise.resolve({ state: {}, sourcingInfo: { criteria: { kind: "any" }, markerPosition: 0n } }),
    )
    const sm = { load: loadFn } as any
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm)
      await load(mockModule, { a: 1, b: 2 })
      await load(mockModule, { b: 2, a: 1 }) // same id, different construction order → cache hit
      expect(loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("populates STATE_CACHE_KEY, STATE_MODULES_KEY and SOURCING_INFOS_KEY on first load", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      await load(mockModule, "c1")

      const state = processingStateStorage.getStore()!
      const cache = state.resources.get(STATE_CACHE_KEY.symbol) as Map<string, Promise<unknown>>
      const modules = state.resources.get(STATE_MODULES_KEY.symbol) as Map<string, { module: any; id: unknown }>
      const infos = state.resources.get(SOURCING_INFOS_KEY.symbol) as Array<{ criteria: any; markerPosition: bigint }>

      expect(cache).toBeDefined()
      expect(cache.has('Course:"c1"')).toBe(true)
      expect(modules).toBeDefined()
      expect(modules.has('Course:"c1"')).toBe(true)
      expect(infos).toBeDefined()
      expect(infos).toHaveLength(1)
      expect(infos[0]!.markerPosition).toBe(0n)
    })
  })

  it("SOURCING_INFOS_KEY accumulates one entry per load() call (per unique module-id)", async () => {
    const sm = makeStateManager()
    await runInNewUoW(emptyMetadata(), async () => {
      setResource(STATE_MANAGER_KEY, sm as any)
      await load(mockModule, "c1")
      await load({ name: "Course" } as any, "c2")
      const state = processingStateStorage.getStore()!
      const infos = state.resources.get(SOURCING_INFOS_KEY.symbol) as any[]
      // Two distinct module-id pairs → two sourcing infos
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
          result = await load(mockModule, "c1")
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
