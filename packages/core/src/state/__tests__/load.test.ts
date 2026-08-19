import { describe, it, expect, mock } from "bun:test"
import { emptyMetadata } from "../../primitives/metadata.js"
import { unitOfWork, NoActiveUnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { loadFunction } from "../load.js"

// ---------------------------------------------------------------------------
// Minimal mock state manager
// ---------------------------------------------------------------------------

function makeStateManager(stateResult: unknown = { name: "Intro" }) {
  const loadFn = mock((_module: any, _id: any) =>
    Promise.resolve({
      state: stateResult,
      sourcingInfo: { query: { tags: {} }, markerPosition: 0n },
    }),
  )
  return {
    load: loadFn,
    _loadFn: loadFn,
  }
}

const mockModule = { identity: "Course", name: "Course" } as any

describe("load", () => {
  it("throws NoActiveUnitOfWork once its unit of work has closed", async () => {
    let load!: ReturnType<typeof loadFunction>
    await unitOfWork().execute(async (uow) => {
      load = loadFunction({ uow, stateManager: makeStateManager() as never })
    })
    await expect(load(mockModule, "c1")).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws 'No state manager configured' when no state manager was bound", async () => {
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow })
      await expect(load(mockModule, "c1")).rejects.toThrow(/state manager/)
    })
  })

  it("delegates to state manager and returns state", async () => {
    const sm = makeStateManager({ name: "Intro to TS" })
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      const result = await load(mockModule, "c1")
      expect(result).toEqual({ name: "Intro to TS" })
      expect(sm._loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("caches the state promise — second load() call is a cache hit", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
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
        sourcingInfo: { query: { tags: {} }, markerPosition: 0n },
      }),
    )
    const sm = { load: loadFn } as any

    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      const a = await load<{ ticketId: string }>(mockModule, { ticketId: "A" })
      const b = await load<{ ticketId: string }>(mockModule, { ticketId: "B" })

      expect(a.ticketId).toBe("A")
      expect(b.ticketId).toBe("B") // would be "A" under the old String(id) key
      expect(loadFn).toHaveBeenCalledTimes(2) // distinct cache keys → two real loads
    })
  })

  it("key order in an object id does not change the cache key", async () => {
    const loadFn = mock((_module: any, _id: any) =>
      Promise.resolve({ state: {}, sourcingInfo: { query: { tags: {} }, markerPosition: 0n } }),
    )
    const sm = { load: loadFn } as any
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      await load(mockModule, { a: 1, b: 2 })
      await load(mockModule, { b: 2, a: 1 }) // same id, different construction order → cache hit
      expect(loadFn).toHaveBeenCalledTimes(1)
    })
  })

  it("populates the state cache, module map and sourcing infos on first load", async () => {
    const sm = makeStateManager({ name: "Intro" })
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      await load(mockModule, "c1")

      expect(uow.stateCache.entries.has('Course:"c1"')).toBe(true)
      expect(uow.stateCache.modules.has('Course:"c1"')).toBe(true)
      expect(uow.events.sourcingInfos).toHaveLength(1)
      expect(uow.events.sourcingInfos[0]!.markerPosition).toBe(0n)
    })
  })

  it("sourcing infos accumulate one entry per load() call (per unique module-id)", async () => {
    const sm = makeStateManager()
    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      await load(mockModule, "c1")
      await load({ identity: "Course" } as any, "c2")
      // Two distinct module-id pairs → two sourcing infos
      expect(uow.events.sourcingInfos).toHaveLength(2)
    })
  })

  it("does NOT have a phase guard — load works from inside onPrepareCommit", async () => {
    const sm = makeStateManager({ name: "Intro" })
    let result: unknown = null
    let caughtError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const load = loadFunction({ uow, stateManager: sm as never })
      uow.onPrepareCommit(async () => {
        try {
          result = await load(mockModule, "c1")
        } catch (e) {
          caughtError = e
        }
      })
    })

    // Must NOT throw WrongUoWPhase — load is read-only
    expect(caughtError).toBeNull()
    expect(result).toEqual({ name: "Intro" })
  })
})
