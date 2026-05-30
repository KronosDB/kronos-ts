import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { event } from "@kronos-ts/messaging"
import { runInNewUoW, NoActiveUnitOfWork, WrongUoWPhase, onPrepareCommit, Phase } from "@kronos-ts/messaging"
import { processingStateStorage } from "@kronos-ts/messaging/processing-state"
import { append, BUFFERED_EVENTS_KEY, STATE_CACHE_KEY, STATE_MODULES_KEY } from "../append.js"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: (p) => [{ key: "courseId", value: p.courseId }],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

describe("append", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", () => {
    expect(() => append(CourseCreated, { courseId: "c1", name: "Intro" })).toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(async () => {
        try {
          append(CourseCreated, { courseId: "c1", name: "Intro" })
        } catch (e) {
          capturedError = e
        }
      })
    })

    expect(capturedError).toBeInstanceOf(WrongUoWPhase)
    const err = capturedError as WrongUoWPhase
    expect(err.currentPhase).toBe(Phase.PREPARE_COMMIT)
    expect(err.message).toContain("INVOCATION")
  })

  it("buffers an EventMessage into BUFFERED_EVENTS_KEY during INVOCATION", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as any[]
      expect(buffered).toHaveLength(1)
      expect(buffered[0].name).toEqual(CourseCreated.name)
      expect(buffered[0].payload).toEqual({ courseId: "c1", name: "Intro" })
      // Tags derived from descriptor
      expect(buffered[0].tags).toEqual([{ key: "courseId", value: "c1" }])
    })
  })

  it("uses UoW metadata as event metadata when not explicitly provided", async () => {
    const meta = { correlationId: "corr-1" } as any
    await runInNewUoW(meta, async () => {
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as any[]
      expect(buffered[0].metadata).toEqual(meta)
    })
  })

  it("uses explicitly provided metadata when provided", async () => {
    const uowMeta = { correlationId: "corr-1" } as any
    const eventMeta = { correlationId: "corr-override" } as any
    await runInNewUoW(uowMeta, async () => {
      append(CourseCreated, { courseId: "c1", name: "Intro" }, eventMeta)
      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as any[]
      expect(buffered[0].metadata).toEqual(eventMeta)
    })
  })

  it("applies matching evolver to STATE_CACHE_KEY when state module has matching evolver", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const state = processingStateStorage.getStore()!

      // Set up a mock state module with an evolver for CourseCreated
      const evolverFn = mock((s: any, { payload: e }: any) => ({ ...s, name: e.name }))
      const mockModule = {
        name: "Course",
        evolvers: [
          {
            descriptor: { name: CourseCreated.name },
            evolve: evolverFn,
          },
        ],
      }

      const initialState = { name: "", courseId: "c1" }
      const stateCache = new Map<string, Promise<unknown>>()
      const stateModules = new Map<string, { module: any; id: unknown }>()

      const cacheKey = "Course:c1"
      stateCache.set(cacheKey, Promise.resolve({ state: initialState, sourcingInfo: {} }))
      stateModules.set(cacheKey, { module: mockModule, id: "c1" })

      state.resources.set(STATE_CACHE_KEY.symbol, stateCache)
      state.resources.set(STATE_MODULES_KEY.symbol, stateModules)

      append(CourseCreated, { courseId: "c1", name: "Intro to TS" })

      // The cache should now hold an updated promise
      const updated = await stateCache.get(cacheKey)!
      expect((updated as any).state).toEqual({ name: "Intro to TS", courseId: "c1" })
      expect(evolverFn).toHaveBeenCalledTimes(1)
    })
  })

  it("does not update cache when no state modules are registered", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      // No STATE_CACHE_KEY / STATE_MODULES_KEY set — should not throw
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as any[]
      expect(buffered).toHaveLength(1)
    })
  })

  it("does not apply evolver when event type does not match", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const state = processingStateStorage.getStore()!

      const evolverFn = mock((_s: any, _e: any, _id: any) => ({}))
      const mockModule = {
        name: "Course",
        evolvers: [
          {
            descriptor: { name: CourseCapacityChanged.name },
            evolve: evolverFn,
          },
        ],
      }

      const stateCache = new Map<string, Promise<unknown>>()
      const stateModules = new Map<string, { module: any; id: unknown }>()
      const cacheKey = "Course:c1"
      stateCache.set(cacheKey, Promise.resolve({ state: { name: "" }, sourcingInfo: {} }))
      stateModules.set(cacheKey, { module: mockModule, id: "c1" })

      state.resources.set(STATE_CACHE_KEY.symbol, stateCache)
      state.resources.set(STATE_MODULES_KEY.symbol, stateModules)

      // Append CourseCreated — evolver is for CourseCapacityChanged, should not fire
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      expect(evolverFn).not.toHaveBeenCalled()
    })
  })
})
