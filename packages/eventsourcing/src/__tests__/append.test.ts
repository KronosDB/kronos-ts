import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { event } from "@kronos-ts/messaging"
import { runInNewUoW, NoActiveUnitOfWork, WrongUoWPhase, onPrepareCommit, Phase } from "@kronos-ts/messaging"
import { processingStateStorage } from "@kronos-ts/messaging/processing-state"
import { append, BUFFERED_EVENTS_KEY, ENTITY_CACHE_KEY, ENTITY_MODULES_KEY } from "../append.js"

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

  it("applies matching evolver to ENTITY_CACHE_KEY when entity module has matching evolver", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const state = processingStateStorage.getStore()!

      // Set up a mock entity module with an evolver for CourseCreated
      const evolverFn = mock((s: any, e: any, _id: any) => ({ ...s, name: e.name }))
      const mockEntity = {
        name: "Course",
        evolvers: [
          {
            descriptor: { name: CourseCreated.name },
            evolve: evolverFn,
          },
        ],
      }

      const initialState = { name: "", courseId: "c1" }
      const entityCache = new Map<string, Promise<unknown>>()
      const entityModules = new Map<string, { entity: any; id: unknown }>()

      const cacheKey = "Course:c1"
      entityCache.set(cacheKey, Promise.resolve({ state: initialState, sourcingInfo: {} }))
      entityModules.set(cacheKey, { entity: mockEntity, id: "c1" })

      state.resources.set(ENTITY_CACHE_KEY.symbol, entityCache)
      state.resources.set(ENTITY_MODULES_KEY.symbol, entityModules)

      append(CourseCreated, { courseId: "c1", name: "Intro to TS" })

      // The cache should now hold an updated promise
      const updated = await entityCache.get(cacheKey)!
      expect((updated as any).state).toEqual({ name: "Intro to TS", courseId: "c1" })
      expect(evolverFn).toHaveBeenCalledTimes(1)
    })
  })

  it("does not update cache when no entity modules are registered", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      // No ENTITY_CACHE_KEY / ENTITY_MODULES_KEY set — should not throw
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
      const mockEntity = {
        name: "Course",
        evolvers: [
          {
            descriptor: { name: CourseCapacityChanged.name },
            evolve: evolverFn,
          },
        ],
      }

      const entityCache = new Map<string, Promise<unknown>>()
      const entityModules = new Map<string, { entity: any; id: unknown }>()
      const cacheKey = "Course:c1"
      entityCache.set(cacheKey, Promise.resolve({ state: { name: "" }, sourcingInfo: {} }))
      entityModules.set(cacheKey, { entity: mockEntity, id: "c1" })

      state.resources.set(ENTITY_CACHE_KEY.symbol, entityCache)
      state.resources.set(ENTITY_MODULES_KEY.symbol, entityModules)

      // Append CourseCreated — evolver is for CourseCapacityChanged, should not fire
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      expect(evolverFn).not.toHaveBeenCalled()
    })
  })
})
