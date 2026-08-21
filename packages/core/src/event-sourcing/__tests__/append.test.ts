import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, type Message } from "../../messaging/messages.js"
import { unitOfWork, NoActiveUnitOfWork, WrongUoWPhase, Phase, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlating } from "../../correlation/correlating.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { appendFunction } from "../append.js"
// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

describe("append", () => {
  it("throws NoActiveUnitOfWork once its unit of work has closed", async () => {
    let append!: ReturnType<typeof appendFunction>
    await unitOfWork().execute(async (uow) => { append = appendFunction({ uow }) })
    expect(() => append(CourseCreated, { courseId: "c1", name: "Intro" })).toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase outside the INVOCATION phase", () => {
    const append = appendFunction({ uow: unitOfWork() })
    expect(() => append(CourseCreated, { courseId: "c1", name: "Intro" })).toThrow(WrongUoWPhase)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      uow.onPrepareCommit(async () => {
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

  it("buffers an EventMessage onto uow.events.buffered during INVOCATION", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered as any[]
      expect(buffered).toHaveLength(1)
      expect(buffered[0].name).toEqual(CourseCreated.name)
      expect(buffered[0].payload).toEqual({ courseId: "c1", name: "Intro" })
      // Tags derived from descriptor
      expect(buffered[0].tags).toEqual([{ key: "courseId", value: "c1" }])
    })
  })

  it("carries nothing from the handled command on its own", async () => {
    // The verb has no idea what a handler is handling, and no opinion about
    // what should jump from it. A birth's metadata is exactly what the caller
    // passed — nothing more.
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered as any[]
      expect(buffered[0].metadata).toEqual({})
    })
  })

  it("carries the handled command's correlation once the handler is wrapped", async () => {
    // Same verb, one wrapper: `correlatingHandler(next, correlationFrom)` puts
    // the pair on the task and overlays it through append's metadata parameter.
    const command = {
      kind: "command",
      identifier: "cmd-1",
      name: qn("university", "CreateCourse"),
      payload: {},
      metadata: { correlationId: "corr-1" },
      timestamp: 0,
    } as any

    const uow = correlating(unitOfWork())
    await uow.execute(async () => {
      const ctx = { append: appendFunction({ uow }), unitOfWork: uow }
      const handler = correlatingHandler((_m: any, c: typeof ctx) => {
        c.append(CourseCreated, { courseId: "c1", name: "Intro" })
      }, correlationFrom)
      handler(command, ctx)

      const buffered = uow.events.buffered as any[]
      expect(buffered[0].metadata).toEqual({ correlationId: "corr-1", causationId: "cmd-1" })
    })
  })

  it("uses empty metadata when nothing was provided", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered as any[]
      expect(buffered[0].metadata).toEqual({})
    })
  })

  it("uses explicitly provided metadata when provided", async () => {
    const uowMeta = { correlationId: "corr-1" } as any
    const eventMeta = { correlationId: "corr-override" } as any
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" }, eventMeta)
      const buffered = uow.events.buffered as any[]
      expect(buffered[0].metadata).toEqual(eventMeta)
    })
  })

  it("applies a matching evolver to the cached state when the module has one", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      // Set up a mock state with an evolver for CourseCreated
      const evolverFn = mock((s: any, { payload: e }: any) => ({ ...s, name: e.name }))
      const mockModule = {
        name: "Course",
        evolvers: [
          [{ name: CourseCreated.name }, evolverFn],
        ],
      }

      const initialState = { name: "", courseId: "c1" }
      const cacheKey = "Course:c1"
      uow.stateCache.entries.set(cacheKey, Promise.resolve({ state: initialState, sourcingInfo: {} }))
      uow.stateCache.modules.set(cacheKey, { module: mockModule, id: "c1" })

      append(CourseCreated, { courseId: "c1", name: "Intro to TS" })

      // The cache should now hold an updated promise
      const updated = await uow.stateCache.entries.get(cacheKey)!
      expect((updated as any).state).toEqual({ name: "Intro to TS", courseId: "c1" })
      expect(evolverFn).toHaveBeenCalledTimes(1)
    })
  })

  it("does not update cache when nothing was loaded", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      // Nothing loaded, so the state cache is empty — should not throw
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered as any[]
      expect(buffered).toHaveLength(1)
    })
  })

  it("does not apply evolver when event type does not match", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      const evolverFn = mock((_s: any, _e: any, _id: any) => ({}))
      const mockModule = {
        name: "Course",
        evolvers: [
          [{ name: CourseCapacityChanged.name }, evolverFn],
        ],
      }

      const cacheKey = "Course:c1"
      uow.stateCache.entries.set(cacheKey, Promise.resolve({ state: { name: "" }, sourcingInfo: {} }))
      uow.stateCache.modules.set(cacheKey, { module: mockModule, id: "c1" })

      // Append CourseCreated — evolver is for CourseCapacityChanged, should not fire
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      expect(evolverFn).not.toHaveBeenCalled()
    })
  })
})

describe("append — batch form", () => {
  it("buffers a list identically to N single calls", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append([
        [CourseCreated, { courseId: "c1", name: "Intro" }],
        [CourseCapacityChanged, { courseId: "c1", capacity: 30 }],
      ])
      const buffered = uow.events.buffered as any[]
      expect(buffered).toHaveLength(2)
      expect(buffered[0].payload).toEqual({ courseId: "c1", name: "Intro" })
      // tags still derive from the descriptor, i.e. the batch path is not a shortcut
      expect(buffered[0].tags).toEqual([{ key: "courseId", value: "c1" }])
      expect(buffered[1].payload).toEqual({ courseId: "c1", capacity: 30 })
    })
  })

  it("carries per-event metadata", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append([[CourseCreated, { courseId: "c2", name: "Algo" }, { tenant: "acme" } as never]])
      const buffered = uow.events.buffered as any[]
      expect(buffered[0].metadata).toMatchObject({ tenant: "acme" })
    })
  })

  it("an empty list is a no-op", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append([])
      expect(uow.events.buffered).toHaveLength(0)
    })
  })

  it("still type-checks each pair", () => {
    // Compile-time assertions only — never invoked. The @ts-expect-error
    // directives fail the BUILD if the mismatches below stop being errors.
    const _typeOnly = (uow: UnitOfWork) => {
      const append = appendFunction({ uow })
      // @ts-expect-error - payload must match the descriptor beside it in the tuple
      append([[CourseCreated, { courseId: "c3", capacity: 10 }]])
      // @ts-expect-error - a mismatch in the SECOND element is caught too
      append([[CourseCreated, { courseId: "c3", name: "ok" }], [CourseCapacityChanged, { nope: 1 }]])
    }
    expect(typeof _typeOnly).toBe("function")
  })
})
