import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event } from "../../messaging/messages.js"
import { unitOfWork, NoActiveUnitOfWork, WrongUoWPhase, Phase, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { correlatingHandler } from "../../correlation/correlating-handler.js"
import { appendFunction } from "../append.js"

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
    expect(err.message).toContain('only allowed during "invocation"')
  })

  it("buffers an EventMessage onto uow.events.buffered during INVOCATION", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered
      expect(buffered).toHaveLength(1)
      expect(buffered[0]!.event.name).toEqual(CourseCreated.name)
      expect(buffered[0]!.event.payload).toEqual({ courseId: "c1", name: "Intro" })
      // Tags derived from descriptor
      expect(buffered[0]!.tags).toEqual([{ key: "courseId", value: "c1" }])
    })
  })

  it("carries nothing from the handled command on its own", async () => {
    // The verb has no idea what a handler is handling, and no opinion about
    // what should jump from it. A birth's metadata is exactly what the caller
    // passed — nothing more.
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered
      expect(buffered[0]!.event.metadata).toEqual({})
    })
  })

  it("carries the handled command's correlation once the handler is wrapped", async () => {
    // Same verb, one wrapper: `correlatingHandler(next)` holds the pair in the
    // invocation's closure and overlays it through append's metadata parameter.
    const command = {
      kind: "command",
      identifier: "cmd-1",
      name: qn("university", "CreateCourse"),
      payload: {},
      metadata: { correlationId: "corr-1" },
      timestamp: 0,
    } as any

    const uow = unitOfWork()
    await uow.execute(async () => {
      const ctx = { append: appendFunction({ uow }), unitOfWork: uow }
      const handler = correlatingHandler((_m: any, c: typeof ctx) => {
        c.append(CourseCreated, { courseId: "c1", name: "Intro" })
      })
      handler(command, ctx)

      const buffered = uow.events.buffered
      expect(buffered[0]!.event.metadata).toEqual({ correlationId: "corr-1", causationId: "cmd-1" })
    })
  })

  it("uses empty metadata when nothing was provided", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered
      expect(buffered[0]!.event.metadata).toEqual({})
    })
  })

  it("uses explicitly provided metadata when provided", async () => {
    const uowMeta = { correlationId: "corr-1" } as any
    const eventMeta = { correlationId: "corr-override" } as any
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" }, eventMeta)
      const buffered = uow.events.buffered
      expect(buffered[0]!.event.metadata).toEqual(eventMeta)
    })
  })

  it("buffers the event when nothing was loaded", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append(CourseCreated, { courseId: "c1", name: "Intro" })
      const buffered = uow.events.buffered
      expect(buffered).toHaveLength(1)
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
      const buffered = uow.events.buffered
      expect(buffered).toHaveLength(2)
      expect(buffered[0]!.event.payload).toEqual({ courseId: "c1", name: "Intro" })
      // tags still derive from the descriptor, i.e. the batch path is not a shortcut
      expect(buffered[0]!.tags).toEqual([{ key: "courseId", value: "c1" }])
      expect(buffered[1]!.event.payload).toEqual({ courseId: "c1", capacity: 30 })
    })
  })

  it("carries per-event metadata", async () => {
    await unitOfWork().execute(async (uow) => {
      const append = appendFunction({ uow })
      append([[CourseCreated, { courseId: "c2", name: "Algo" }, { tenant: "acme" } as never]])
      const buffered = uow.events.buffered
      expect(buffered[0]!.event.metadata).toMatchObject({ tenant: "acme" })
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
