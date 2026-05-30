import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { event, query, command } from "../descriptor.js"
import { on, onEvent } from "../handler.js"
import { commandHandler } from "../command-handler.js"
import { eventHandler } from "../event-handler.js"
import { queryHandler } from "../query-handler.js"

// -- Test fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const GetCourse = query({
  name: qn("university", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

describe("on()", () => {
  it("creates an event handler registration", () => {
    const reg = on(CourseCreated, async ({ payload }) => {
      // event is typed as { courseId: string, name: string }
      payload.courseId
      payload.name
    })

    expect(reg.kind).toBe("event-handler")
    expect(reg.descriptor).toBe(CourseCreated)
  })

  it("creates a query handler registration", () => {
    const reg = on(GetCourse, async ({ payload }) => {
      return { courseId: payload.courseId, name: "Test Course" }
    })

    expect(reg.kind).toBe("query-handler")
    expect(reg.descriptor).toBe(GetCourse)
  })
})

describe("onEvent()", () => {
  it("creates an evolver registration", () => {
    type CourseState = { name: string; capacity: number }

    const reg = onEvent<CourseState, typeof CourseCreated.payload>(
      CourseCreated,
      (state, { payload }) => ({ ...state, name: payload.name }),
    )

    expect(reg.kind).toBe("evolver")
    expect(reg.descriptor).toBe(CourseCreated)

    const evolved = reg.evolve(
      { name: "", capacity: 0 },
      {
        identifier: "evt-1",
        name: CourseCreated.name,
        version: CourseCreated.version,
        payload: { courseId: "cs-101", name: "Intro" },
        metadata: {},
        timestamp: Date.now(),
        tags: [],
      },
    )
    expect(evolved.name).toBe("Intro")
  })
})

describe("commandHandler()", () => {
  it("creates a command handler definition with simple form (no result)", () => {
    const handler = commandHandler(CreateCourse, async ({ payload: cmd }) => {
      // void handler
    })

    expect(handler.kind).toBe("command-handler")
    expect(handler.descriptor).toBe(CreateCourse)
    expect(handler.appendCondition).toBeUndefined()
  })

  it("creates a command handler definition with typed result from descriptor", () => {
    const CreateCourseWithResult = command({
      name: qn("university", "CreateCourse"),
      payload: z.object({ courseId: z.string(), name: z.string() }),
      result: z.object({ courseId: z.string() }),
    })

    const handler = commandHandler(CreateCourseWithResult, async ({ payload: cmd }) => {
      return { courseId: cmd.courseId }
    })

    expect(handler.kind).toBe("command-handler")
    expect(handler.descriptor.result).toBeDefined()
  })

  it("creates a command handler with appendCondition override", () => {
    const handler = commandHandler(CreateCourse, {
      appendCondition: (cmd, sourced) => sourced,
      handler: async () => {},
    })

    expect(handler.appendCondition).toBeDefined()
  })
})

// ─── Plan 11-02 — singular handler factories ────────────────────────────────
// Singular eventHandler / queryHandler factories. The old grouped
// `eventHandlers({...})` / `queryHandlers({...})` factories were removed
// by Plan 11-04.

describe("eventHandler()", () => {
  it("creates a singular event handler definition with the expected shape", () => {
    const def = eventHandler(CourseCreated, async ({ payload }) => {
      payload.courseId
      payload.name
    })

    expect(def.kind).toBe("event-handler")
    expect(def.descriptor).toBe(CourseCreated)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the handler with payload + metadata", async () => {
    const seen: Array<{ courseId: string; name: string }> = []
    const def = eventHandler(CourseCreated, async ({ payload }) => {
      seen.push(payload)
    })

    await def.handler({
      identifier: "evt-1",
      name: CourseCreated.name,
      version: CourseCreated.version,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata: {},
      timestamp: Date.now(),
      tags: [],
    })
    expect(seen).toEqual([{ courseId: "cs-101", name: "Intro" }])
  })
})

describe("queryHandler()", () => {
  it("creates a singular query handler definition with the expected shape", () => {
    const def = queryHandler(GetCourse, async ({ payload }) => {
      return { courseId: payload.courseId, name: "Test" }
    })

    expect(def.kind).toBe("query-handler")
    expect(def.descriptor).toBe(GetCourse)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the handler and returns its result", async () => {
    const def = queryHandler(GetCourse, async ({ payload }) => {
      return { courseId: payload.courseId, name: "Echo" }
    })

    const result = await def.handler({
      identifier: "qry-1",
      name: GetCourse.name,
      payload: { courseId: "cs-999" },
      metadata: {},
      timestamp: Date.now(),
    })
    expect(result).toEqual({ courseId: "cs-999", name: "Echo" })
  })
})
