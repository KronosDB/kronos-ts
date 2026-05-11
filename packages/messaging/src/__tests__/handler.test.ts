import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { event, query, command } from "../descriptor.js"
import { on, onEvent } from "../handler.js"
import { commandHandler } from "../command-handler.js"
import { eventHandler, eventHandlers } from "../event-handler.js"
import { queryHandler, queryHandlers } from "../query-handler.js"

// -- Test fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
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
    const reg = on(CourseCreated, async (event) => {
      // event is typed as { courseId: string, name: string }
      event.courseId
      event.name
    })

    expect(reg.kind).toBe("event-handler")
    expect(reg.descriptor).toBe(CourseCreated)
  })

  it("creates a query handler registration", () => {
    const reg = on(GetCourse, async (query) => {
      return { courseId: query.courseId, name: "Test Course" }
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
      (state, event) => ({ ...state, name: event.name }),
    )

    expect(reg.kind).toBe("evolver")
    expect(reg.descriptor).toBe(CourseCreated)

    const evolved = reg.evolve(
      { name: "", capacity: 0 },
      { courseId: "cs-101", name: "Intro" },
      "cs-101",
    )
    expect(evolved.name).toBe("Intro")
  })
})

describe("commandHandler()", () => {
  it("creates a command handler definition with simple form (no result)", () => {
    const handler = commandHandler(CreateCourse, async (cmd, _metadata) => {
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

    const handler = commandHandler(CreateCourseWithResult, async (cmd, _metadata) => {
      return { courseId: cmd.courseId }
    })

    expect(handler.kind).toBe("command-handler")
    expect(handler.descriptor.result).toBeDefined()
  })

  it("creates a command handler with appendCondition override", () => {
    const handler = commandHandler(CreateCourse, {
      appendCondition: (cmd, sourced) => sourced,
      handler: async (cmd, _metadata) => {},
    })

    expect(handler.appendCondition).toBeDefined()
  })
})

describe("eventHandlers()", () => {
  it("creates an event handlers definition", () => {
    const def = eventHandlers({
      name: "course-projection",
      handlers: [
        on(CourseCreated, async (event) => {}),
        on(CourseCapacityChanged, async (event) => {}),
      ],
    })

    expect(def.kind).toBe("event-handlers")
    expect(def.name).toBe("course-projection")
    expect(def.handlers).toHaveLength(2)
    expect(def.sequencedBy).toBeUndefined()
  })

  it("supports optional sequencedBy and onReset", () => {
    const def = eventHandlers({
      name: "course-projection",
      handlers: [on(CourseCreated, async () => {})],
      sequencedBy: (event: any) => event.courseId,
      onReset: async () => {},
    })

    expect(def.sequencedBy).toBeDefined()
    expect(def.onReset).toBeDefined()
  })
})

describe("queryHandlers()", () => {
  it("creates a query handlers definition", () => {
    const def = queryHandlers({
      name: "course-queries",
      handlers: [
        on(GetCourse, async (query) => {
          return { courseId: query.courseId, name: "Test" }
        }),
      ],
    })

    expect(def.kind).toBe("query-handlers")
    expect(def.name).toBe("course-queries")
    expect(def.handlers).toHaveLength(1)
  })
})

// ─── Plan 11-02 — singular handler factories ────────────────────────────────
// These cases pin the singular eventHandler / queryHandler shape. The old
// grouped describe blocks above are deleted by Plan 11-04 together with the
// factories themselves.

describe("eventHandler()", () => {
  it("creates a singular event handler definition with the expected shape", () => {
    const def = eventHandler(CourseCreated, async (event) => {
      event.courseId
      event.name
    })

    expect(def.kind).toBe("event-handler")
    expect(def.descriptor).toBe(CourseCreated)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the handler with payload + metadata", async () => {
    const seen: Array<{ courseId: string; name: string }> = []
    const def = eventHandler(CourseCreated, async (event) => {
      seen.push(event)
    })

    await def.handler({ courseId: "cs-101", name: "Intro" }, {})
    expect(seen).toEqual([{ courseId: "cs-101", name: "Intro" }])
  })
})

describe("queryHandler()", () => {
  it("creates a singular query handler definition with the expected shape", () => {
    const def = queryHandler(GetCourse, async (query) => {
      return { courseId: query.courseId, name: "Test" }
    })

    expect(def.kind).toBe("query-handler")
    expect(def.descriptor).toBe(GetCourse)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the handler and returns its result", async () => {
    const def = queryHandler(GetCourse, async (query) => {
      return { courseId: query.courseId, name: "Echo" }
    })

    const result = await def.handler({ courseId: "cs-999" }, {})
    expect(result).toEqual({ courseId: "cs-999", name: "Echo" })
  })
})
