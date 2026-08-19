import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { event, query, command } from "../../index.js"
import { commandHandler } from "../command-handler.js"
import { eventHandler } from "../event-handler.js"
import { queryHandler } from "../query-handler.js"

/**
 * The handlers under test never touch the context; a stub keeps the arity
 * honest without pulling a whole UnitOfWork into a unit test.
 */
const TEST_CTX = {} as never



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

// `on()` (the callback-pairing DSL for evolvers) is deleted — evolvers are now
// correlated-tuple DATA on `state({ evolve: [...] })`, covered by
// `packages/modelling/src/__tests__/state.test.ts`. `QueryHandlerRegistration`
// is a different, unrelated pattern (see handler.ts) and is left in place.

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
    }, TEST_CTX)
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
    }, TEST_CTX)
    expect(result).toEqual({ courseId: "cs-999", name: "Echo" })
  })
})
