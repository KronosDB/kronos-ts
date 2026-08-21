import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "../../messaging/messages.js"
import { query } from "../../index.js"
import { queryHandler, type QueryHandler } from "../handler.js"

/**
 * The handlers under test never touch the context; a stub keeps the arity
 * honest without pulling a whole UnitOfWork into a unit test.
 */
const TEST_CTX = {} as never


// Test fixtures
const GetCourseView = query({
  name: qn("university", "GetCourseView"),
  payload: z.object({ courseId: z.string() }),
})

describe("queryHandler() — singular factory (Phase 11-01)", () => {
  it("returns a definition with kind 'query-handler', descriptor, and handler", () => {
    const def = queryHandler(GetCourseView, async ({ payload: q }) => {
      return { courseId: q.courseId, name: "Intro" }
    })

    expect(def.kind).toBe("query-handler")
    expect(def.descriptor).toBe(GetCourseView)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the user handler with the query payload and returns its result", async () => {
    const def = queryHandler(GetCourseView, async ({ payload: q }) => {
      return { courseId: q.courseId, name: "Intro" }
    })

    const result = await def.handler({
      identifier: "qry-1",
      name: GetCourseView.name,
      payload: { courseId: "cs-101" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    }, TEST_CTX)

    expect(result).toEqual({ courseId: "cs-101", name: "Intro" })
  })

  it("supports synchronous handlers that return the result directly", () => {
    const def = queryHandler(GetCourseView, ({ payload: q }) => {
      return { courseId: q.courseId, name: "Sync" }
    })

    const result = def.handler({
      identifier: "qry-1",
      name: GetCourseView.name,
      payload: { courseId: "cs-101" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    }, TEST_CTX)

    expect(result).toEqual({ courseId: "cs-101", name: "Sync" })
  })

  it("infers the result type from the handler return", () => {
    // Type-only — the inferred R should be { courseId: string; name: string }
    const def: QueryHandler<
      typeof GetCourseView.payload,
      { courseId: string; name: string }
    > = queryHandler(GetCourseView, async ({ payload: q }) => ({
      courseId: q.courseId,
      name: "Inferred",
    }))
    expect(def.kind).toBe("query-handler")
  })

})
