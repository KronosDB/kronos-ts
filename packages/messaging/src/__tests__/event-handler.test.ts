import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { event } from "../descriptor.js"
import {
  eventHandler,
  type EventHandlerDefinition,
} from "../event-handler.js"

// Test fixtures
const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

describe("eventHandler() — singular factory (Phase 11-01)", () => {
  it("returns a definition with kind 'event-handler', descriptor, and handler", () => {
    const def = eventHandler(CourseCreated, async (e, _metadata) => {
      // e is typed as { courseId: string, name: string }
      e.courseId
      e.name
    })

    expect(def.kind).toBe("event-handler")
    expect(def.descriptor).toBe(CourseCreated)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the user handler with the event payload and metadata", async () => {
    const seen: Array<{ courseId: string; name: string }> = []
    const def = eventHandler(CourseCreated, async (e, _metadata) => {
      seen.push(e)
    })

    await def.handler({ courseId: "cs-101", name: "Intro" }, emptyMetadata())

    expect(seen).toEqual([{ courseId: "cs-101", name: "Intro" }])
  })

  it("supports synchronous handlers", () => {
    let called = false
    const def = eventHandler(CourseCreated, (_e, _metadata) => {
      called = true
    })

    const result = def.handler(
      { courseId: "cs-101", name: "Intro" },
      emptyMetadata(),
    )

    // Sync handler — return type is void (undefined)
    expect(result).toBeUndefined()
    expect(called).toBe(true)
  })

  it("produces a value assignable to EventHandlerDefinition<typeof Type.payload>", () => {
    // Type-only test — compile-time assertion via explicit annotation
    const def: EventHandlerDefinition<typeof CourseCreated.payload> =
      eventHandler(CourseCreated, async () => {})
    expect(def.kind).toBe("event-handler")
  })

})
