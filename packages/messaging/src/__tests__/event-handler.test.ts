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
    const def = eventHandler(CourseCreated, async ({ payload }) => {
      // e is typed as { courseId: string, name: string }
      payload.courseId
      payload.name
    })

    expect(def.kind).toBe("event-handler")
    expect(def.descriptor).toBe(CourseCreated)
    expect(typeof def.handler).toBe("function")
  })

  it("invokes the user handler with the event message", async () => {
    const seen: Array<{ courseId: string; name: string }> = []
    const def = eventHandler(CourseCreated, async ({ payload }) => {
      seen.push(payload)
    })

    await def.handler({
      identifier: "evt-1",
      name: CourseCreated.name,
      version: CourseCreated.version,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [],
    })

    expect(seen).toEqual([{ courseId: "cs-101", name: "Intro" }])
  })

  it("supports destructuring event message details", async () => {
    const metadata = emptyMetadata()
    const message = {
      identifier: "evt-1",
      name: CourseCreated.name,
      version: CourseCreated.version,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata,
      timestamp: 1715472000000,
      tags: [{ key: "courseId", value: "cs-101" }],
    }

    let seenTimestamp: number | undefined
    let seenVersion: string | undefined
    let seenTag: string | undefined

    const def = eventHandler(
      CourseCreated,
      async ({ timestamp, version, tags }) => {
        seenTimestamp = timestamp
        seenVersion = version
        seenTag = tags[0]?.value
      },
    )

    await def.handler(message)

    expect(seenTimestamp).toBe(1715472000000)
    expect(seenVersion).toBe(CourseCreated.version)
    expect(seenTag).toBe("cs-101")
  })

  it("exposes metadata on the message", async () => {
    const metadata = emptyMetadata()
    let seenMetadata: unknown

    const def = eventHandler(CourseCreated, async ({ metadata }) => {
      seenMetadata = metadata
    })

    await def.handler({
      identifier: "evt-1",
      name: CourseCreated.name,
      version: CourseCreated.version,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata,
      timestamp: Date.now(),
      tags: [],
    })

    expect(seenMetadata).toBe(metadata)
  })

  it("supports synchronous handlers", () => {
    let called = false
    const def = eventHandler(CourseCreated, () => {
      called = true
    })

    const result = def.handler({
      identifier: "evt-1",
      name: CourseCreated.name,
      version: CourseCreated.version,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [],
    })

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
