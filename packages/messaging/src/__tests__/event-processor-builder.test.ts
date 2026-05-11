/**
 * Plan 11-02 Task 1 — processor builder migration to the flat singular-handler
 * shape. Asserts:
 *   - `.eventHandlers(...handlers)` varargs builder method exists on both
 *     tracking and subscribing builders.
 *   - `.onReset(fn)` exists on the tracking builder only (subscribing
 *     processors don't support reset).
 *   - The built module carries a flat `eventHandlers: ReadonlyArray<EventHandlerDefinition>`
 *     field (not `handlerGroups`).
 *   - `TrackingProcessorModule.initialSegmentCount` defaults to 16 when the
 *     builder did not call `.initialSegmentCount(n)`.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { event } from "../descriptor.js"
import { eventHandler } from "../event-handler.js"
import {
  trackingProcessor,
  subscribingProcessor,
} from "../event-processor-builder.js"

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string() }),
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

describe("trackingProcessor builder — flat singular-handler shape (Plan 11-02)", () => {
  it("exposes .eventHandlers(...) varargs and produces a flat eventHandlers array", () => {
    const onCreated = eventHandler(CourseCreated, async () => {})
    const onCapChanged = eventHandler(CourseCapacityChanged, async () => {})

    const mod = trackingProcessor("course-projection")
      .eventHandlers(onCreated, onCapChanged)
      .build()

    expect(mod.kind).toBe("tracking")
    expect(mod.name).toBe("course-projection")
    expect(mod.eventHandlers).toHaveLength(2)
    expect(mod.eventHandlers[0]).toBe(onCreated)
    expect(mod.eventHandlers[1]).toBe(onCapChanged)
  })

  it("defaults initialSegmentCount to 16 when not configured (Axon Framework parity)", () => {
    const mod = trackingProcessor("default-count").build()
    expect(mod.initialSegmentCount).toBe(16)
  })

  it("honors an explicit .initialSegmentCount(n) override", () => {
    const mod = trackingProcessor("custom-count").initialSegmentCount(4).build()
    expect(mod.initialSegmentCount).toBe(4)
  })

  it("exposes .onReset(fn) and threads it onto the module as a flat field", async () => {
    let resetCalled = false
    const mod = trackingProcessor("resettable")
      .onReset(async () => {
        resetCalled = true
      })
      .build()
    expect(typeof mod.onReset).toBe("function")
    await mod.onReset!()
    expect(resetCalled).toBe(true)
  })

})

describe("subscribingProcessor builder — flat singular-handler shape (Plan 11-02)", () => {
  it("exposes .eventHandlers(...) varargs and produces a flat eventHandlers array", () => {
    const onCreated = eventHandler(CourseCreated, async () => {})

    const mod = subscribingProcessor("notifications").eventHandlers(onCreated).build()

    expect(mod.kind).toBe("subscribing")
    expect(mod.name).toBe("notifications")
    expect(mod.eventHandlers).toHaveLength(1)
    expect(mod.eventHandlers[0]).toBe(onCreated)
  })

  it("does NOT expose onReset (subscribing processors don't support reset)", () => {
    const builder = subscribingProcessor("no-reset")
    expect(
      (builder as unknown as { onReset?: unknown }).onReset,
    ).toBeUndefined()
  })
})
