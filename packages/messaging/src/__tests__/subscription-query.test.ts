import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, generateIdentifier } from "@kronos-ts/common"
import { simpleQueryBus } from "../simple-query-bus.js"
import { onCommit } from "../processing-state.js"
import { runInNewUoW } from "../unit-of-work.js"
import type { QueryMessage } from "../message.js"
import { updateHandler } from "../subscription-query.js"

function queryMsg(name: string, payload: unknown = {}): QueryMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

describe("UpdateHandler", () => {
  it("buffers updates for async iteration", async () => {
    const handler = updateHandler(queryMsg("TestQuery"))

    handler.offer("update-1")
    handler.offer("update-2")
    handler.complete()

    const results: unknown[] = []
    for await (const update of handler.iterable) {
      results.push(update)
    }

    expect(results).toEqual(["update-1", "update-2"])
  })

  it("waits for updates when buffer is empty", async () => {
    const handler = updateHandler(queryMsg("TestQuery"))

    const collected: unknown[] = []
    const consumer = (async () => {
      for await (const update of handler.iterable) {
        collected.push(update)
      }
    })()

    // Let consumer start waiting
    await new Promise((r) => setTimeout(r, 10))
    expect(collected).toEqual([])

    // Push updates
    handler.offer("update-1")
    await new Promise((r) => setTimeout(r, 10))
    expect(collected).toEqual(["update-1"])

    handler.offer("update-2")
    handler.complete()

    await consumer
    expect(collected).toEqual(["update-1", "update-2"])
  })

  it("rejects offer when buffer is full", () => {
    const handler = updateHandler(queryMsg("TestQuery"), 2)

    expect(handler.offer("a")).toBe(true)
    expect(handler.offer("b")).toBe(true)
    expect(handler.offer("c")).toBe(false) // buffer full
  })

  it("completes the iterable on complete()", async () => {
    const handler = updateHandler(queryMsg("TestQuery"))

    handler.complete()

    const results: unknown[] = []
    for await (const update of handler.iterable) {
      results.push(update)
    }

    expect(results).toEqual([])
  })

  it("propagates error on completeExceptionally()", async () => {
    const handler = updateHandler(queryMsg("TestQuery"))

    handler.completeExceptionally(new Error("boom"))

    const results: unknown[] = []
    await expect(async () => {
      for await (const update of handler.iterable) {
        results.push(update)
      }
    }).toThrow("boom")
  })

  it("is not active after completion", () => {
    const handler = updateHandler(queryMsg("TestQuery"))
    expect(handler.active).toBe(true)

    handler.complete()
    expect(handler.active).toBe(false)
  })
})

describe("SimpleQueryBus subscription queries", () => {
  it("returns initial result from regular query handler", async () => {
    const bus = simpleQueryBus()

    bus.subscribe("test.GetCourse", async (msg) => {
      return { id: msg.payload.courseId, name: "Intro" }
    })

    const result = bus.subscriptionQuery(
      queryMsg("GetCourse", { courseId: "cs-101" }),
    )

    const initial = await result.initialResult
    expect(initial).toEqual({ id: "cs-101", name: "Intro" })

    result.close()
  })

  it("receives updates via emitUpdate", async () => {
    const bus = simpleQueryBus()

    bus.subscribe("test.GetCourse", async (msg) => {
      return { id: msg.payload.courseId, name: "Intro" }
    })

    const result = bus.subscriptionQuery(
      queryMsg("GetCourse", { courseId: "cs-101" }),
    )

    await result.initialResult

    const updates: unknown[] = []
    const consumer = (async () => {
      for await (const update of result.updates) {
        updates.push(update)
      }
    })()

    // Emit updates
    bus.emitUpdate("test.GetCourse", () => true, { name: "Updated" })
    bus.emitUpdate("test.GetCourse", () => true, { name: "Updated Again" })

    // Wait for delivery
    await new Promise((r) => setTimeout(r, 50))

    result.close()
    await consumer

    expect(updates).toEqual([{ name: "Updated" }, { name: "Updated Again" }])
  })

  it("filters updates by query payload", async () => {
    const bus = simpleQueryBus()

    bus.subscribe("test.GetCourse", async (msg) => {
      return { id: msg.payload.courseId }
    })

    const result1 = bus.subscriptionQuery(
      queryMsg("GetCourse", { courseId: "cs-101" }),
    )
    const result2 = bus.subscriptionQuery(
      queryMsg("GetCourse", { courseId: "cs-201" }),
    )

    await result1.initialResult
    await result2.initialResult

    const updates1: unknown[] = []
    const updates2: unknown[] = []

    const c1 = (async () => { for await (const u of result1.updates) updates1.push(u) })()
    const c2 = (async () => { for await (const u of result2.updates) updates2.push(u) })()

    // Only update cs-101 subscriptions
    bus.emitUpdate(
      "test.GetCourse",
      (q: any) => q.courseId === "cs-101",
      { name: "Only for 101" },
    )

    await new Promise((r) => setTimeout(r, 50))

    result1.close()
    result2.close()
    await c1
    await c2

    expect(updates1).toEqual([{ name: "Only for 101" }])
    expect(updates2).toEqual([]) // filtered out
  })

  it("defers updates to AFTER_COMMIT when ProcessingContext is provided", async () => {
    const bus = simpleQueryBus()
    const log: string[] = []

    bus.subscribe("test.GetCourse", async () => ({ id: "cs-101" }))

    const result = bus.subscriptionQuery(queryMsg("GetCourse", { courseId: "cs-101" }))
    await result.initialResult

    const updates: unknown[] = []
    const consumer = (async () => { for await (const u of result.updates) updates.push(u) })()

    // Plan 03-04: drive lifecycle via the runner + module-level accessors.
    // `runAfterCommitOrImmediately` in subscription-query reads from the
    // active ALS state, so emission must happen inside an active UoW. Emit
    // directly in the action body (the runner's "invocation" phase) — late
    // registration for INVOCATION is dropped by `drivePhases`.
    await runInNewUoW(emptyMetadata(), async () => {
      bus.emitUpdate("test.GetCourse", () => true, { deferred: true })
      log.push("emitted")
      onCommit(() => {
        log.push("committed")
      })
    })

    // Wait for delivery
    await new Promise((r) => setTimeout(r, 50))

    result.close()
    await consumer

    // Update should arrive AFTER commit
    expect(log).toEqual(["emitted", "committed"])
    expect(updates).toEqual([{ deferred: true }])
  })

  it("completes subscription via completeSubscription", async () => {
    const bus = simpleQueryBus()

    bus.subscribe("test.GetCourse", async () => ({ id: "cs-101" }))

    const result = bus.subscriptionQuery(queryMsg("GetCourse"))
    await result.initialResult

    const updates: unknown[] = []
    const consumer = (async () => { for await (const u of result.updates) updates.push(u) })()

    bus.emitUpdate("test.GetCourse", () => true, "update-1")
    await new Promise((r) => setTimeout(r, 10))
    bus.completeSubscription("test.GetCourse")

    await consumer

    expect(updates).toEqual(["update-1"])
  })

  it("throws on duplicate subscription", async () => {
    const bus = simpleQueryBus()
    bus.subscribe("test.GetCourse", async () => ({}))

    const msg = queryMsg("GetCourse")
    bus.subscriptionQuery(msg)

    expect(() => bus.subscriptionQuery(msg)).toThrow("already registered")
  })
})
