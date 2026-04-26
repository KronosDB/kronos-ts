import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, generateIdentifier } from "@kronos-ts/common"
import { createSimpleQueryBus } from "../simple-query-bus.js"
import { createProcessingContext } from "../default-processing-context.js"
import { Phase } from "../processing-context.js"
import type { QueryMessage } from "../message.js"
import { createUpdateHandler } from "../subscription-query.js"
import {
  processingStateStorage,
  createInitialProcessingState,
} from "../processing-state.js"

/**
 * Phase 2 Plan 01: ctx resource methods now require an active
 * processingStateStorage.run scope. Tests that drive ctx.executePhases
 * outside the UnitOfWork must wrap in this helper.
 */
function inUoW<R>(fn: () => R | Promise<R>): Promise<R> {
  return processingStateStorage.run(
    createInitialProcessingState(emptyMetadata()),
    async () => fn(),
  )
}

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
    const handler = createUpdateHandler(queryMsg("TestQuery"))

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
    const handler = createUpdateHandler(queryMsg("TestQuery"))

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
    const handler = createUpdateHandler(queryMsg("TestQuery"), 2)

    expect(handler.offer("a")).toBe(true)
    expect(handler.offer("b")).toBe(true)
    expect(handler.offer("c")).toBe(false) // buffer full
  })

  it("completes the iterable on complete()", async () => {
    const handler = createUpdateHandler(queryMsg("TestQuery"))

    handler.complete()

    const results: unknown[] = []
    for await (const update of handler.iterable) {
      results.push(update)
    }

    expect(results).toEqual([])
  })

  it("propagates error on completeExceptionally()", async () => {
    const handler = createUpdateHandler(queryMsg("TestQuery"))

    handler.completeExceptionally(new Error("boom"))

    const results: unknown[] = []
    await expect(async () => {
      for await (const update of handler.iterable) {
        results.push(update)
      }
    }).toThrow("boom")
  })

  it("is not active after completion", () => {
    const handler = createUpdateHandler(queryMsg("TestQuery"))
    expect(handler.active).toBe(true)

    handler.complete()
    expect(handler.active).toBe(false)
  })
})

describe("SimpleQueryBus subscription queries", () => {
  it("returns initial result from regular query handler", async () => {
    const bus = createSimpleQueryBus()

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
    const bus = createSimpleQueryBus()

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
    const bus = createSimpleQueryBus()

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
    const bus = createSimpleQueryBus()
    const log: string[] = []

    bus.subscribe("test.GetCourse", async () => ({ id: "cs-101" }))

    const result = bus.subscriptionQuery(queryMsg("GetCourse", { courseId: "cs-101" }))
    await result.initialResult

    const updates: unknown[] = []
    const consumer = (async () => { for await (const u of result.updates) updates.push(u) })()

    // Create a ProcessingContext and emit within it.
    // Phase 2 Plan 01: ctx.computeIfAbsent (used by runAfterCommitOrImmediately)
    // is now an ALS shim — must run inside processingStateStorage.run.
    await inUoW(async () => {
      const ctx = createProcessingContext(emptyMetadata())

      // Emit during "invocation" — should be deferred
      ctx.on(Phase.INVOCATION, () => {
        bus.emitUpdate("test.GetCourse", () => true, { deferred: true })
        log.push("emitted")
      })

      ctx.on(Phase.COMMIT, () => {
        log.push("committed")
      })

      // At this point, update should NOT have been delivered yet
      ctx.markStarted()
      await ctx.executePhases()
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
    const bus = createSimpleQueryBus()

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
    const bus = createSimpleQueryBus()
    bus.subscribe("test.GetCourse", async () => ({}))

    const msg = queryMsg("GetCourse")
    bus.subscriptionQuery(msg)

    expect(() => bus.subscriptionQuery(msg)).toThrow("already registered")
  })
})
