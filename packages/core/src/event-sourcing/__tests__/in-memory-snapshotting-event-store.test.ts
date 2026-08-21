/**
 * The capability tier's null implementation, on its own terms.
 *
 * There is no store seam to test any more, so there is no `load` to call: a
 * cached fold goes IN through `storeSnapshot` and comes back OUT leading a
 * `source` whose condition carried the key. That asymmetry is the design, not
 * an awkwardness — reading is not a second call, it is the read you were
 * already making.
 */
import { describe, expect, it } from "bun:test"
import { inMemoryEventStore } from "../in-memory.js"
import { inMemorySnapshottingEventStore } from "../in-memory-snapshotting-event-store.js"
import { sourcingCondition } from "../sourcing-condition.js"
import { snapshotIdentifier } from "../snapshot.js"

/** What a capable store hands back for `key`, read the only way there is. */
async function cached(store: ReturnType<typeof wired>, key: string) {
  const result = await store.source(
    sourcingCondition({ tags: { anything: "at-all" } }, undefined, { key }),
  )
  return result.snapshot
}

function wired() {
  return inMemorySnapshottingEventStore(inMemoryEventStore())
}

describe("inMemorySnapshottingEventStore — the capability, keyed by ONE string", () => {
  it("stores a cached fold and leads the next matching read with it", async () => {
    const store = wired()
    const snapshot = { state: { name: "test" }, position: 42n }

    await store.storeSnapshot("course-v1:cs-101", snapshot)

    expect(await cached(store, "course-v1:cs-101")).toEqual(snapshot)
  })

  it("leads with nothing when no entry exists — a miss is a plain read", async () => {
    expect(await cached(wired(), "nothing-here")).toBeUndefined()
  })

  it("REPLACES on store — a cache has a current entry, not a history", async () => {
    const store = wired()
    await store.storeSnapshot("k", { state: { v: 1 }, position: 10n })

    const updated = { state: { v: 2 }, position: 50n }
    await store.storeSnapshot("k", updated)

    expect(await cached(store, "k")).toEqual(updated)
  })

  it("keys are OPAQUE — different strings are different caches, and that is all", async () => {
    const store = wired()
    const a = { state: { type: "course" }, position: 10n }
    const b = { state: { type: "student" }, position: 20n }

    await store.storeSnapshot("course-v1:1", a)
    await store.storeSnapshot("student-v1:1", b)

    expect(await cached(store, "course-v1:1")).toEqual(a)
    expect(await cached(store, "student-v1:1")).toEqual(b)
    // A RENAMED key is simply a different cache — the invalidation story.
    expect(await cached(store, "course-v2:1")).toBeUndefined()
  })

  it("a condition WITHOUT a key is passed down untouched", async () => {
    const store = wired()
    await store.storeSnapshot("k", { state: { v: 1 }, position: 10n })

    const result = await store.source(sourcingCondition({ tags: { anything: "at-all" } }))

    expect(result.snapshot).toBeUndefined()
  })
})

describe("snapshotIdentifier — the durable flattening the composed key uses", () => {
  it("leaves a string id as itself, unquoted", () => {
    expect(snapshotIdentifier("cs-101")).toBe("cs-101")
  })

  it("SORTS object keys, so id construction order is not part of an identity", () => {
    expect(snapshotIdentifier({ courseId: "c", studentId: "s" })).toBe(
      snapshotIdentifier({ studentId: "s", courseId: "c" }),
    )
  })

  it("keeps different ids of the same state distinct", () => {
    expect(snapshotIdentifier({ courseId: "c-1" })).not.toBe(
      snapshotIdentifier({ courseId: "c-2" }),
    )
  })

  it("does not collapse a bigint onto the string that prints the same", () => {
    expect(snapshotIdentifier({ n: 1n })).not.toBe(snapshotIdentifier({ n: "1" }))
  })
})
