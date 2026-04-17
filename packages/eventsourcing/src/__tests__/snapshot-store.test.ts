import { describe, expect, it } from "bun:test"
import { createInMemorySnapshotStore } from "../snapshot-store.js"

describe("InMemorySnapshotStore", () => {
  it("stores and loads a snapshot", async () => {
    // given
    const store = createInMemorySnapshotStore()
    const snapshot = { position: 42n, payload: { name: "test" }, timestamp: Date.now(), metadata: {} }

    // when
    await store.store("Course", "c-1", snapshot)
    const loaded = await store.load("Course", "c-1")

    // then
    expect(loaded).toEqual(snapshot)
  })

  it("returns undefined when no snapshot exists", async () => {
    // given
    const store = createInMemorySnapshotStore()

    // when
    const loaded = await store.load("Course", "nonexistent")

    // then
    expect(loaded).toBeUndefined()
  })

  it("replaces existing snapshot", async () => {
    // given
    const store = createInMemorySnapshotStore()
    await store.store("Course", "c-1", { position: 10n, payload: { v: 1 }, timestamp: 1000, metadata: {} })

    // when
    const updated = { position: 50n, payload: { v: 2 }, timestamp: 2000, metadata: {} }
    await store.store("Course", "c-1", updated)
    const loaded = await store.load("Course", "c-1")

    // then
    expect(loaded).toEqual(updated)
  })

  it("deletes snapshots", async () => {
    // given
    const store = createInMemorySnapshotStore()
    await store.store("Course", "c-1", { position: 10n, payload: {}, timestamp: 1000, metadata: {} })

    // when
    await store.deleteSnapshots("Course", "c-1")
    const loaded = await store.load("Course", "c-1")

    // then
    expect(loaded).toBeUndefined()
  })

  it("isolates snapshots by entity name and id", async () => {
    // given
    const store = createInMemorySnapshotStore()
    const s1 = { position: 10n, payload: { type: "course" }, timestamp: 1000, metadata: {} }
    const s2 = { position: 20n, payload: { type: "student" }, timestamp: 2000, metadata: {} }

    // when
    await store.store("Course", "c-1", s1)
    await store.store("Student", "s-1", s2)

    // then
    expect(await store.load("Course", "c-1")).toEqual(s1)
    expect(await store.load("Student", "s-1")).toEqual(s2)
    expect(await store.load("Course", "s-1")).toBeUndefined()
  })
})
