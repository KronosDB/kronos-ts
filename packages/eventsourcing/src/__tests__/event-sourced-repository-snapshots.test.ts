import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import { event, EventCriteria, type EventMessage } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { createInMemoryEventStore } from "../in-memory-event-store.js"
import { createEventSourcedRepository } from "../event-sourced-repository.js"
import { createInMemorySnapshotStore } from "../snapshot-store.js"
import { afterEvents, noSnapshotPolicy } from "../snapshot-policy.js"

// -- Fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = { created: boolean; name: string; capacity: number }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [
    on(CourseCreated, (state, { payload: e }) => ({
      ...state, created: true, name: e.name, capacity: e.capacity,
    })),
    on(CourseCapacityChanged, (state, { payload: e }) => ({
      ...state, capacity: e.capacity,
    })),
  ],
})

function eventMsg(descriptor: any, payload: any): EventMessage {
  const tags = descriptor.tags ? descriptor.tags(payload) : []
  return {
    identifier: generateIdentifier(),
    name: qn(descriptor.name.namespace, descriptor.name.name),
    version: descriptor.version ?? "1.0",
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags,
  }
}

// -- Tests --

describe("EventSourcedRepository with Snapshots", () => {
  describe("snapshot loading", () => {
    it("loads state from snapshot and replays only subsequent events", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()

      // Append 3 events
      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      ])
      await eventStore.append([
        eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      ])
      await eventStore.append([
        eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
      ])

      // Store a snapshot at position 1 (after 2 events)
      await snapshotStore.store("Course", "cs-101", {
        position: 1n,
        payload: { created: true, name: "CS 101", capacity: 50 },
        timestamp: Date.now(),
        metadata: {},
      })

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore)

      // when
      const result = await repo.load({ courseId: "cs-101" })

      // then — state should reflect snapshot + 1 remaining event
      expect(result.state).toEqual({
        created: true,
        name: "CS 101",
        capacity: 60, // from 3rd event, replayed after snapshot
      })
    })

    it("works without snapshot store", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      ])

      const repo = createEventSourcedRepository(Course, eventStore)

      // when
      const result = await repo.load({ courseId: "cs-101" })

      // then
      expect(result.state.name).toBe("CS 101")
      expect(result.state.capacity).toBe(30)
    })

    it("falls back to full replay when no snapshot exists", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()
      // No snapshot stored

      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
        eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      ])

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore)

      // when
      const result = await repo.load({ courseId: "cs-101" })

      // then — all events replayed from beginning
      expect(result.state).toEqual({
        created: true,
        name: "CS 101",
        capacity: 50,
      })
    })
  })

  describe("snapshot creation", () => {
    it("creates snapshot when policy triggers", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()
      const policy = afterEvents(2)

      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
        eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
        eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
      ])

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore, policy)

      // when
      await repo.load({ courseId: "cs-101" })
      // Wait for async snapshot storage
      await new Promise((r) => setTimeout(r, 10))

      // then
      const snapshot = await snapshotStore.load("Course", { courseId: "cs-101" })
      expect(snapshot).toBeDefined()
      expect(snapshot!.payload).toEqual({
        created: true,
        name: "CS 101",
        capacity: 60,
      })
    })

    it("does not create snapshot when policy does not trigger", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()
      const policy = afterEvents(100) // threshold too high

      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      ])

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore, policy)

      // when
      await repo.load({ courseId: "cs-101" })
      await new Promise((r) => setTimeout(r, 10))

      // then
      const snapshot = await snapshotStore.load("Course", { courseId: "cs-101" })
      expect(snapshot).toBeUndefined()
    })

    it("does not create snapshot with noSnapshotPolicy", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()

      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      ])

      const repo = createEventSourcedRepository(
        Course, eventStore, snapshotStore, noSnapshotPolicy(),
      )

      // when
      await repo.load({ courseId: "cs-101" })
      await new Promise((r) => setTimeout(r, 10))

      // then
      const snapshot = await snapshotStore.load("Course", { courseId: "cs-101" })
      expect(snapshot).toBeUndefined()
    })

    it("does not create snapshot when no new events after snapshot", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()
      const policy = afterEvents(1)

      await eventStore.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      ])

      // Pre-existing snapshot that covers all events
      await snapshotStore.store("Course", "cs-101", {
        position: 0n,
        payload: { created: true, name: "CS 101", capacity: 30 },
        timestamp: Date.now(),
        metadata: {},
      })

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore, policy)

      // when — load with no new events since snapshot
      const result = await repo.load({ courseId: "cs-101" })
      await new Promise((r) => setTimeout(r, 10))

      // then — state is correct and no new snapshot created (eventsApplied = 0)
      expect(result.state.capacity).toBe(30)
    })
  })

  describe("snapshot + subsequent events", () => {
    it("correctly combines snapshot state with new events", async () => {
      // given
      const eventStore = createInMemoryEventStore()
      const snapshotStore = createInMemorySnapshotStore()
      const policy = afterEvents(2) // snapshot after more than 2 events

      // Append 5 events
      for (let i = 0; i < 5; i++) {
        await eventStore.append([
          eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: (i + 1) * 10 }),
        ])
      }

      // Snapshot at position 1 (after first 2 events)
      await snapshotStore.store("Course", "cs-101", {
        position: 1n,
        payload: { created: false, name: "", capacity: 20 },
        timestamp: Date.now(),
        metadata: {},
      })

      const repo = createEventSourcedRepository(Course, eventStore, snapshotStore, policy)

      // when
      const result = await repo.load({ courseId: "cs-101" })
      await new Promise((r) => setTimeout(r, 10))

      // then — 3 events applied after snapshot: capacity 30, 40, 50
      expect(result.state.capacity).toBe(50)

      // Snapshot should be created (3 events applied > threshold of 2)
      const newSnapshot = await snapshotStore.load("Course", { courseId: "cs-101" })
      expect(newSnapshot).toBeDefined()
      expect(newSnapshot!.payload).toEqual({ created: false, name: "", capacity: 50 })
    })
  })
})
