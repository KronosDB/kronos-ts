/**
 * What the REPOSITORY owns, now that the read fusion is a mechanism.
 *
 * Two things, and they are the two halves of the split this suite exists to
 * pin: the repository ASKS for the strategy (a key on the condition, set only
 * when both halves are present) and it WRITES the cache entry (the fold's own
 * bookkeeping, which no read-path decorator could do for it). Whether a read is
 * actually served from a cache is somebody else's business — see
 * `snapshotting-event-store-wrappers.test.ts` for that end.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { state } from "../state.js"
import { inMemoryEventStore } from "../in-memory.js"
import { eventSourcedRepository } from "../repository.js"
import type { EventStore, SourcingResult } from "../event-store.js"
import type { SourcingCondition } from "../sourcing-condition.js"
import { markerAt } from "../consistency-marker.js"
import type { SnapshotCapableEventStore } from "../event-store.js"
import type { Snapshot } from "../snapshot.js"
import {
  snapshotIdentifier,
  afterEvents,
  noSnapshotPolicy,
  type SnapshotPolicy,
} from "../snapshot.js"

// -- Fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; name: string; capacity: number }

/**
 * The SAME fold under a chosen policy. The policy is part of the state value,
 * so "this state snapshots after N events" is said where the state is declared
 * rather than where the repository is built.
 */
const courseWith = (when?: SnapshotPolicy) =>
  state({
    id: { courseId: z.string() },
    tags: (id) => ({ courseId: id.courseId }),
    evolve: [
      () => ({ created: false, name: "", capacity: 0 }) as CourseState,
      [CourseCreated, (state, { payload: e }) => ({
        ...state, created: true, name: e.name, capacity: e.capacity,
      })],
      [CourseCapacityChanged, (state, { payload: e }) => ({
        ...state, capacity: e.capacity,
      })],
    ],
    ...(when ? { snapshot: { key: "course-v1", when } } : {}),
  })

/** A state that snapshots at all — the read half needs a policy too. */
const Course = courseWith(afterEvents(2))

const cs101 = { courseId: "cs-101" }
const cs101Key = snapshotIdentifier(cs101)
/** What entries are filed under: DERIVED from the fold, never written down. */
const KEY = `course-v1:${cs101Key}`

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

/**
 * A CAPABLE log that records the conditions it was asked for and the entries
 * written to it, and optionally answers with a leading snapshot — standing in
 * for whichever family wrapper is underneath.
 */
function recordingLog(leading?: SourcingResult["snapshot"]) {
  const inner = inMemoryEventStore()
  const conditions: SourcingCondition[] = []
  const written = new Map<string, Snapshot>()
  const store: SnapshotCapableEventStore = {
    ...inner,
    async storeSnapshot(key, snapshot) {
      written.set(key, snapshot)
    },
    async source(condition) {
      conditions.push(condition)
      // Serve the strategy only when it was ASKED for — a condition with no
      // key is a plain full-range read, which is exactly what a discard
      // replays with.
      const serves = leading !== undefined && condition.snapshot !== undefined
      const result = await inner.source(
        serves ? { ...condition, start: leading!.position + 1n } : condition,
      )
      return serves ? { ...result, snapshot: leading } : result
    },
  }
  return { store, conditions, written, append: inner.append.bind(inner) }
}

/** A capable log with no cleverness — what most of the write tests need. */
function capableLog() {
  const inner = inMemoryEventStore()
  const written = new Map<string, Snapshot>()
  const store: SnapshotCapableEventStore = {
    ...inner,
    async storeSnapshot(key, snapshot) {
      written.set(key, snapshot)
    },
  }
  return { store, written, append: inner.append.bind(inner) }
}

// -- Tests --

describe("the repository ASKS for the snapshot strategy", () => {
  it("sets the key on the condition when the state has a policy AND the log is CAPABLE", async () => {
    // given
    const log = recordingLog()
    const repo = eventSourcedRepository(Course, log.store)

    // when
    await repo.load(cs101)

    // then — the key the STATE declared, with the id appended so one declared
    // key serves every instance without them colliding
    expect(log.conditions[0]!.snapshot).toEqual({ key: KEY })
  })

  it("a snapshotting state over a BARE log THROWS — the one line of runtime demand", () => {
    // given — the pair the TYPES already refuse. A TypeScript caller cannot
    // reach this (see `snapshot-demand.types.ts`); a JavaScript one can, and a
    // silent full replay that also writes to nothing would be a performance
    // mystery rather than a mistake.
    const bare = inMemoryEventStore()

    // when / then — it names the state and the fix
    expect(() => eventSourcedRepository(Course, bare)).toThrow(
      /declares a snapshot policy.*<family>SnapshottingEventStore/s,
    )
  })

  it("sets NO key when the state has no policy — the state half is missing", async () => {
    // given
    const log = recordingLog()
    const repo = eventSourcedRepository(courseWith(), log.store)

    // when
    await repo.load(cs101)

    // then
    expect(log.conditions[0]!.snapshot).toBeUndefined()
  })
})

describe("the repository STARTS from a leading snapshot", () => {
  it("uses snapshot.state instead of initial(id), and folds what came after", async () => {
    // given — a log answering with a cached fold at position 1 plus one later event
    const log = recordingLog({ state: { created: true, name: "CS 101", capacity: 50 }, position: 1n })
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
    ])
    const repo = eventSourcedRepository(Course, log.store)

    // when
    const result = await repo.load(cs101)

    // then — the cached fold plus the one event after it
    expect(result.state).toEqual({ created: true, name: "CS 101", capacity: 60 })
  })

  it("falls back to initial(id) when nothing leads the result", async () => {
    // given
    const log = recordingLog()
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
    ])
    const repo = eventSourcedRepository(Course, log.store)

    // when
    const result = await repo.load(cs101)

    // then
    expect(result.state).toEqual({ created: true, name: "CS 101", capacity: 50 })
  })

  it("works over a plain, uncapable log when the state declares no policy", async () => {
    // given — the base contract, complete for event sourcing on its own
    const eventStore = inMemoryEventStore()
    await eventStore.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
    ])
    const repo = eventSourcedRepository(courseWith(), eventStore)

    // when
    const result = await repo.load(cs101)

    // then
    expect(result.state.name).toBe("CS 101")
    expect(result.state.capacity).toBe(30)
  })
})

  it("DISCARDS a leading snapshot that no longer fits, and replays instead", async () => {
    // given — a log that leads with a cached fold missing a key the current
    // initial state declares. The repository is the only party holding both the
    // value and the shape, so it is the only party that can notice.
    const log = recordingLog({ state: { created: true, name: "CS 101" }, position: 1n })
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
    ])
    const repo = eventSourcedRepository(Course, log.store)

    // when
    const result = await repo.load(cs101)

    // then — the full fold, not the unfit entry plus one event
    expect(result.state).toEqual({ created: true, name: "CS 101", capacity: 60 })
    // and the second read carried NO strategy — a plain full-range replay
    expect(log.conditions.length).toBe(2)
    expect(log.conditions[1]!.snapshot).toBeUndefined()
    expect(log.conditions[1]!.start).toBeUndefined()
  })

describe("the repository WRITES the cache entry — the fold's own bookkeeping", () => {
  it("writes when the policy triggers, under the declared key + flattened id", async () => {
    // given
    const log = capableLog()
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
    ])
    const repo = eventSourcedRepository(courseWith(afterEvents(2)), log.store)

    // when
    await repo.load(cs101)
    await new Promise((r) => setTimeout(r, 10))

    // then — the FOLD is what was cached, at the marker it was folded to
    const entry = log.written.get(KEY)
    expect(entry).toBeDefined()
    expect(entry!.state).toEqual({ created: true, name: "CS 101", capacity: 60 })
    expect(entry!.position).toBe(2n)
  })

  it("does not write when the policy does not trigger", async () => {
    // given
    const log = capableLog()
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
    ])
    const repo = eventSourcedRepository(courseWith(afterEvents(100)), log.store)

    // when
    await repo.load(cs101)
    await new Promise((r) => setTimeout(r, 10))

    // then
    expect(log.written.get(KEY)).toBeUndefined()
  })

  it("does not write under noSnapshotPolicy", async () => {
    // given
    const log = capableLog()
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
    ])
    const repo = eventSourcedRepository(courseWith(noSnapshotPolicy()), log.store)

    // when
    await repo.load(cs101)
    await new Promise((r) => setTimeout(r, 10))

    // then
    expect(log.written.get(KEY)).toBeUndefined()
  })

  it("does not write when the load folded nothing new — arithmetic, not policy", async () => {
    // given — a log that leads with a cache entry covering everything
    const log = recordingLog({ state: { created: true, name: "CS 101", capacity: 30 }, position: 0n })
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
    ])
    const repo = eventSourcedRepository(courseWith(afterEvents(0)), log.store)

    // when
    const result = await repo.load(cs101)
    await new Promise((r) => setTimeout(r, 10))

    // then — the state is right and nothing new was cached
    expect(result.state.capacity).toBe(30)
    expect(log.written.get(KEY)).toBeUndefined()
  })

  it("a failed cache write never fails the load", async () => {
    // given
    const inner = inMemoryEventStore()
    await inner.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 60 }),
    ])
    const eventStore: SnapshotCapableEventStore = {
      ...inner,
      async storeSnapshot() { throw new Error("cache is down") },
    }
    const repo = eventSourcedRepository(courseWith(afterEvents(2)), eventStore)

    // when / then
    const result = await repo.load(cs101)
    await new Promise((r) => setTimeout(r, 10))
    expect(result.state.capacity).toBe(60)
  })
})

describe("THE KEY COMPOSITION — `${declared key}:${flattened id}`", () => {
  /** Every key a run of `repo.load` filed something under. */
  async function keysWrittenFor(ids: ReadonlyArray<{ courseId: string }>) {
    const inner = inMemoryEventStore()
    for (const { courseId } of ids) {
      await inner.append([
        eventMsg(CourseCreated, { courseId, name: courseId, capacity: 30 }),
        eventMsg(CourseCapacityChanged, { courseId, capacity: 50 }),
        eventMsg(CourseCapacityChanged, { courseId, capacity: 60 }),
      ])
    }
    const written: string[] = []
    const eventStore: SnapshotCapableEventStore = {
      ...inner,
      async storeSnapshot(key) { written.push(key) },
    }
    const repo = eventSourcedRepository(courseWith(afterEvents(2)), eventStore)
    for (const id of ids) await repo.load(id)
    await new Promise((r) => setTimeout(r, 10))
    return written
  }

  it("gives every id of one state its OWN entry — one declared key, no collisions", async () => {
    const written = await keysWrittenFor([{ courseId: "cs-101" }, { courseId: "cs-202" }])

    expect(written).toEqual([
      `course-v1:${snapshotIdentifier({ courseId: "cs-101" })}`,
      `course-v1:${snapshotIdentifier({ courseId: "cs-202" })}`,
    ])
    expect(written[0]).not.toBe(written[1])
  })

  it("prefixes with the DECLARED key, so renaming it moves every id at once", async () => {
    const written = await keysWrittenFor([{ courseId: "cs-101" }])
    expect(written[0]!.startsWith("course-v1:")).toBe(true)
  })
})

describe("the marker a load reports is unchanged by any of this", () => {
  it("reports the position the read got to, which is what the append condition uses", async () => {
    // given
    const log = capableLog()
    await log.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "CS 101", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
    ])
    const repo = eventSourcedRepository(Course, log.store)

    // when
    const result = await repo.load(cs101)

    // then
    expect(result.sourcingInfo.markerPosition).toBe(markerAt(1n).position)
  })
})
