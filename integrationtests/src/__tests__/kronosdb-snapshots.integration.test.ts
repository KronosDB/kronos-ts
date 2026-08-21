/**
 * KRONOSDB NATIVE SNAPSHOTS — the fused read, against a real server.
 *
 * KronosDB 0.8 serves snapshots itself (ADR-0005: "snapshots ride the log").
 * `kronosDbSnapshottingEventStore` no longer fuses anything client-side — it
 * makes ONE `SnapshottedSource` call and reads the oneof stream the server
 * sends back: at most one snapshot frame, always first, then event batches,
 * then the same consistency marker a plain `Source` ends with.
 *
 * These tests hold the wrapper to that contract on the two axes that matter:
 *
 *   THE FLOOR — events at or after the snapshot's fold marker come back, and
 *   the ones it already summarizes do not. The marker is NEXT-EXCLUSIVE, so
 *   the boundary is `>=`, and a test that only checked "some events were
 *   skipped" would pass just as happily on the `+ 1` that silently drops one.
 *
 *   THE MARKER — a fused read ends where a plain read ends, so an append
 *   condition built from one holds exactly as it would from the other. That is
 *   what makes the snapshot an optimisation and not a change of meaning.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { z } from "zod"
import {
  qn,
  event,
  state,
  afterEvents,
  snapshotIdentifier,
  repositoryFor,
  emptyMetadata,
  generateIdentifier,
  jsonSerializer,
  type EventDescriptor,
  type EventMessage,
  type EventStore,
  type SnapshotCapability,
} from "@kronos-ts/core"
import {
  kronosDbConnection,
  kronosDbEventStore,
  kronosDbSnapshottingEventStore,
  type KronosDbConnectionHandle,
} from "@kronos-ts/kronosdb"

// ── Domain ──────────────────────────────────────────────────────────────────

const CourseCreated = event({
  name: qn("kronosdb-snap", "CourseCreated"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("kronosdb-snap", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

type CourseState = { created: boolean; capacity: number; enrolled: string[] }

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    () => ({ created: false, capacity: 0, enrolled: [] }) as CourseState,
    [CourseCreated, (s, { payload: e }) => ({ ...s, created: true, capacity: e.capacity })],
    [StudentSubscribed, (s, { payload: e }) => ({ ...s, enrolled: [...s.enrolled, e.studentId] })],
  ],
  // Two events folded is enough to earn an entry — this suite is about the
  // wire path, not about tuning a policy.
  snapshot: { key: "course-v1", when: afterEvents(2) },
})

/** One event, built at the raw layer — no command bus, no unit of work. */
function fact(descriptor: EventDescriptor<any>, payload: any): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: descriptor.name,
    version: descriptor.version,
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: descriptor.tags ? descriptor.tags(payload) : [],
  }
}

const courseQuery = (courseId: string) => ({ tags: { courseId } })

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await predicate()) return
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error("waitFor: condition never became true")
}

// ── Suite ───────────────────────────────────────────────────────────────────

describe("KronosDB native snapshots — one fused RPC", () => {
  let container: StartedTestContainer
  let backend: KronosDbConnectionHandle
  let store: EventStore & SnapshotCapability
  let plain: EventStore

  beforeAll(async () => {
    container = await new GenericContainer("ghcr.io/kronosdb/kronosdb:0.8.0")
      .withExposedPorts(50051, 9240)
      // "KronosDB starting" logs when the gRPC listener BINDS, which is before
      // the raft leader gate opens — an append that wins that race is rejected
      // with "context is not installed in the active leader epoch". The admin
      // `/ready` endpoint is the actual readiness signal.
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()

    backend = await kronosDbConnection({
      componentName: "kronosdb-snapshot-test",
      host: container.getHost(),
      port: container.getMappedPort(50051),
      context: "default",
      serializer: jsonSerializer(),
    })

    plain = kronosDbEventStore(backend, "default")
    store = kronosDbSnapshottingEventStore(plain, backend, "default")
  }, 120_000)

  afterAll(async () => {
    await backend?.close?.()
    await container?.stop()
  }, 60_000)

  it("a fused read with no stored snapshot behaves exactly as a plain Source", async () => {
    const courseId = "miss-101"
    await store.append([
      fact(CourseCreated, { courseId, capacity: 5 }),
      fact(StudentSubscribed, { courseId, studentId: "stu-1" }),
    ])

    const fused = await store.source({
      query: courseQuery(courseId),
      snapshot: { key: `nothing-was-ever-filed-here:${courseId}` },
    })
    const bare = await plain.source({ query: courseQuery(courseId) })

    // No snapshot frame, and the events and marker are the plain read's.
    expect(fused.snapshot).toBeUndefined()
    expect(fused.events.length).toBe(2)
    expect(fused.events.map((e) => e.name)).toEqual(bare.events.map((e) => e.name))
    expect(fused.marker.position).toBe(bare.marker.position)
  }, 60_000)

  it("stores a fold and leads the next read with it, byte-exact", async () => {
    const courseId = "roundtrip-101"
    const key = `course-v1:${courseId}`
    await store.append([fact(CourseCreated, { courseId, capacity: 5 })])

    const { marker } = await store.source({ query: courseQuery(courseId) })
    const folded: CourseState = { created: true, capacity: 5, enrolled: ["stu-1", "stu-2"] }
    await store.storeSnapshot(key, { state: folded, position: marker.position })

    const fused = await store.source({ query: courseQuery(courseId), snapshot: { key } })

    expect(fused.snapshot).toBeDefined()
    // The state comes back as it went in — the server stores opaque bytes.
    expect(fused.snapshot!.state).toEqual(folded)
    // And the fold marker goes over the wire UNMODIFIED, in both directions.
    expect(fused.snapshot!.position).toBe(marker.position)
  }, 60_000)

  it("resumes AT the fold marker — an event that landed after it is not lost", async () => {
    // THE OFF-BY-ONE GUARD. The marker is next-exclusive, so an event that
    // lands at or after it is NOT summarized by the state filed with it.
    // Resuming at `position + 1` — which the deleted client-side fusion did —
    // would drop exactly this event.
    const courseId = "floor-101"
    const key = `course-v1:${courseId}`

    await store.append([
      fact(CourseCreated, { courseId, capacity: 5 }),
      fact(StudentSubscribed, { courseId, studentId: "stu-1" }),
      fact(StudentSubscribed, { courseId, studentId: "stu-2" }),
    ])

    // Fold everything so far, and keep the marker that fold was computed at.
    const { events: folded, marker } = await store.source({ query: courseQuery(courseId) })
    expect(folded.length).toBe(3)

    // A fourth event lands BETWEEN the fold and the snapshot write — the exact
    // interleaving ADR-0005 names. It sits AT the marker, so the state filed
    // below does not summarize it.
    await store.append([fact(StudentSubscribed, { courseId, studentId: "stu-3" })])

    // ...and only THEN is the snapshot written, for the earlier marker.
    await store.storeSnapshot(key, {
      state: { created: true, capacity: 5, enrolled: ["stu-1", "stu-2"] } satisfies CourseState,
      position: marker.position,
    })

    const fused = await store.source({ query: courseQuery(courseId), snapshot: { key } })

    expect(fused.snapshot).toBeDefined()
    // Exactly the event the snapshot does not summarize — no more, no fewer.
    expect(fused.events.length).toBe(1)
    expect((fused.events[0]!.payload as { studentId: string }).studentId).toBe("stu-3")

    // AND THE GUARD BITES: replaying from `position + 1` — precisely what the
    // deleted client-side fusion did — loses that event entirely. Asserting
    // this makes the test about the BOUNDARY rather than about a count that a
    // broken implementation could also produce.
    const offByOne = await plain.source({
      query: courseQuery(courseId),
      start: marker.position + 1n,
    })
    expect(offByOne.events.length).toBe(0)
  }, 60_000)

  it("ends on the same marker a plain read ends on", async () => {
    const courseId = "marker-101"
    const key = `course-v1:${courseId}`
    await store.append([
      fact(CourseCreated, { courseId, capacity: 5 }),
      fact(StudentSubscribed, { courseId, studentId: "stu-1" }),
    ])

    const { marker: foldMarker } = await store.source({ query: courseQuery(courseId) })
    await store.storeSnapshot(key, {
      state: { created: true, capacity: 5, enrolled: ["stu-1"] } satisfies CourseState,
      position: foldMarker.position,
    })

    const fused = await store.source({ query: courseQuery(courseId), snapshot: { key } })
    const bare = await plain.source({ query: courseQuery(courseId) })

    // Same tail, so the two reads are interchangeable as a basis for an append.
    expect(fused.marker.position).toBe(bare.marker.position)
  }, 60_000)

  it("the append condition from a fused read still holds — conflicting fails", async () => {
    const courseId = "conflict-101"
    const key = `course-v1:${courseId}`
    await store.append([fact(CourseCreated, { courseId, capacity: 5 })])

    const { marker: foldMarker } = await store.source({ query: courseQuery(courseId) })
    await store.storeSnapshot(key, {
      state: { created: true, capacity: 5, enrolled: [] } satisfies CourseState,
      position: foldMarker.position,
    })

    // Read through the snapshot, and hold on to the marker it ended with.
    const fused = await store.source({ query: courseQuery(courseId), snapshot: { key } })

    // A rival writes exactly what this read was reading.
    await store.append([fact(StudentSubscribed, { courseId, studentId: "rival" })])

    // The condition was built on a view that is now stale — it must fail.
    await expect(
      store.append([fact(StudentSubscribed, { courseId, studentId: "stu-1" })], {
        marker: fused.marker,
        query: courseQuery(courseId),
      }),
    ).rejects.toThrow()
  }, 60_000)

  it("the append condition from a fused read still holds — disjoint succeeds", async () => {
    const courseId = "disjoint-101"
    const other = "disjoint-999"
    const key = `course-v1:${courseId}`
    await store.append([fact(CourseCreated, { courseId, capacity: 5 })])

    const { marker: foldMarker } = await store.source({ query: courseQuery(courseId) })
    await store.storeSnapshot(key, {
      state: { created: true, capacity: 5, enrolled: [] } satisfies CourseState,
      position: foldMarker.position,
    })

    const fused = await store.source({ query: courseQuery(courseId), snapshot: { key } })

    // A rival writes to a DIFFERENT course — not what this read read.
    await store.append([fact(CourseCreated, { courseId: other, capacity: 1 })])

    // Disjoint, so the condition holds and the append lands.
    await store.append([fact(StudentSubscribed, { courseId, studentId: "stu-1" })], {
      marker: fused.marker,
      query: courseQuery(courseId),
    })

    const after = await store.source({ query: courseQuery(courseId) })
    expect(after.events.length).toBe(2)
  }, 60_000)

  it("a declared snapshot policy writes through AppendSnapshot, and the next load seeds from it", async () => {
    // The state value carries `snapshot: { key, when: afterEvents(2) }`, so the
    // repository writes the entry itself — fire-and-forget, hence the poll.
    const courseId = "policy-101"
    const key = `course-v1:${snapshotIdentifier({ courseId })}`
    const repository = repositoryFor(Course, store)

    await store.append([
      fact(CourseCreated, { courseId, capacity: 5 }),
      fact(StudentSubscribed, { courseId, studentId: "stu-1" }),
      fact(StudentSubscribed, { courseId, studentId: "stu-2" }),
    ])

    // A load that folds three events trips afterEvents(2).
    const loaded = await repository.load({ courseId })
    expect(loaded.state.enrolled).toEqual(["stu-1", "stu-2"])

    const cached = async () =>
      (await store.source({ query: courseQuery(courseId), snapshot: { key } })).snapshot

    await waitFor(async () => (await cached()) !== undefined)

    const snapshot = await cached()
    expect(snapshot).toBeDefined()
    // The entry is the FOLDED STATE, not the events it was folded from.
    expect((snapshot!.state as CourseState).enrolled).toEqual(["stu-1", "stu-2"])

    // EVENT-COUNT EVIDENCE that the next load is actually seeded from it: the
    // fused read hands back the cached fold and NO events, because the
    // snapshot already summarizes every event matching this query.
    const seeded = await store.source({ query: courseQuery(courseId), snapshot: { key } })
    expect(seeded.snapshot).toBeDefined()
    expect(seeded.events.length).toBe(0)

    // And the state the repository returns is identical either way.
    const reloaded = await repository.load({ courseId })
    expect(reloaded.state).toEqual(loaded.state)
  }, 60_000)
})
