/**
 * `ctx.source(query)` — THE RAW LAYER.
 *
 * `state()` derives a query and folds for you; this is the layer under it. You
 * write the query, you run the fold — and the append condition still holds,
 * which is the whole reason it is a context capability instead of a store call
 * a handler could have made itself.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  qn,
  emptyMetadata,
  command,
  event,
  is,
  queryDescriptor,
  type EventDescriptor,
  type EventMessage,
} from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { commandHandler } from "../../command-handling/handler.js"
import { queryHandler } from "../../query-handling/handler.js"
import { queryHandlerContext } from "../../query-handling/context.js"
import { eventHandlerContext } from "../../event-processing/context.js"
import { kronos } from "../../kronos.js"
import {
  send,
  query as ask,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  upcastingEventStore,
} from "../../index.js"
import { inMemoryEventStore, AppendConditionError } from "../in-memory.js"
import type { EventStore } from "../event-store.js"

// ─── Domain ─────────────────────────────────────────────────────────────────

const CourseCreated = event({
  name: qn("raw", "CourseCreated"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribed = event({
  name: qn("raw", "StudentSubscribed"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

const StudentEnrolledInFaculty = event({
  name: qn("raw", "StudentEnrolledInFaculty"),
  payload: z.object({ studentId: z.string() }),
  tags: { studentId: (p) => p.studentId },
})

const Subscribe = command({
  name: qn("raw", "Subscribe"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

const CountSubscribers = queryDescriptor({
  name: qn("raw", "CountSubscribers"),
  payload: z.object({ courseId: z.string() }),
})

// ─── Helpers ────────────────────────────────────────────────────────────────

/** A fact, written straight to the log — the "given" of these tests. */
function fact(descriptor: EventDescriptor<any>, payload: any, version?: string): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: descriptor.name,
    version: version ?? descriptor.version,
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: descriptor.tags ? descriptor.tags(payload) : [],
  }
}

function buses(eventStore: EventStore) {
  const uow = () => unitOfWork()
  const commandBus = localCommandBus(uow)
  const queryBus = localQueryBus(uow)
  return { commandBus, queryBus, eventStore }
}

// ─── The read itself ────────────────────────────────────────────────────────

describe("ctx.source — the raw read", () => {
  it("runs a types + tags query against the entry's log, in stream order", async () => {
    const store = inMemoryEventStore()
    await store.append([
      fact(CourseCreated, { courseId: "cs-101", capacity: 30 }),
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-1" }),
      fact(StudentSubscribed, { courseId: "cs-102", studentId: "stu-2" }),
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-3" }),
    ])

    const uow = unitOfWork()
    const ctx = eventHandlerContext({ uow, eventStore: store })
    const seen = await uow.execute(async () =>
      ctx.source({ tags: { courseId: "cs-101" }, types: [StudentSubscribed] }),
    )

    expect(seen.map((e) => (e.payload as { studentId: string }).studentId)).toEqual([
      "stu-1",
      "stu-3",
    ])
  })

  it("an ARRAY of items is an OR", async () => {
    const store = inMemoryEventStore()
    await store.append([
      fact(CourseCreated, { courseId: "cs-101", capacity: 30 }),
      fact(StudentEnrolledInFaculty, { studentId: "stu-1" }),
      fact(CourseCreated, { courseId: "cs-999", capacity: 5 }),
    ])

    const uow = unitOfWork()
    const ctx = eventHandlerContext({ uow, eventStore: store })
    const seen = await uow.execute(async () =>
      ctx.source([{ tags: { courseId: "cs-101" } }, { tags: { studentId: "stu-1" } }]),
    )

    expect(seen.map((e) => e.name.name)).toEqual(["CourseCreated", "StudentEnrolledInFaculty"])
  })

  it("`types` accepts descriptors, qualified names and plain strings alike", async () => {
    const store = inMemoryEventStore()
    await store.append([
      fact(CourseCreated, { courseId: "cs-101", capacity: 30 }),
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-1" }),
    ])

    const uow = unitOfWork()
    const ctx = eventHandlerContext({ uow, eventStore: store })

    await uow.execute(async () => {
      const byDescriptor = await ctx.source({ tags: { courseId: "cs-101" }, types: [CourseCreated] })
      const byQualifiedName = await ctx.source({
        tags: { courseId: "cs-101" },
        types: [CourseCreated.name],
      })
      const byString = await ctx.source({
        tags: { courseId: "cs-101" },
        types: ["raw.CourseCreated"],
      })

      expect(byDescriptor).toHaveLength(1)
      expect(byQualifiedName).toEqual(byDescriptor)
      expect(byString).toEqual(byDescriptor)
    })
  })

  it("sees UPCASTED events — it reads the entry's store, whatever that store is composed of", async () => {
    // The 2019 shape: no capacity back then.
    const CourseCreatedV1 = event({
      name: qn("raw", "CourseCreated"),
      version: "0.9",
      payload: z.object({ courseId: z.string() }),
      tags: { courseId: (p) => p.courseId },
    })

    const inner = inMemoryEventStore()
    await inner.append([fact(CourseCreatedV1, { courseId: "cs-101" }, "0.9")])

    const store = upcastingEventStore(inner, (e) =>
      is(e, CourseCreatedV1)
        ? { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
        : e,
    )

    const uow = unitOfWork()
    const ctx = eventHandlerContext({ uow, eventStore: store })
    const seen = await uow.execute(async () => ctx.source({ tags: { courseId: "cs-101" } }))

    expect(seen).toHaveLength(1)
    expect(seen[0]!.version).toBe(CourseCreated.version)
    expect(seen[0]!.payload).toEqual({ courseId: "cs-101", capacity: 30 })
  })

  it("is a pure read on a QUERY context — no append there to condition, but the read still works", async () => {
    const store = inMemoryEventStore()
    await store.append([
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-1" }),
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-2" }),
    ])

    const countSubscribers = queryHandler(CountSubscribers, async ({ payload }, ctx) => {
      const events = await ctx.source({
        tags: { courseId: payload.courseId },
        types: [StudentSubscribed],
      })
      return events.length
    })

    const wiring = buses(store)
    const app = kronos({ queryHandlers: [{ ...countSubscribers, ...wiring }] })
    try {
      expect(await ask(wiring.queryBus, CountSubscribers, { courseId: "cs-101" })).toBe(2)
    } finally {
      await app.stop()
    }
  })

  it("without an `eventStore` on the entry it says so, and says where to attach one", async () => {
    const uow = unitOfWork()
    const ctx = queryHandlerContext({ uow })
    await expect(
      uow.execute(async () => ctx.source({ tags: { courseId: "cs-101" } })),
    ).rejects.toThrow(/ctx\.source.*needs a log to source from.*eventStore/s)
  })
})

// ─── The guarantee ──────────────────────────────────────────────────────────

describe("ctx.source — the append condition", () => {
  /**
   * A handler that reads by hand and then writes. `interleave` runs as if
   * ANOTHER task committed between this handling's read and its flush.
   */
  const subscribeReadingRaw = (
    types: ReadonlyArray<EventDescriptor<any>> | undefined,
    interleave: (store: EventStore) => Promise<void>,
    store: EventStore,
  ) =>
    commandHandler(Subscribe, async ({ payload }, ctx) => {
      await ctx.source(
        types === undefined
          ? { tags: { courseId: payload.courseId } }
          : { tags: { courseId: payload.courseId }, types },
      )
      await interleave(store)
      ctx.append(StudentSubscribed, payload)
    })

  it("a concurrent MATCHING append makes this task's append fail", async () => {
    const store = inMemoryEventStore()
    await store.append([fact(CourseCreated, { courseId: "cs-101", capacity: 2 })])

    const handler = subscribeReadingRaw(
      [StudentSubscribed],
      async (s) => {
        // Another task, same course, same type — exactly what was read.
        await s.append([fact(StudentSubscribed, { courseId: "cs-101", studentId: "rival" })])
      },
      store,
    )

    const wiring = buses(store)
    const app = kronos({ commandHandlers: [{ ...handler, ...wiring }] })
    try {
      await expect(
        send(wiring.commandBus, Subscribe, { courseId: "cs-101", studentId: "stu-1" }, emptyMetadata()),
      ).rejects.toThrow(AppendConditionError)
    } finally {
      await app.stop()
    }
  })

  it("a concurrent DISJOINT append does not — a different course is not this read", async () => {
    const store = inMemoryEventStore()
    await store.append([fact(CourseCreated, { courseId: "cs-101", capacity: 2 })])

    const handler = subscribeReadingRaw(
      [StudentSubscribed],
      async (s) => {
        await s.append([fact(StudentSubscribed, { courseId: "cs-999", studentId: "rival" })])
      },
      store,
    )

    const wiring = buses(store)
    const app = kronos({ commandHandlers: [{ ...handler, ...wiring }] })
    try {
      await send(
        wiring.commandBus,
        Subscribe,
        { courseId: "cs-101", studentId: "stu-1" },
        emptyMetadata(),
      )
      const written = await store.source({
        query: { tags: { courseId: "cs-101" }, types: [StudentSubscribed] },
      })
      expect(written.events).toHaveLength(1)
    } finally {
      await app.stop()
    }
  })

  it("declaring `types` NARROWS the conflict window; omitting it widens it", async () => {
    // The same interleaved event — a course capacity change on the same course
    // — is a conflict for a read that named no types, and none for a read that
    // named only the subscription type.
    const CourseCapacityChanged = event({
      name: qn("raw", "CourseCapacityChanged"),
      payload: z.object({ courseId: z.string(), capacity: z.number() }),
      tags: { courseId: (p) => p.courseId },
    })
    const interleave = async (s: EventStore) => {
      await s.append([fact(CourseCapacityChanged, { courseId: "cs-101", capacity: 99 })])
    }

    const narrowStore = inMemoryEventStore()
    const narrow = buses(narrowStore)
    const narrowApp = kronos({
      commandHandlers: [
        { ...subscribeReadingRaw([StudentSubscribed], interleave, narrowStore), ...narrow },
      ],
    })
    try {
      await send(
        narrow.commandBus,
        Subscribe,
        { courseId: "cs-101", studentId: "stu-1" },
        emptyMetadata(),
      )
    } finally {
      await narrowApp.stop()
    }

    const wideStore = inMemoryEventStore()
    const wide = buses(wideStore)
    const wideApp = kronos({
      commandHandlers: [{ ...subscribeReadingRaw(undefined, interleave, wideStore), ...wide }],
    })
    try {
      await expect(
        send(wide.commandBus, Subscribe, { courseId: "cs-101", studentId: "stu-1" }, emptyMetadata()),
      ).rejects.toThrow(AppendConditionError)
    } finally {
      await wideApp.stop()
    }
  })
})

// ─── The idiom ──────────────────────────────────────────────────────────────

describe("ctx.source — the documented idiom: is() + reduce", () => {
  it("a hand-rolled fold decides, and its append is conditioned on what it read", async () => {
    const store = inMemoryEventStore()
    await store.append([
      fact(CourseCreated, { courseId: "cs-101", capacity: 2 }),
      fact(StudentSubscribed, { courseId: "cs-101", studentId: "stu-1" }),
    ])

    const subscribe = commandHandler(Subscribe, async ({ payload }, ctx) => {
      const events = await ctx.source({
        tags: { courseId: payload.courseId },
        types: [CourseCreated, StudentSubscribed],
      })

      // The fold, written out. `is()` narrows the payload per case, so this is
      // the same typed switch an upcaster is written with.
      const course = events.reduce(
        (s, e) => {
          if (is(e, CourseCreated)) return { ...s, capacity: e.payload.capacity }
          if (is(e, StudentSubscribed)) return { ...s, taken: s.taken + 1 }
          return s
        },
        { capacity: 0, taken: 0 },
      )

      if (course.taken >= course.capacity) throw new Error("course is full")
      ctx.append(StudentSubscribed, payload)
    })

    const wiring = buses(store)
    const app = kronos({ commandHandlers: [{ ...subscribe, ...wiring }] })
    try {
      await send(wiring.commandBus, Subscribe, { courseId: "cs-101", studentId: "stu-2" }, emptyMetadata())

      await expect(
        send(wiring.commandBus, Subscribe, { courseId: "cs-101", studentId: "stu-3" }, emptyMetadata()),
      ).rejects.toThrow(/course is full/)

      const written = await store.source({
        query: { tags: { courseId: "cs-101" }, types: [StudentSubscribed] },
      })
      expect(written.events).toHaveLength(2)
    } finally {
      await app.stop()
    }
  })
})
