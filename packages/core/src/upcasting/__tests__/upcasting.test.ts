import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { upcast, upcastingEventStore } from "../upcasting-event-store.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { event, qn, type EventDescriptor, type EventMessage, type TaggedEvent } from "../../messaging/messages.js"
import { state } from "../../event-sourcing/state.js"
import { eventSourcedRepository } from "../../event-sourcing/repository.js"

// The descriptor as it stands today, and the two versions before it, each
// declared as a descriptor of its own.
const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  version: "3.0",
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number(), department: z.string() }),
  tags: { courseId: (p) => p.courseId },
})
const CourseCreatedV2 = event({
  name: qn("university", "CourseCreated"),
  version: "2.0",
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})
const CourseCreatedV1 = event({
  name: qn("university", "CourseCreated"),
  version: "1.0",
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: { courseId: (p) => p.courseId },
})

const capacityAdded = upcast(CourseCreatedV1, CourseCreatedV2, (payload) => ({ ...payload, capacity: 30 }))
const departmentAdded = upcast(CourseCreatedV2, CourseCreated, (payload) => ({ ...payload, department: "general" }))

let seq = 0
/** An event as the log holds it, written under `descriptor`. */
function stored(
  descriptor: EventDescriptor<any>,
  payload: Record<string, unknown>,
  tags: Record<string, string> = { courseId: String(payload.courseId) },
): TaggedEvent {
  return {
    event: {
      kind: "event",
      identifier: `e-${++seq}`,
      name: descriptor.name,
      version: descriptor.version,
      payload,
      metadata: {},
      timestamp: 1_700_000_000_000,
    },
    tags: Object.entries(tags).map(([key, value]) => ({ key, value })),
  }
}

const onCourse = { query: { tags: { courseId: "cs-101" } } }
const told = (e: EventMessage) => `${e.name.name}@${e.version}`

describe("upcastingEventStore", () => {
  it("hands an event stored under the outdated descriptor out as the newer one", async () => {
    const log = inMemoryEventStore()
    await log.append([stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" })])

    const { events } = await upcastingEventStore(log, [capacityAdded]).source(onCourse)

    expect(events.map(told)).toEqual(["CourseCreated@2.0"])
    expect(events[0]!.payload).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("takes an event through every step that applies, in whatever order they were listed", async () => {
    const log = inMemoryEventStore()
    await log.append([
      stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" }),
      stored(CourseCreatedV2, { courseId: "cs-101", name: "Algebra", capacity: 12 }),
      stored(CourseCreated, { courseId: "cs-101", name: "Logic", capacity: 8, department: "math" }),
    ])

    const { events } = await upcastingEventStore(log, [departmentAdded, capacityAdded]).source(onCourse)

    expect(events.map(told)).toEqual(["CourseCreated@3.0", "CourseCreated@3.0", "CourseCreated@3.0"])
    expect(events.map((e) => e.payload)).toEqual([
      { courseId: "cs-101", name: "Intro", capacity: 30, department: "general" },
      { courseId: "cs-101", name: "Algebra", capacity: 12, department: "general" },
      { courseId: "cs-101", name: "Logic", capacity: 8, department: "math" },
    ])
  })

  it("hands an event no upcast is declared for back as it is", async () => {
    const log = inMemoryEventStore()
    const current = stored(CourseCreated, { courseId: "cs-101", name: "Logic", capacity: 8, department: "math" })
    const other = stored(event({ name: qn("billing", "Charged"), payload: z.object({}) }), { courseId: "cs-101" })
    await log.append([current, other])

    const { events } = await upcastingEventStore(log, [capacityAdded, departmentAdded]).source(onCourse)

    expect(events).toEqual([current.event, other.event])
  })

  it("hands the map the stored event beside its payload", async () => {
    const log = inMemoryEventStore()
    const outdated = stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" })
    await log.append([{ ...outdated, event: { ...outdated.event, metadata: { by: "dean" } } }])
    const stamped = upcast(CourseCreatedV1, CourseCreatedV2, (payload, e) => ({
      ...payload,
      capacity: e.metadata.by === "dean" ? 100 : 30,
    }))

    const { events } = await upcastingEventStore(log, [stamped]).source(onCourse)

    expect(events[0]!.payload).toMatchObject({ capacity: 100 })
  })

  it("upcasts what a stream delivers", async () => {
    const log = inMemoryEventStore()
    await log.append([stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" })])

    const stream = upcastingEventStore(log, [capacityAdded, departmentAdded]).open({ position: 0n })

    expect(told(stream.peek()!.event)).toBe("CourseCreated@3.0")
    expect(stream.next()!.event.payload).toMatchObject({ capacity: 30, department: "general" })
    expect(stream.progressToken).toBeUndefined()
    stream.close()
  })

  it("appends an event as it was given", async () => {
    const log = inMemoryEventStore()
    const store = upcastingEventStore(log, [capacityAdded])
    const outdated = stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" })

    await store.append([outdated])

    expect((await log.source(onCourse)).events).toEqual([outdated.event])
    expect(await store.getHeadPosition()).toBe(1n)
  })

  it("with no upcasts hands back what the store does", async () => {
    const log = inMemoryEventStore()
    const outdated = stored(CourseCreatedV1, { courseId: "cs-101", name: "Intro" })
    await log.append([outdated])

    expect((await upcastingEventStore(log, []).source(onCourse)).events).toEqual([outdated.event])
  })

  it("refuses two upcasts from the same descriptor", () => {
    expect(() => upcastingEventStore(inMemoryEventStore(), [capacityAdded, capacityAdded])).toThrow(
      /more than one upcast is declared from university\.CourseCreated 1\.0/,
    )
  })

  it("refuses upcasts that lead back to where they started", () => {
    const back = upcast(CourseCreatedV2, CourseCreatedV1, ({ courseId, name }) => ({ courseId, name }))
    expect(() => upcastingEventStore(inMemoryEventStore(), [capacityAdded, back])).toThrow(/lead back to it/)
  })
})

// An upcast between descriptors with different names: the events are in the log
// under the old name, and a reader asks for the new one.
describe("upcastingEventStore and an event that was renamed", () => {
  const OrderPlaced = event({
    name: qn("shop", "OrderPlaced"),
    payload: z.object({ orderId: z.string() }),
    tags: { orderId: (p) => p.orderId },
  })
  const OrderCreated = event({
    name: qn("shop", "OrderCreated"),
    version: "2.0",
    payload: z.object({ orderId: z.string() }),
    tags: { orderId: (p) => p.orderId },
  })
  const Shipped = event({
    name: qn("shop", "Shipped"),
    payload: z.object({ orderId: z.string() }),
    tags: { orderId: (p) => p.orderId },
  })
  const renamed = [upcast(OrderPlaced, OrderCreated, (payload) => payload)]
  const order = (descriptor: EventDescriptor<any>, orderId = "o-1") => stored(descriptor, { orderId }, { orderId })

  /** A seeded log that remembers the query of every read, stream and append condition. */
  async function seeded() {
    const inner = inMemoryEventStore()
    await inner.append([order(OrderPlaced), order(Shipped), order(OrderCreated)])
    const asked: unknown[] = []
    const log: typeof inner = {
      ...inner,
      source(condition) {
        asked.push(condition.query)
        return inner.source(condition)
      },
      open(condition) {
        asked.push(condition.query)
        return inner.open(condition)
      },
      append(events, condition, uow) {
        if (condition !== undefined) asked.push(condition.query, ...(condition.reads ?? []).map((read) => read.query))
        return inner.append(events, condition, uow)
      },
    }
    return { log, asked }
  }

  it("asks the log for the old name too, and hands both out under the new one", async () => {
    const { log, asked } = await seeded()
    const store = upcastingEventStore(log, renamed)

    const { events } = await store.source({ query: { tags: { orderId: "o-1" }, types: [OrderCreated] } })

    expect(asked).toEqual([{ tags: { orderId: "o-1" }, types: ["shop.OrderCreated", "shop.OrderPlaced"] }])
    expect(events.map(told)).toEqual(["OrderCreated@2.0", "OrderCreated@2.0"])
  })

  it("asks a stream's log for the old name too", async () => {
    const { log, asked } = await seeded()
    const stream = upcastingEventStore(log, renamed).open({ position: 0n, query: { types: [OrderCreated] } })

    const seen: string[] = []
    for (let e = stream.next(); e; e = stream.next()) seen.push(told(e.event))
    stream.close()

    expect(asked).toEqual([{ types: ["shop.OrderCreated", "shop.OrderPlaced"] }])
    expect(seen).toEqual(["OrderCreated@2.0", "OrderCreated@2.0"])
  })

  it("translates the types of each item and leaves its tags, and a type nothing was renamed to, alone", async () => {
    const { log, asked } = await seeded()
    const store = upcastingEventStore(log, renamed)

    store.open({ position: 0n, query: [{ tags: { orderId: "o-1" }, types: [OrderCreated] }, { types: [Shipped] }] }).close()
    store.open({ position: 0n, query: { tags: { orderId: "o-1" } } }).close()
    store.open({ position: 0n }).close()

    expect(asked).toEqual([
      [{ tags: { orderId: "o-1" }, types: ["shop.OrderCreated", "shop.OrderPlaced"] }, { types: ["shop.Shipped"] }],
      { tags: { orderId: "o-1" } },
      undefined,
    ])
  })

  it("asks the log exactly what the reader asked when no upcast renames", async () => {
    const { log, asked } = await seeded()
    const query = { tags: { orderId: "o-1" }, types: [OrderCreated] }

    await upcastingEventStore(log, [capacityAdded]).source({ query })

    expect(asked[0]).toBe(query)
  })

  it("a state folding the new name sees the events stored under the old one", async () => {
    const { log } = await seeded()
    const Order = state({
      id: { orderId: z.string() },
      tags: ({ orderId }) => ({ orderId }),
      evolve: [() => ({ created: 0 }), [OrderCreated, (s) => ({ created: s.created + 1 })]],
    })

    const { state: loaded } = await eventSourcedRepository(Order, upcastingEventStore(log, renamed)).load({ orderId: "o-1" })

    expect(loaded).toEqual({ created: 2 })
  })

  it("an append condition on the new name conflicts with an event appended under the old one", async () => {
    const { log, asked } = await seeded()
    const store = upcastingEventStore(log, renamed)
    const query = { tags: { orderId: "o-1" }, types: [OrderCreated] }
    const { marker } = await store.source({ query })
    await store.append([order(OrderPlaced)])
    asked.length = 0

    const refused = store.append([order(OrderCreated)], { query, marker, reads: [{ query, marker }] })

    await expect(refused).rejects.toThrow(/Append condition violated/)
    const translated = { tags: { orderId: "o-1" }, types: ["shop.OrderCreated", "shop.OrderPlaced"] }
    expect(asked).toEqual([translated, translated])
  })

  it("drops an event the log returned for the old name that no upcast hands out under the new one", async () => {
    // The old name at a version nothing is declared from.
    const OrderPlacedLater = event({
      name: qn("shop", "OrderPlaced"),
      version: "9.0",
      payload: z.object({ orderId: z.string() }),
      tags: { orderId: (p) => p.orderId },
    })
    const { log } = await seeded()
    await log.append([order(OrderPlacedLater)])

    const { events } = await upcastingEventStore(log, renamed).source({
      query: { tags: { orderId: "o-1" }, types: [OrderCreated] },
    })

    expect(events.map(told)).toEqual(["OrderCreated@2.0", "OrderCreated@2.0"])
  })
})

// A read whose query has several items. The log matched each event against the
// items; what comes back does not say which item an event was read for.
describe("upcastingEventStore and a read of several items", () => {
  const CourseOpened = event({
    name: qn("school", "CourseOpened"),
    payload: z.object({ courseId: z.string() }),
    tags: { courseId: (p) => p.courseId },
  })
  const Created = event({
    name: qn("school", "CourseCreated"),
    payload: z.object({ courseId: z.string() }),
    tags: { courseId: (p) => p.courseId },
  })
  const Subscribed = event({
    name: qn("school", "StudentSubscribed"),
    payload: z.object({ courseId: z.string(), studentId: z.string() }),
    tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
  })
  const renamed = [upcast(CourseOpened, Created, (payload) => payload)]
  const course = (descriptor: EventDescriptor<any>, courseId: string) => stored(descriptor, { courseId })
  const subscription = (courseId: string, studentId: string) =>
    stored(Subscribed, { courseId, studentId }, { courseId, studentId })

  /** A log that remembers how many times it was read. */
  async function seeded() {
    const inner = inMemoryEventStore()
    await inner.append([
      course(CourseOpened, "c-1"),
      course(Created, "c-1"),
      subscription("c-1", "s-1"),
      subscription("c-1", "s-2"),
      subscription("c-2", "s-1"),
    ])
    let reads = 0
    const log: typeof inner = {
      ...inner,
      async source(condition) {
        reads++
        return inner.source(condition)
      },
    }
    return { log, reads: () => reads }
  }

  const said = (e: EventMessage) => {
    const p = e.payload as { courseId: string; studentId?: string }
    return `${e.name.name}:${p.courseId}${p.studentId === undefined ? "" : `:${p.studentId}`}`
  }

  it("keeps what each item asked for, with no tags on the events to go by", async () => {
    const { log } = await seeded()

    const { events } = await upcastingEventStore(log, renamed).source({
      query: [
        { tags: { courseId: "c-1" }, types: [Created] },
        { tags: { studentId: "s-1" }, types: [Subscribed] },
      ],
    })

    // Not the other student's subscription to c-1: no item asked for it.
    expect(events.map(said)).toEqual([
      "CourseCreated:c-1",
      "CourseCreated:c-1",
      "StudentSubscribed:c-1:s-1",
      "StudentSubscribed:c-2:s-1",
    ])
  })

  it("reads items that ask for the same types under different tags", async () => {
    const { log } = await seeded()

    const { events } = await upcastingEventStore(log, renamed).source({
      query: [
        { tags: { courseId: "c-2" }, types: [Created, Subscribed] },
        { tags: { studentId: "s-2" }, types: [Created, Subscribed] },
      ],
    })

    expect(events.map(said)).toEqual(["StudentSubscribed:c-1:s-2", "StudentSubscribed:c-2:s-1"])
  })

  it("reads an item with no types beside one with types", async () => {
    const { log } = await seeded()

    const { events } = await upcastingEventStore(log, renamed).source({
      query: [{ tags: { courseId: "c-1" }, types: [Created] }, { tags: { courseId: "c-1", studentId: "s-2" } }],
    })

    expect(events.map(said)).toEqual(["CourseCreated:c-1", "CourseCreated:c-1", "StudentSubscribed:c-1:s-2"])
  })

  it("refuses a read when one stored name is handed out as types that items with different tags ask for", async () => {
    // Two versions of one stored name, renamed to two different types.
    const Old = (version: string) =>
      event({ name: qn("school", "Enrolment"), version, payload: z.object({}), tags: { courseId: () => "c-1" } })
    const split = [upcast(Old("1.0"), Created, () => ({ courseId: "c-1" })), upcast(Old("2.0"), Subscribed, () => ({ courseId: "c-1", studentId: "s-1" }))]
    const { log, reads } = await seeded()

    const refused = upcastingEventStore(log, split).source({
      query: [
        { tags: { courseId: "c-1" }, types: [Created] },
        { tags: { studentId: "s-1" }, types: [Subscribed] },
      ],
    })

    await expect(refused).rejects.toThrow(/cannot tell which item.*school\.Enrolment/s)
    expect(reads()).toBe(0)
  })
})
