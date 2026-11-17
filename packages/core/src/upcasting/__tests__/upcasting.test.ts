import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { upcastingEventStore, type Upcast } from "../upcasting-event-store.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { event, is, qn, type EventMessage } from "../../messaging/messages.js"
import { state } from "../../event-sourcing/state.js"
import { eventSourcedRepository } from "../../event-sourcing/repository.js"

// The descriptor as it stands TODAY — version 2.0, capacity required. Events
// written under 1.0 are still in the log and still have to fold.
const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  version: "2.0",
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

// The OUTDATED version, declared as its own descriptor. That is the whole
// technique: the old shape gets a name, so `is()` narrows `payload` to what it
// looked like back then and the transform is written against real types.
const CourseCreatedV1 = event({
  name: qn("university", "CourseCreated"),
  version: "1.0",
  payload: z.object({ courseId: z.string(), name: z.string() }), // no capacity back then
  tags: { courseId: (p) => p.courseId },
})

let seq = 0
function stored(overrides: Partial<EventMessage> = {}): EventMessage {
  return {
    kind: "event",
    identifier: `e-${++seq}`,
    name: CourseCreated.name,
    version: "1.0",
    payload: { courseId: "cs-101", name: "Intro" },
    metadata: {},
    timestamp: 1_700_000_000_000,
    tags: [{ key: "courseId", value: "cs-101" }],
    ...overrides,
  }
}

// Written BY HAND — there is no shipped constructor, because writing the match
// is the whole lesson. `is()` makes it a typed switch, and the target version is
// read off the CURRENT descriptor so it can never disagree with itself.
const capacityAdded: Upcast = (e) => {
  if (is(e, CourseCreatedV1)) {
    return { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
  }
  return e
}

describe("an upcaster written with `is()`", () => {
  it("transforms a matching type at a matching version", () => {
    const result = capacityAdded(stored())
    expect(result.payload).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("stamps the version the DESCRIPTOR carries — the target is never restated", () => {
    expect(capacityAdded(stored()).version).toBe("2.0")
    expect(capacityAdded(stored()).version).toBe(CourseCreated.version)
  })

  it("leaves tags alone — they are how the event was INDEXED, not what it means", () => {
    const before = stored()
    expect(capacityAdded(before).tags).toEqual(before.tags)
  })

  it("is identity for another version of the same type", () => {
    const current = stored({ version: "2.0" })
    expect(capacityAdded(current)).toBe(current)
  })

  it("is identity for another type", () => {
    const other = stored({ name: qn("billing", "Charged") })
    expect(capacityAdded(other)).toBe(other)
  })

  it("narrows the payload to the OUTDATED descriptor's shape", () => {
    const e = stored()
    if (is(e, CourseCreatedV1)) {
      // Typed as `{ courseId: string; name: string }` — no `capacity` in sight,
      // which is exactly what the log holds.
      const name: string = e.payload.name
      expect(name).toBe("Intro")
    } else {
      throw new Error("expected the v1 descriptor to match")
    }
  })
})

describe("composing upcasters", () => {
  // Every hop is a plain arrow written the same way — that IS the mechanism.
  const CourseCreatedV0 = event({
    name: qn("university", "CourseCreated"),
    version: "0.9",
    payload: z.object({ courseId: z.string(), title: z.string() }),
    tags: { courseId: (p) => p.courseId },
  })

  const renamed: Upcast = (e) =>
    is(e, CourseCreatedV0)
      ? { ...e, version: CourseCreatedV1.version, payload: { courseId: e.payload.courseId, name: e.payload.title } }
      : e

  const chain: Upcast = (e) => capacityAdded(renamed(e))

  it("runs a multi-hop chain in order, with no chain object", () => {
    const result = chain(stored({ version: "0.9", payload: { courseId: "cs-101", title: "Intro" } }))
    expect(result.version).toBe("2.0")
    expect(result.payload).toMatchObject({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("enters the chain part-way when the stored version says so", () => {
    const result = chain(stored())
    expect(result.version).toBe("2.0")
    expect(result.payload).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("passes an unrelated event through the whole chain untouched", () => {
    const unrelated = stored({ name: qn("billing", "Charged") })
    expect(chain(unrelated)).toBe(unrelated)
  })
})

describe("upcastingEventStore", () => {
  it("upcasts what `source` hands a fold — over a plain in-memory store", async () => {
    const store = upcastingEventStore(inMemoryEventStore(), capacityAdded)
    await store.append([stored()])

    const { events } = await store.source({ query: { tags: { courseId: "cs-101" } } })
    expect(events).toHaveLength(1)
    expect(events[0]!.version).toBe("2.0")
    expect(events[0]!.payload).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("upcasts what `open` streams to a processor", async () => {
    const store = upcastingEventStore(inMemoryEventStore(), capacityAdded)
    await store.append([stored()])

    const stream = store.open({ position: 0n })
    const first = stream.next()
    stream.close()

    expect(first?.event.version).toBe("2.0")
    expect(first?.event.payload).toMatchObject({ capacity: 30 })
  })


  it("WRITES ARE UNTOUCHED — what was appended is what is stored", async () => {
    const inner = inMemoryEventStore()
    const store = upcastingEventStore(inner, capacityAdded)

    await store.append([stored()])

    // Read through the WRAPPED store: upcasted.
    const outer = await store.source({ query: { tags: { courseId: "cs-101" } } })
    expect(outer.events[0]!.version).toBe("2.0")

    // Read through the store underneath: exactly what was written.
    const raw = await inner.source({ query: { tags: { courseId: "cs-101" } } })
    expect(raw.events[0]!.version).toBe("1.0")
    expect(raw.events[0]!.payload).toEqual({ courseId: "cs-101", name: "Intro" })
  })

  it("passes non-event members straight through", async () => {
    const inner = inMemoryEventStore()
    const store = upcastingEventStore(inner, capacityAdded)
    await store.append([stored()])

    expect(await store.getHeadPosition()).toBe(await inner.getHeadPosition())
    expect((await store.latestToken()).position()).toBe((await inner.latestToken()).position())
  })
})

// ---------------------------------------------------------------------------
// A FILTERED STREAM MEETS AN UPCASTER.
//
// A processor opens its stream asking for the types it handles. The log keeps
// the names events were written under, so a type the upcaster renamed must be
// asked for by its OLD name or the events are silently missed.
// ---------------------------------------------------------------------------

describe("upcastingEventStore and a filtered stream", () => {
  const Placed = qn("shop", "OrderPlaced") // what the log has for 2019's orders
  const Created = qn("shop", "OrderCreated") // what the code calls them today

  // An upcaster that RENAMES: nothing in its type says so.
  const renamed: Upcast = (e) =>
    e.name.namespace === Placed.namespace && e.name.name === Placed.name ? { ...e, name: Created, version: "2.0" } : e

  const asked: Array<unknown> = []
  /** An in-memory store that remembers the query each stream was opened with. */
  function recording() {
    const inner = inMemoryEventStore()
    return {
      ...inner,
      open(condition: Parameters<typeof inner.open>[0]) {
        asked.push(condition.query)
        return inner.open(condition)
      },
    }
  }

  async function drain(stream: ReturnType<EventStoreOpen>) {
    const seen: string[] = []
    for (let e = stream.next(); e; e = stream.next()) seen.push(`${e.event.name.name}@${e.event.version}`)
    stream.close()
    return seen
  }
  type EventStoreOpen = ReturnType<typeof inMemoryEventStore>["open"]

  async function seeded() {
    asked.length = 0
    const log = recording()
    await log.append([
      stored({ name: Placed, version: "1.0" }),
      stored({ name: qn("shop", "Shipped"), version: "1.0" }),
      stored({ name: Created, version: "2.0" }),
    ])
    return log
  }

  it("asks the log for the OLD name too, so a renamed event is not missed", async () => {
    const log = await seeded()
    const store = upcastingEventStore(log, renamed, {
      storedTypes: (type) => (type === "shop.OrderCreated" ? [type, "shop.OrderPlaced"] : [type]),
    })

    const seen = await drain(store.open({ position: 0n, query: { types: [Created] } }))

    expect(asked[0]).toEqual({ types: ["shop.OrderCreated", "shop.OrderPlaced"] })
    // Both the renamed one and the one already written under the new name,
    // and what it receives is the upcast form.
    expect(seen).toEqual(["OrderCreated@2.0", "OrderCreated@2.0"])
  })

  it("keeps the tags of the query and translates the types of each item separately", async () => {
    const log = await seeded()
    const store = upcastingEventStore(log, renamed, {
      storedTypes: (type) => (type === "shop.OrderCreated" ? [type, "shop.OrderPlaced"] : [type]),
    })

    store.open({ position: 0n, query: [{ tags: { courseId: "cs-101" }, types: [Created] }, { types: ["shop.Shipped"] }] }).close()

    expect(asked[0]).toEqual([
      { tags: { courseId: "cs-101" }, types: ["shop.OrderCreated", "shop.OrderPlaced"] },
      { types: ["shop.Shipped"] },
    ])
  })

  it("WITHOUT being told, widens the filter instead of trusting it: nothing is missed", async () => {
    const log = await seeded()
    // The same renaming upcaster, and no `storedTypes`.
    const store = upcastingEventStore(log, renamed)

    const seen = await drain(store.open({ position: 0n, query: { types: [Created] } }))

    // The type restriction is gone from what the log was asked...
    expect(asked[0]).toEqual({})
    // ...so the renamed event arrives (and the reader drops the Shipped one itself).
    expect(seen).toContain("OrderCreated@2.0")
    expect(seen.filter((s) => s === "OrderCreated@2.0")).toHaveLength(2)
  })

  it("widening keeps a tag restriction — tags are not re-derived by an upcast", async () => {
    const log = await seeded()
    const store = upcastingEventStore(log, renamed)

    store.open({ position: 0n, query: { tags: { courseId: "cs-101" }, types: [Created] } }).close()

    expect(asked[0]).toEqual({ tags: { courseId: "cs-101" } })
  })

  it("leaves a query with no types, and a stream with no query, exactly as it was", async () => {
    const log = await seeded()
    const store = upcastingEventStore(log, renamed)

    store.open({ position: 0n, query: { tags: { courseId: "cs-101" } } }).close()
    store.open({ position: 0n }).close()

    expect(asked).toEqual([{ tags: { courseId: "cs-101" } }, undefined])
  })

  it("passes where the stream has looked straight through", async () => {
    const log = await seeded()
    const store = upcastingEventStore(log, renamed, { storedTypes: (type) => [type] })

    const stream = store.open({ position: 0n, query: { types: ["shop.Nothing"] } })
    expect(stream.progressToken?.()?.position()).toBe(3n)
    stream.close()

    // And a stream whose log cannot say does not grow the member.
    const plain = upcastingEventStore(log, renamed).open({ position: 0n })
    expect(plain.progressToken).toBeUndefined()
    plain.close()
  })
})

// ---------------------------------------------------------------------------
// The renaming translation on the DCB read, and on the append condition built
// from it.
// ---------------------------------------------------------------------------

describe("upcastingEventStore and a sourced read", () => {
  const Placed = qn("shop", "OrderPlaced")
  const Created = qn("shop", "OrderCreated")
  const OrderCreated = event({
    name: Created,
    version: "2.0",
    payload: z.object({ orderId: z.string() }),
    tags: { orderId: (p) => p.orderId },
  })
  const renamed: Upcast = (e) =>
    e.name.namespace === Placed.namespace && e.name.name === Placed.name ? { ...e, name: Created, version: "2.0" } : e
  const storedTypes = (type: string) => (type === "shop.OrderCreated" ? [type, "shop.OrderPlaced"] : [type])

  function order(name: typeof Placed, orderId = "o-1"): EventMessage {
    return stored({
      name,
      version: name === Placed ? "1.0" : "2.0",
      payload: { orderId },
      tags: [{ key: "orderId", value: orderId }],
    })
  }

  async function seeded() {
    const log = inMemoryEventStore()
    await log.append([order(Placed), stored({ name: qn("shop", "Shipped"), tags: [{ key: "orderId", value: "o-1" }] }), order(Created)])
    return log
  }

  const Order = state({
    id: { orderId: z.string() },
    tags: ({ orderId }) => ({ orderId }),
    evolve: [() => ({ created: 0 }), [OrderCreated, (s) => ({ created: s.created + 1 })]],
  })

  it("sources events stored under the old name of a type the query asks for", async () => {
    const store = upcastingEventStore(await seeded(), renamed, { storedTypes })

    const { events } = await store.source({ query: { tags: { orderId: "o-1" }, types: [Created] } })

    expect(events.map((e) => `${e.name.name}@${e.version}`)).toEqual(["OrderCreated@2.0", "OrderCreated@2.0"])
  })

  it("a state folding the renamed event sees the events stored under the old name", async () => {
    const store = upcastingEventStore(await seeded(), renamed, { storedTypes })

    const { state: loaded } = await eventSourcedRepository(Order, store).load({ orderId: "o-1" })

    expect(loaded).toEqual({ created: 2 })
  })

  it("WITHOUT being told, widens the read and hands back only the asked types", async () => {
    const store = upcastingEventStore(await seeded(), renamed)

    const { events } = await store.source({ query: { tags: { orderId: "o-1" }, types: [Created] } })

    expect(events.map((e) => e.name.name)).toEqual(["OrderCreated", "OrderCreated"])
  })

  it("an append condition on the new name conflicts with an event appended under the old name", async () => {
    const store = upcastingEventStore(await seeded(), renamed, { storedTypes })
    const query = { tags: { orderId: "o-1" }, types: [Created] }
    const { marker } = await store.source({ query })

    await store.append([order(Placed)])

    await expect(store.append([order(Created)], { query, marker })).rejects.toThrow(/Append condition violated/)
  })

  it("translates every read an append condition carries", async () => {
    const store = upcastingEventStore(await seeded(), renamed, { storedTypes })
    const query = { tags: { orderId: "o-1" }, types: [Created] }
    const { marker } = await store.source({ query })

    await store.append([order(Placed)])

    await expect(
      store.append([order(Created)], { query, marker, reads: [{ query, marker }] }),
    ).rejects.toThrow(/Append condition violated/)
  })
})
