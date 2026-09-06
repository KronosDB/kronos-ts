import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { upcastingEventStore, type Upcast } from "../upcasting-event-store.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { event, is, qn, type EventMessage } from "../../messaging/messages.js"

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
