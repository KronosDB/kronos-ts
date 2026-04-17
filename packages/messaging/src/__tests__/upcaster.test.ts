import { describe, expect, it } from "bun:test"
import {
  singleEventUpcaster,
  upcasterChain,
  upcastingSerializer,
  type IntermediateEventRepresentation,
  type EventUpcaster,
} from "../upcaster.js"
import { jsonSerializer } from "../serializer.js"

describe("singleEventUpcaster", () => {
  const upcaster = singleEventUpcaster({
    typeName: "university.CourseCreated",
    fromRevision: "1.0",
    toRevision: "2.0",
    upcast: (payload: any) => ({
      ...payload,
      capacity: payload.capacity ?? 30,
    }),
  })

  it("upcasts matching events", () => {
    const result = upcaster.upcast({
      payload: { courseId: "cs-101", name: "Intro" },
      typeName: "university.CourseCreated",
      revision: "1.0",
      metadata: {},
    })

    const r = result as IntermediateEventRepresentation
    expect(r.payload).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
    expect(r.revision).toBe("2.0")
  })

  it("matches only the specified type and revision", () => {
    expect(upcaster.canUpcast("university.CourseCreated", "1.0")).toBe(true)
    expect(upcaster.canUpcast("university.CourseCreated", "2.0")).toBe(false)
    expect(upcaster.canUpcast("other.Event", "1.0")).toBe(false)
  })

  it("can upcast metadata", () => {
    const withMetadata = singleEventUpcaster({
      typeName: "Test",
      fromRevision: "1.0",
      toRevision: "2.0",
      upcast: (p) => p,
      upcastMetadata: (m) => ({ ...m, upcasted: true }),
    })

    const result = withMetadata.upcast({
      payload: {},
      typeName: "Test",
      revision: "1.0",
      metadata: { original: true },
    }) as IntermediateEventRepresentation

    expect(result.metadata).toEqual({ original: true, upcasted: true })
  })
})

describe("upcasterChain", () => {
  it("chains multiple upcasters in order", () => {
    const v1ToV2 = singleEventUpcaster({
      typeName: "Course",
      fromRevision: "1.0",
      toRevision: "2.0",
      upcast: (p: any) => ({ ...p, capacity: p.capacity ?? 30 }),
    })

    const v2ToV3 = singleEventUpcaster({
      typeName: "Course",
      fromRevision: "2.0",
      toRevision: "3.0",
      upcast: (p: any) => ({ ...p, department: p.department ?? "CS" }),
    })

    const chain = upcasterChain(v1ToV2, v2ToV3)

    const result = chain.upcast({
      payload: { name: "Intro" },
      typeName: "Course",
      revision: "1.0",
      metadata: {},
    }) as IntermediateEventRepresentation

    // Should go through both upcasters: v1→v2→v3
    expect(result.payload).toEqual({ name: "Intro", capacity: 30, department: "CS" })
    expect(result.revision).toBe("3.0")
  })

  it("passes through events that no upcaster handles", () => {
    const chain = upcasterChain(
      singleEventUpcaster({
        typeName: "Other",
        fromRevision: "1.0",
        toRevision: "2.0",
        upcast: (p) => p,
      }),
    )

    const input: IntermediateEventRepresentation = {
      payload: { untouched: true },
      typeName: "Unrelated",
      revision: "1.0",
      metadata: {},
    }

    const result = chain.upcast(input) as IntermediateEventRepresentation
    expect(result.payload).toEqual({ untouched: true })
  })

  it("handles one-to-many upcasting", () => {
    const splitter: EventUpcaster = {
      canUpcast: (t, r) => t === "BatchEvent" && r === "1.0",
      upcast: (event) => {
        const items = (event.payload as any).items as string[]
        return items.map((item) => ({
          payload: { item },
          typeName: "SingleEvent",
          revision: "1.0",
          metadata: event.metadata,
        }))
      },
    }

    const chain = upcasterChain(splitter)

    const result = chain.upcast({
      payload: { items: ["a", "b", "c"] },
      typeName: "BatchEvent",
      revision: "1.0",
      metadata: {},
    })

    expect(Array.isArray(result)).toBe(true)
    const arr = result as IntermediateEventRepresentation[]
    expect(arr).toHaveLength(3)
    expect(arr[0]!.payload).toEqual({ item: "a" })
    expect(arr[0]!.typeName).toBe("SingleEvent")
  })

  it("canUpcast returns true if any upcaster matches", () => {
    const chain = upcasterChain(
      singleEventUpcaster({ typeName: "A", fromRevision: "1.0", toRevision: "2.0", upcast: (p) => p }),
      singleEventUpcaster({ typeName: "B", fromRevision: "1.0", toRevision: "2.0", upcast: (p) => p }),
    )

    expect(chain.canUpcast("A", "1.0")).toBe(true)
    expect(chain.canUpcast("B", "1.0")).toBe(true)
    expect(chain.canUpcast("C", "1.0")).toBe(false)
  })
})

describe("upcastingSerializer", () => {
  it("upcasts during deserialization", () => {
    const delegate = jsonSerializer()

    const upcaster = singleEventUpcaster({
      typeName: "CourseCreated",
      fromRevision: "1.0",
      toRevision: "2.0",
      upcast: (p: any) => ({ ...p, capacity: 30 }),
    })

    const serializer = upcastingSerializer(delegate, upcaster)

    // Serialize v1 data
    const serialized = delegate.serialize(
      { courseId: "cs-101", name: "Intro" },
      "CourseCreated",
      "1.0",
    )

    // Deserialize should upcast to v2
    const result = serializer.deserialize(serialized) as any
    expect(result).toEqual({ courseId: "cs-101", name: "Intro", capacity: 30 })
  })

  it("passes through when no upcaster matches", () => {
    const delegate = jsonSerializer()

    const upcaster = singleEventUpcaster({
      typeName: "Other",
      fromRevision: "1.0",
      toRevision: "2.0",
      upcast: (p) => p,
    })

    const serializer = upcastingSerializer(delegate, upcaster)

    const serialized = delegate.serialize(
      { data: "original" },
      "Unrelated",
      "1.0",
    )

    const result = serializer.deserialize(serialized)
    expect(result).toEqual({ data: "original" })
  })

  it("serialization passes through to delegate", () => {
    const delegate = jsonSerializer()
    const upcaster = singleEventUpcaster({
      typeName: "X",
      fromRevision: "1",
      toRevision: "2",
      upcast: (p) => p,
    })

    const serializer = upcastingSerializer(delegate, upcaster)

    const serialized = serializer.serialize({ hello: "world" }, "Test", "1.0")
    expect(serialized.type).toBe("Test")

    // Should round-trip without upcasting (type doesn't match)
    const deserialized = serializer.deserialize(serialized)
    expect(deserialized).toEqual({ hello: "world" })
  })
})
