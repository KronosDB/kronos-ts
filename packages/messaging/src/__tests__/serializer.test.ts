import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  jsonSerializer,
  zodValidatingSerializer,
  eventSchemaRegistry,
} from "../serializer.js"

describe("JsonSerializer", () => {
  const serializer = jsonSerializer()

  it("serializes and deserializes an object", () => {
    const value = { courseId: "cs-101", name: "Intro to CS" }
    const serialized = serializer.serialize(value, "CourseCreated", "1.0")

    expect(serialized.type).toBe("CourseCreated")
    expect(serialized.revision).toBe("1.0")

    const deserialized = serializer.deserialize<typeof value>(serialized)
    expect(deserialized).toEqual(value)
  })

  it("serializes primitives", () => {
    const s1 = serializer.serialize(42, "Number")
    expect(serializer.deserialize(s1)).toBe(42)

    const s2 = serializer.serialize("hello", "String")
    expect(serializer.deserialize(s2)).toBe("hello")

    const s3 = serializer.serialize(true, "Boolean")
    expect(serializer.deserialize(s3)).toBe(true)

    const s4 = serializer.serialize(null, "Null")
    expect(serializer.deserialize(s4)).toBeNull()
  })

  it("serializes arrays", () => {
    const value = [1, 2, 3]
    const serialized = serializer.serialize(value, "Array")
    expect(serializer.deserialize(serialized)).toEqual([1, 2, 3])
  })

  it("defaults revision to empty string", () => {
    const serialized = serializer.serialize({}, "Type")
    expect(serialized.revision).toBe("")
  })

  it("handles empty data", () => {
    const result = serializer.deserialize({
      type: "Empty",
      revision: "",
      data: new Uint8Array(0),
    })
    expect(result).toBeUndefined()
  })

  it("canConvert returns true for everything", () => {
    expect(serializer.canConvert("anything")).toBe(true)
  })
})

describe("ZodValidatingSerializer", () => {
  it("validates deserialized data against registered schema", () => {
    const registry = eventSchemaRegistry()
    const schema = z.object({
      courseId: z.string(),
      name: z.string(),
    })
    registry.register("CourseCreated", "1.0", schema)

    const serializer = zodValidatingSerializer(jsonSerializer(), registry)

    const serialized = serializer.serialize(
      { courseId: "cs-101", name: "Intro" },
      "CourseCreated",
      "1.0",
    )

    const result = serializer.deserialize(serialized)
    expect(result).toEqual({ courseId: "cs-101", name: "Intro" })
  })

  it("throws on invalid data when schema exists", () => {
    const registry = eventSchemaRegistry()
    registry.register(
      "CourseCreated",
      "1.0",
      z.object({ courseId: z.string(), name: z.string() }),
    )

    const delegate = jsonSerializer()
    const serializer = zodValidatingSerializer(delegate, registry)

    // Serialize invalid data (missing name field)
    const serialized = delegate.serialize(
      { courseId: "cs-101" },
      "CourseCreated",
      "1.0",
    )

    expect(() => serializer.deserialize(serialized)).toThrow()
  })

  it("passes through when no schema is registered", () => {
    const registry = eventSchemaRegistry()
    const serializer = zodValidatingSerializer(jsonSerializer(), registry)

    const serialized = serializer.serialize(
      { anything: "goes" },
      "Unknown",
      "1.0",
    )

    const result = serializer.deserialize(serialized)
    expect(result).toEqual({ anything: "goes" })
  })

  it("falls back to no-revision schema", () => {
    const registry = eventSchemaRegistry()
    registry.register(
      "CourseCreated",
      "",
      z.object({ courseId: z.string() }),
    )

    const serializer = zodValidatingSerializer(jsonSerializer(), registry)

    // Serialize with a specific revision, but only "" revision is registered
    const serialized = serializer.serialize(
      { courseId: "cs-101" },
      "CourseCreated",
      "2.0",
    )

    const result = serializer.deserialize(serialized)
    expect(result).toEqual({ courseId: "cs-101" })
  })
})

describe("SchemaRegistry", () => {
  it("isolates by type name", () => {
    const registry = eventSchemaRegistry()
    const schema1 = z.object({ a: z.string() })
    const schema2 = z.object({ b: z.number() })

    registry.register("TypeA", "1.0", schema1)
    registry.register("TypeB", "1.0", schema2)

    expect(registry.get("TypeA", "1.0")).toBe(schema1)
    expect(registry.get("TypeB", "1.0")).toBe(schema2)
  })

  it("isolates by revision", () => {
    const registry = eventSchemaRegistry()
    const v1 = z.object({ name: z.string() })
    const v2 = z.object({ name: z.string(), capacity: z.number() })

    registry.register("Course", "1.0", v1)
    registry.register("Course", "2.0", v2)

    expect(registry.get("Course", "1.0")).toBe(v1)
    expect(registry.get("Course", "2.0")).toBe(v2)
  })

  it("returns undefined for unknown types", () => {
    const registry = eventSchemaRegistry()
    expect(registry.get("Unknown", "1.0")).toBeUndefined()
  })
})
