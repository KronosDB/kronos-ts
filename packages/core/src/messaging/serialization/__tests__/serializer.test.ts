import { describe, expect, it } from "bun:test"
import { jsonSerializer } from "../serializer.js"

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
