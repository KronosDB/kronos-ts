import { describe, expect, it } from "bun:test"
import { tag } from "../tag.js"

describe("Tag", () => {
  it("creates a tag with key and value", () => {
    const t = tag("courseId", "cs-101")

    expect(t.key).toBe("courseId")
    expect(t.value).toBe("cs-101")
  })
})
