import { describe, test, expect } from "bun:test"
import { generateIdentifier } from "../identifier.js"

describe("generateIdentifier", () => {
  test("returns a string matching the UUID v7 shape", () => {
    const id = generateIdentifier()
    expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/)
  })

  test("two ids generated >1ms apart are lexicographically ordered", async () => {
    const id1 = generateIdentifier()
    await new Promise<void>((r) => setTimeout(r, 2))
    const id2 = generateIdentifier()
    expect(id2 > id1).toBe(true)
  })

  test("produces unique ids across a tight batch", () => {
    const ids = new Set<string>()
    for (let i = 0; i < 100; i++) {
      ids.add(generateIdentifier())
    }
    expect(ids.size).toBe(100)
  })
})
