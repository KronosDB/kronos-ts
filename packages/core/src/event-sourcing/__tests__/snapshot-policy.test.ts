import { describe, it, expect } from "bun:test"
import { afterEvents, whenSourcingTimeExceeds } from "../snapshot.js"

describe("SnapshotPolicy", () => {
  describe("afterEvents", () => {
    it("is due once more than the threshold has been folded", () => {
      const policy = afterEvents(10)
      expect(policy({ eventsApplied: 11, sourcingTimeMs: 5 })).toBe(true)
      expect(policy({ eventsApplied: 50, sourcingTimeMs: 5 })).toBe(true)
    })

    it("is not due at or below the threshold", () => {
      const policy = afterEvents(10)
      expect(policy({ eventsApplied: 10, sourcingTimeMs: 5 })).toBe(false)
      expect(policy({ eventsApplied: 9, sourcingTimeMs: 5 })).toBe(false)
    })
  })

  describe("whenSourcingTimeExceeds", () => {
    it("is due at the threshold", () => {
      const policy = whenSourcingTimeExceeds(100)
      expect(policy({ eventsApplied: 5, sourcingTimeMs: 100 })).toBe(true)
    })

    it("is not due below the threshold", () => {
      const policy = whenSourcingTimeExceeds(100)
      expect(policy({ eventsApplied: 5, sourcingTimeMs: 99 })).toBe(false)
    })
  })

  it("two policies combine as predicates", () => {
    const events = afterEvents(100)
    const time = whenSourcingTimeExceeds(50)
    const policy = (r: Parameters<typeof events>[0]) => events(r) || time(r)

    expect(policy({ eventsApplied: 101, sourcingTimeMs: 5 })).toBe(true)
    expect(policy({ eventsApplied: 5, sourcingTimeMs: 50 })).toBe(true)
    expect(policy({ eventsApplied: 5, sourcingTimeMs: 5 })).toBe(false)
  })
})
