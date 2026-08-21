import { describe, expect, it } from "bun:test"
import { afterEvents, whenSourcingTimeExceeds, noSnapshotPolicy } from "../snapshot.js"

describe("SnapshotPolicy", () => {
  describe("afterEvents", () => {
    it("triggers when event count exceeds threshold", () => {
      const policy = afterEvents(10)
      expect(policy.shouldSnapshot({ eventsApplied: 11, sourcingTimeMs: 5 })).toBe(true)
      expect(policy.shouldSnapshot({ eventsApplied: 50, sourcingTimeMs: 5 })).toBe(true)
    })

    it("does not trigger at or below threshold", () => {
      const policy = afterEvents(10)
      expect(policy.shouldSnapshot({ eventsApplied: 10, sourcingTimeMs: 5 })).toBe(false)
      expect(policy.shouldSnapshot({ eventsApplied: 9, sourcingTimeMs: 5 })).toBe(false)
    })
  })

  describe("whenSourcingTimeExceeds", () => {
    it("triggers when sourcing time meets threshold", () => {
      const policy = whenSourcingTimeExceeds(100)
      expect(policy.shouldSnapshot({ eventsApplied: 5, sourcingTimeMs: 100 })).toBe(true)
    })

    it("does not trigger below threshold", () => {
      const policy = whenSourcingTimeExceeds(100)
      expect(policy.shouldSnapshot({ eventsApplied: 5, sourcingTimeMs: 99 })).toBe(false)
    })
  })

  describe("noSnapshotPolicy", () => {
    it("never triggers", () => {
      const policy = noSnapshotPolicy()
      expect(policy.shouldSnapshot({ eventsApplied: 10000, sourcingTimeMs: 99999 })).toBe(false)
    })
  })

  describe("or composition", () => {
    it("triggers when either policy triggers", () => {
      const policy = afterEvents(100).or(whenSourcingTimeExceeds(50))

      // First triggers (event count exceeds 100)
      expect(policy.shouldSnapshot({ eventsApplied: 101, sourcingTimeMs: 5 })).toBe(true)
      // Second triggers (sourcing time)
      expect(policy.shouldSnapshot({ eventsApplied: 5, sourcingTimeMs: 50 })).toBe(true)
      // Neither triggers
      expect(policy.shouldSnapshot({ eventsApplied: 5, sourcingTimeMs: 5 })).toBe(false)
    })

    it("chains multiple or() calls", () => {
      const policy = afterEvents(100)
        .or(whenSourcingTimeExceeds(50))
        .or(afterEvents(10))

      // Third policy triggers (exceeds 10)
      expect(policy.shouldSnapshot({ eventsApplied: 11, sourcingTimeMs: 5 })).toBe(true)
    })
  })
})
