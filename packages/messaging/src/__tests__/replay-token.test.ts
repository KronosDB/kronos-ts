import { describe, expect, it } from "bun:test"
import { emptyMetadata } from "@kronos-ts/common"
import {
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isGlobalSequenceToken,
  isReplaying,
  advanceToken,
  unwrapToken,
  wasProcessedBeforeReset,
  type TrackingToken,
  type ReplayToken,
} from "../tracking-token.js"
import { isReplay, REPLAY_STATE_KEY } from "../replay-token.js"
import { createProcessingContext } from "../default-processing-context.js"

describe("TrackingToken", () => {
  describe("GlobalSequenceToken", () => {
    it("returns position", () => {
      const token = globalSequenceToken(42n)
      expect(token.position()).toBe(42n)
    })

    it("covers tokens at same or earlier position", () => {
      const token = globalSequenceToken(10n)
      expect(token.covers(globalSequenceToken(5n))).toBe(true)
      expect(token.covers(globalSequenceToken(10n))).toBe(true)
      expect(token.covers(globalSequenceToken(11n))).toBe(false)
    })

    it("lowerBound returns the earlier position", () => {
      const a = globalSequenceToken(10n)
      const b = globalSequenceToken(20n)

      expect(a.lowerBound(b).position()).toBe(10n)
      expect(b.lowerBound(a).position()).toBe(10n)
    })

    it("upperBound returns the later position", () => {
      const a = globalSequenceToken(10n)
      const b = globalSequenceToken(20n)

      expect(a.upperBound(b).position()).toBe(20n)
      expect(b.upperBound(a).position()).toBe(20n)
    })

    it("has kind global-sequence", () => {
      expect(isGlobalSequenceToken(globalSequenceToken(0n))).toBe(true)
    })
  })

  describe("ReplayToken", () => {
    it("wraps current and reset tokens", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )

      expect(isReplayToken(token)).toBe(true)
      expect(token.position()).toBe(0n)
      expect(token.tokenAtReset.position()).toBe(100n)
    })

    it("preserves reset context", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
        "schema migration",
      )

      expect(token.resetContext).toBe("schema migration")
    })

    it("covers based on current token", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(50n),
      )

      expect(token.covers(globalSequenceToken(50n))).toBe(true)
      expect(token.covers(globalSequenceToken(51n))).toBe(false)
    })

    it("is identified as replay token", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )
      expect(isReplayToken(token)).toBe(true)
      expect(isGlobalSequenceToken(token)).toBe(false)
    })
  })

  describe("advanceToken", () => {
    it("advances a GlobalSequenceToken", () => {
      const token = globalSequenceToken(10n)
      const advanced = advanceToken(token, 20n)

      expect(advanced.position()).toBe(20n)
      expect(isGlobalSequenceToken(advanced)).toBe(true)
    })

    it("keeps replay wrapper while position < resetPosition", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )
      const advanced = advanceToken(token, 50n)

      expect(isReplayToken(advanced)).toBe(true)
      expect(advanced.position()).toBe(50n)
    })

    it("unwraps replay when position >= resetPosition", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )
      const advanced = advanceToken(token, 100n)

      expect(isReplayToken(advanced)).toBe(false)
      expect(advanced.position()).toBe(100n)
    })

    it("unwraps replay when position > resetPosition", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )
      const advanced = advanceToken(token, 150n)

      expect(isReplayToken(advanced)).toBe(false)
      expect(advanced.position()).toBe(150n)
    })
  })

  describe("isReplaying", () => {
    it("returns true for replay tokens", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(0n),
      )
      expect(isReplaying(token)).toBe(true)
    })

    it("returns false for non-replay tokens", () => {
      expect(isReplaying(globalSequenceToken(42n))).toBe(false)
    })
  })

  describe("unwrapToken", () => {
    it("returns non-replay tokens unchanged", () => {
      const token = globalSequenceToken(42n)
      expect(unwrapToken(token)).toBe(token)
    })

    it("unwraps replay to inner token", () => {
      const inner = globalSequenceToken(50n)
      const token = replayToken(globalSequenceToken(100n), inner)
      const unwrapped = unwrapToken(token)

      expect(isReplayToken(unwrapped)).toBe(false)
      expect(unwrapped.position()).toBe(50n)
    })
  })

  describe("wasProcessedBeforeReset", () => {
    it("returns true for events before reset position", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(50n),
      ) as ReplayToken

      expect(wasProcessedBeforeReset(token, 99n)).toBe(true)
    })

    it("returns false for events at or after reset position", () => {
      const token = replayToken(
        globalSequenceToken(100n),
        globalSequenceToken(50n),
      ) as ReplayToken

      expect(wasProcessedBeforeReset(token, 100n)).toBe(true) // exactly at reset
      expect(wasProcessedBeforeReset(token, 101n)).toBe(false) // after reset
    })
  })

  describe("isReplay (ProcessingContext helper)", () => {
    it("returns false when no replay state in context", () => {
      const ctx = createProcessingContext(emptyMetadata())
      expect(isReplay(ctx)).toBe(false)
    })

    it("returns true when replay state is set", () => {
      const ctx = createProcessingContext(emptyMetadata())
      ctx.set(REPLAY_STATE_KEY, { replaying: true })
      expect(isReplay(ctx)).toBe(true)
    })

    it("returns false when replay state indicates not replaying", () => {
      const ctx = createProcessingContext(emptyMetadata())
      ctx.set(REPLAY_STATE_KEY, { replaying: false })
      expect(isReplay(ctx)).toBe(false)
    })
  })

  describe("extensibility", () => {
    it("custom token types work with standard operations", () => {
      // Simulate a hypothetical PostgreSQL token with commit timestamp
      const pgToken: TrackingToken = {
        kind: "postgres-commit",
        position: () => 42n,
        covers: (other) => 42n >= other.position(),
        lowerBound: (other) => 42n < other.position() ? pgToken : other,
        upperBound: (other) => 42n > other.position() ? pgToken : other,
      }

      // Can be used with replay
      const replay = replayToken(globalSequenceToken(100n), pgToken)
      expect(replay.position()).toBe(42n)
      expect(isReplaying(replay)).toBe(true)

      // Can be advanced
      const advanced = advanceToken(replay, 100n)
      expect(isReplaying(advanced)).toBe(false)
    })
  })
})
