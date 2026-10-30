import { describe, expect, it } from "bun:test"
import { gapAwareToken, globalSequenceToken, replayToken, isGapAwareToken, isReplayToken, advanceTokenTo, serializeToken, deserializeToken, type ReplayToken } from "../tracking-token.js"

describe("gapAwareToken", () => {
  it("exposes the sequence as its position and carries the gapKey", () => {
    const t = gapAwareToken(42n, "1234")
    expect(t.kind).toBe("gap-aware")
    expect(t.position()).toBe(42n)
    expect(t.gapKey).toBe("1234")
    expect(isGapAwareToken(t)).toBe(true)
    expect(isGapAwareToken(globalSequenceToken(42n))).toBe(false)
  })

  it("covers/samePositionAs compare by sequence position", () => {
    const t = gapAwareToken(10n, "x")
    expect(t.covers(globalSequenceToken(10n))).toBe(true)
    expect(t.covers(globalSequenceToken(11n))).toBe(false)
    expect(t.samePositionAs(globalSequenceToken(10n))).toBe(true)
  })
})

describe("serializeToken / deserializeToken", () => {
  it("round-trips a GlobalSequenceToken (no gapKey)", () => {
    const s = serializeToken(globalSequenceToken(7n))
    expect(s.data).not.toContain("gapKey")
    // Label matches the historical value so existing token rows are unchanged.
    expect(s.type).toBe("GlobalSequenceToken")
    const back = deserializeToken(s.type, s.data)
    expect(back!.kind).toBe("global-sequence")
    expect(back!.position()).toBe(7n)
  })

  it("round-trips a GapAwareToken preserving the gapKey", () => {
    const s = serializeToken(gapAwareToken(7n, "98765"))
    // PascalCase label, consistent with GlobalSequenceToken in the same column.
    expect(s.type).toBe("GapAwareToken")
    const back = deserializeToken(s.type, s.data)
    expect(isGapAwareToken(back!)).toBe(true)
    expect(back!.position()).toBe(7n)
    expect((back as { gapKey: string }).gapKey).toBe("98765")
  })

  it("round-trips a ReplayToken: the replay, where it ends, and the inner gapKey", () => {
    const replay = replayToken(gapAwareToken(100n, "999"), gapAwareToken(25n, "555"))
    const s = serializeToken(replay)
    expect(s.type).toBe("ReplayToken")
    const back = deserializeToken(s.type, s.data)
    expect(isReplayToken(back!)).toBe(true)
    const { currentToken, tokenAtReset } = back as ReplayToken
    expect(currentToken.position()).toBe(25n)
    expect((currentToken as { gapKey: string }).gapKey).toBe("555")
    expect(tokenAtReset.position()).toBe(100n)
    expect((tokenAtReset as { gapKey: string }).gapKey).toBe("999")
  })

  it("reads a replay that ends at a plain position", () => {
    const s = serializeToken(replayToken(globalSequenceToken(100n), globalSequenceToken(10n)))
    const back = deserializeToken(s.type, s.data) as ReplayToken
    expect(back.kind).toBe("replay")
    expect(back.tokenAtReset.kind).toBe("global-sequence")
    expect(back.position()).toBe(10n)
  })

  it("returns undefined for an absent token", () => {
    expect(deserializeToken("global-sequence", null)).toBeUndefined()
    expect(deserializeToken(null, null)).toBeUndefined()
  })
})

describe("advanceTokenTo", () => {
  it("returns the next token verbatim when not replaying", () => {
    const next = gapAwareToken(5n, "x")
    expect(advanceTokenTo(globalSequenceToken(2n), next)).toBe(next)
  })

  it("keeps the replay wrapper while the next token is within the replay range", () => {
    const replay = replayToken(globalSequenceToken(100n), globalSequenceToken(10n))
    const advanced = advanceTokenTo(replay, gapAwareToken(20n, "x"))
    expect(advanced.kind).toBe("replay")
    expect(advanced.position()).toBe(20n)
  })

  it("unwraps the replay wrapper once the next token covers the reset point", () => {
    const replay = replayToken(globalSequenceToken(100n), globalSequenceToken(10n))
    const advanced = advanceTokenTo(replay, gapAwareToken(100n, "x"))
    expect(isGapAwareToken(advanced)).toBe(true)
    expect(advanced.position()).toBe(100n)
  })
})
