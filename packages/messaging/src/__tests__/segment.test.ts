import { describe, expect, it } from "bun:test"
import {
  ROOT_SEGMENT,
  segment,
  segmentMatches,
  splitSegment,
  mergeSegments,
  isMergeable,
  segmentCount,
  hashOf,
  segments,
} from "../segment.js"

describe("Segment", () => {
  describe("ROOT_SEGMENT", () => {
    it("matches all hashes", () => {
      expect(segmentMatches(ROOT_SEGMENT, 0)).toBe(true)
      expect(segmentMatches(ROOT_SEGMENT, 42)).toBe(true)
      expect(segmentMatches(ROOT_SEGMENT, 999999)).toBe(true)
    })

    it("has count 1", () => {
      expect(segmentCount(ROOT_SEGMENT)).toBe(1)
    })
  })

  describe("splitSegment", () => {
    it("splits root into two segments", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)

      expect(a.segmentId).toBe(0)
      expect(a.mask).toBe(1)
      expect(b.segmentId).toBe(1)
      expect(b.mask).toBe(1)
    })

    it("split segments together cover all hashes", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)

      let aCount = 0
      let bCount = 0
      for (let i = 0; i < 100; i++) {
        if (segmentMatches(a, i)) aCount++
        if (segmentMatches(b, i)) bCount++
      }

      // Every hash matches exactly one segment
      expect(aCount + bCount).toBe(100)
      expect(aCount).toBeGreaterThan(0)
      expect(bCount).toBeGreaterThan(0)
    })

    it("can split further", () => {
      const [a, _b] = splitSegment(ROOT_SEGMENT)
      const [a1, a2] = splitSegment(a)

      expect(a1.mask).toBe(3) // 0b11
      expect(a2.mask).toBe(3)
      expect(a1.segmentId).not.toBe(a2.segmentId)
    })
  })

  describe("mergeSegments", () => {
    it("merges siblings back to parent", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)
      const merged = mergeSegments(a, b)

      expect(merged.segmentId).toBe(ROOT_SEGMENT.segmentId)
      expect(merged.mask).toBe(ROOT_SEGMENT.mask)
    })

    it("merges in either order", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)

      const merged1 = mergeSegments(a, b)
      const merged2 = mergeSegments(b, a)

      expect(merged1.segmentId).toBe(merged2.segmentId)
      expect(merged1.mask).toBe(merged2.mask)
    })

    it("throws for non-sibling segments", () => {
      const [a, _b] = splitSegment(ROOT_SEGMENT)
      const [a1, _a2] = splitSegment(a)

      // a1 and _b are not siblings
      expect(() => mergeSegments(a1, _b)).toThrow("not mergeable")
    })
  })

  describe("isMergeable", () => {
    it("returns true for siblings", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)
      expect(isMergeable(a, b)).toBe(true)
    })

    it("returns false for root segment with itself", () => {
      expect(isMergeable(ROOT_SEGMENT, ROOT_SEGMENT)).toBe(false)
    })

    it("returns false for non-siblings", () => {
      const [a, b] = splitSegment(ROOT_SEGMENT)
      const [a1, _a2] = splitSegment(a)
      expect(isMergeable(a1, b)).toBe(false)
    })
  })

  describe("segments", () => {
    it("creates 1 segment (root)", () => {
      const segs = segments(1)
      expect(segs).toHaveLength(1)
      expect(segs[0]).toEqual(ROOT_SEGMENT)
    })

    it("creates 2 segments", () => {
      const segs = segments(2)
      expect(segs).toHaveLength(2)
      expect(segs[0]!.mask).toBe(1)
      expect(segs[1]!.mask).toBe(1)
    })

    it("creates 4 segments", () => {
      const segs = segments(4)
      expect(segs).toHaveLength(4)

      // All should have the same mask
      const mask = segs[0]!.mask
      for (const seg of segs) {
        expect(seg.mask).toBe(mask)
      }

      // Together they cover all hashes
      for (let i = 0; i < 100; i++) {
        const matching = segs.filter((s) => segmentMatches(s, i))
        expect(matching).toHaveLength(1)
      }
    })

    it("rounds up to nearest power of 2", () => {
      const segs = segments(3)
      expect(segs).toHaveLength(4) // Rounded up
    })
  })

  describe("hashOf", () => {
    it("produces consistent hashes", () => {
      expect(hashOf("test")).toBe(hashOf("test"))
    })

    it("produces different hashes for different inputs", () => {
      expect(hashOf("abc")).not.toBe(hashOf("xyz"))
    })

    it("distributes across segments", () => {
      const segs = segments(4)
      const counts = new Map<number, number>()

      for (let i = 0; i < 1000; i++) {
        const hash = hashOf(`key-${i}`)
        for (const seg of segs) {
          if (segmentMatches(seg, hash)) {
            counts.set(seg.segmentId, (counts.get(seg.segmentId) ?? 0) + 1)
          }
        }
      }

      // Each segment should get some events (rough distribution)
      for (const seg of segs) {
        expect(counts.get(seg.segmentId) ?? 0).toBeGreaterThan(100)
      }
    })
  })
})
