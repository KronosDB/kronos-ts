import { describe, expect, it } from "bun:test"
import { boundedRead } from "../bounded-read.js"

/** A read that only ever settles by being cancelled — the lost-`end` hang. */
function hangs(signal: AbortSignal): Promise<never> {
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(new Error("cancelled")), {
      once: true,
    })
  })
}

describe("boundedRead", () => {
  it("answers from the first attempt and never cancels it", async () => {
    const signals: AbortSignal[] = []
    const result = await boundedRead(50, async (signal) => {
      signals.push(signal)
      return "answer"
    })
    expect(result).toBe("answer")
    expect(signals.length).toBe(1)
    expect(signals[0]?.aborted).toBe(false)
  })

  it("cancels a read that never completes and asks once more", async () => {
    let attempts = 0
    const result = await boundedRead(20, async (signal) => {
      attempts++
      if (attempts === 1) return hangs(signal)
      return "answer"
    })
    expect(result).toBe("answer")
    expect(attempts).toBe(2)
  })

  it("does not retry a read that fails on its own", async () => {
    let attempts = 0
    await expect(
      boundedRead(50, async () => {
        attempts++
        throw new Error("Unknown Context default")
      }),
    ).rejects.toThrow("Unknown Context default")
    expect(attempts).toBe(1)
  })

  it("reports a read lost twice, with the deadline in the message", async () => {
    let attempts = 0
    const started = Date.now()
    await expect(
      boundedRead(20, async (signal) => {
        attempts++
        return hangs(signal)
      }),
    ).rejects.toThrow("did not complete within 20 ms, twice")
    expect(attempts).toBe(2)
    expect(Date.now() - started).toBeGreaterThanOrEqual(35)
  })
})
