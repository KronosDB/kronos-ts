import { describe, expect, it, mock, spyOn } from "bun:test"
import { withRetry, healthCheck, type ResilienceConfig } from "../resilience.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Fast resilience config — keeps real timers under millisecond budget so the
 * suite runs instantly under bun:test (which lacks vitest-style fake timers).
 * Backoff math invariants are independent of magnitude — testing at 1ms base
 * with 50ms cap exercises every code path the production 100ms/30s config does.
 */
const FAST: Partial<ResilienceConfig> = {
  initialDelayMs: 1,
  maxDelayMs: 50,
  multiplier: 2,
}

function deferredRandom(values: number[]): () => number {
  let i = 0
  return () => {
    const v = values[i % values.length]!
    i++
    return v
  }
}

/** Build a function that fails the first N invocations, then succeeds. */
function failsThenSucceeds<T>(failCount: number, value: T): () => Promise<T> {
  let calls = 0
  return async () => {
    calls++
    if (calls <= failCount) throw new Error(`transient ${calls}`)
    return value
  }
}

// ---------------------------------------------------------------------------
// withRetry
// ---------------------------------------------------------------------------

describe("withRetry", () => {
  it("Test 1: success on first attempt invokes fn exactly once", async () => {
    const fn = mock(async () => "ok")
    const result = await withRetry(fn, { event: "per-operation", ...FAST })
    expect(result).toBe("ok")
    expect(fn).toHaveBeenCalledTimes(1)
  })

  it("Test 2: success after N retries (exponential delays observed)", async () => {
    const observed: number[] = []
    const realSetTimeout = globalThis.setTimeout
    const stSpy = spyOn(globalThis, "setTimeout").mockImplementation(((
      cb: (...args: any[]) => void,
      ms?: number,
    ) => {
      observed.push(ms ?? 0)
      return realSetTimeout(cb, 0)
    }) as any)
    try {
      // Random=1 => delay = 1.0 * exponential cap (deterministic).
      const rndSpy = spyOn(Math, "random").mockImplementation(() => 1)
      try {
        const fn = failsThenSucceeds(2, 42)
        const result = await withRetry(fn, { event: "per-operation", ...FAST })
        expect(result).toBe(42)
        // Two retries => two timer scheduling calls; growth: base*2^0, base*2^1.
        // initialDelayMs=1, multiplier=2 => observed[0]≈1, observed[1]≈2.
        expect(observed.length).toBe(2)
        expect(observed[0]).toBeGreaterThanOrEqual(0)
        expect(observed[1]).toBeGreaterThanOrEqual(observed[0]!)
      } finally {
        rndSpy.mockRestore()
      }
    } finally {
      stSpy.mockRestore()
    }
  })

  it("Test 3: full-jitter range [0, base * mult^attempt]", async () => {
    const observed: number[] = []
    const realSetTimeout = globalThis.setTimeout
    const stSpy = spyOn(globalThis, "setTimeout").mockImplementation(((
      cb: (...args: any[]) => void,
      ms?: number,
    ) => {
      observed.push(ms ?? 0)
      return realSetTimeout(cb, 0)
    }) as any)
    try {
      // random=0 => delay 0 (lower bound). random=1 => upper bound.
      const rndSpy = spyOn(Math, "random").mockImplementation(deferredRandom([0, 1]))
      try {
        const fn = failsThenSucceeds(2, "ok")
        await withRetry(fn, { event: "per-operation", ...FAST })
        expect(observed[0]).toBe(0) // attempt 0 with random=0
        // attempt 1 with random=1: delay = min(1*2^1, 50) = 2
        expect(observed[1]).toBe(2)
      } finally {
        rndSpy.mockRestore()
      }
    } finally {
      stSpy.mockRestore()
    }
  })

  it("Test 4: maxDelayMs cap enforced once exponential exceeds it", async () => {
    const observed: number[] = []
    const realSetTimeout = globalThis.setTimeout
    const stSpy = spyOn(globalThis, "setTimeout").mockImplementation(((
      cb: (...args: any[]) => void,
      ms?: number,
    ) => {
      observed.push(ms ?? 0)
      return realSetTimeout(cb, 0)
    }) as any)
    try {
      const rndSpy = spyOn(Math, "random").mockImplementation(() => 1)
      try {
        // initialDelayMs=10, multiplier=2, maxDelayMs=15 => sequence: 10, 15(capped), 15, ...
        let calls = 0
        const fn = async () => {
          calls++
          if (calls <= 4) throw new Error("nope")
          return "ok"
        }
        await withRetry(fn, {
          event: "per-operation",
          initialDelayMs: 10,
          multiplier: 2,
          maxDelayMs: 15,
        })
        expect(observed[0]).toBe(10)
        expect(observed[1]).toBe(15)
        expect(observed[2]).toBe(15)
        expect(observed[3]).toBe(15)
      } finally {
        rndSpy.mockRestore()
      }
    } finally {
      stSpy.mockRestore()
    }
  })

  it("Test 5: maxAttempts exhausted rethrows the last error", async () => {
    let calls = 0
    const fn = async () => {
      calls++
      throw new Error(`fail-${calls}`)
    }
    const rndSpy = spyOn(Math, "random").mockImplementation(() => 0)
    try {
      await expect(
        withRetry(fn, { event: "per-operation", ...FAST, maxAttempts: 3 }),
      ).rejects.toThrow("fail-3")
      expect(calls).toBe(3)
    } finally {
      rndSpy.mockRestore()
    }
  })

  it("Test 6: isRetryable=false short-circuits (no retry, immediate throw)", async () => {
    let calls = 0
    const fn = async () => {
      calls++
      throw new Error("terminal")
    }
    await expect(
      withRetry(fn, {
        event: "per-operation",
        ...FAST,
        isRetryable: () => false,
      }),
    ).rejects.toThrow("terminal")
    expect(calls).toBe(1)
  })

  it("Test 7: per-event default attempt cap (initial-connect=10, reconnect=30)", async () => {
    const rndSpy = spyOn(Math, "random").mockImplementation(() => 0)
    try {
      let initialConnectCalls = 0
      const initial = async () => {
        initialConnectCalls++
        throw new Error("nope")
      }
      await expect(
        withRetry(initial, { event: "initial-connect", ...FAST }),
      ).rejects.toThrow()
      expect(initialConnectCalls).toBe(10)

      let reconnectCalls = 0
      const reconnect = async () => {
        reconnectCalls++
        throw new Error("nope")
      }
      await expect(
        withRetry(reconnect, { event: "reconnect", ...FAST }),
      ).rejects.toThrow()
      expect(reconnectCalls).toBe(30)
    } finally {
      rndSpy.mockRestore()
    }
  })

  it("Test 8: log fires on retry, NOT on success or terminal short-circuit", async () => {
    const logCalls: string[] = []
    const log = (m: string) => logCalls.push(m)
    const rndSpy = spyOn(Math, "random").mockImplementation(() => 0)
    try {
      // 8a: success on first attempt — no log.
      await withRetry(async () => "ok", { event: "per-operation", ...FAST, log })
      expect(logCalls.length).toBe(0)

      // 8b: success after 2 fails — 2 log calls.
      logCalls.length = 0
      await withRetry(failsThenSucceeds(2, "ok"), {
        event: "per-operation",
        ...FAST,
        log,
      })
      expect(logCalls.length).toBe(2)
      expect(logCalls[0]).toContain("per-operation attempt 1/")
      expect(logCalls[1]).toContain("per-operation attempt 2/")

      // 8c: terminal error — no log (we short-circuit before scheduling next).
      logCalls.length = 0
      await expect(
        withRetry(
          async () => {
            throw new Error("boom")
          },
          {
            event: "per-operation",
            ...FAST,
            log,
            isRetryable: () => false,
          },
        ),
      ).rejects.toThrow()
      expect(logCalls.length).toBe(0)
    } finally {
      rndSpy.mockRestore()
    }
  })
})

// ---------------------------------------------------------------------------
// healthCheck
// ---------------------------------------------------------------------------

describe("healthCheck", () => {
  it("Test 9: success resolves without invoking log", async () => {
    const logCalls: string[] = []
    await healthCheck(async () => undefined, {
      thresholdMs: 50,
      log: (m) => logCalls.push(m),
    })
    expect(logCalls.length).toBe(0)
  })

  it("Test 10: warn-then-continue when probe never resolves", async () => {
    const logCalls: string[] = []
    // fn never resolves — threshold path drives the resolution.
    await healthCheck(() => new Promise<void>(() => {}), {
      thresholdMs: 5,
      log: (m) => logCalls.push(m),
    })
    expect(logCalls.length).toBe(1)
    expect(logCalls[0]).toContain("soft-failed")
  })

  it("Test 10b: probe rejection is also warn-then-continue", async () => {
    const logCalls: string[] = []
    await healthCheck(
      async () => {
        throw new Error("upstream-down")
      },
      {
        thresholdMs: 50,
        log: (m) => logCalls.push(m),
      },
    )
    expect(logCalls.length).toBe(1)
    expect(logCalls[0]).toContain("upstream-down")
  })
})
