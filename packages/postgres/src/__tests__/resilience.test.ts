import { describe, expect, it, spyOn } from "bun:test"
import type { PostgresAdapter } from "../adapter.js"
import { postgresPool } from "../postgres-pool.js"
import { withRetry } from "../resilience.js"

/** Run `fn` with `console.warn` captured, and return what it was called with. */
async function capturingWarn(fn: () => Promise<unknown>): Promise<string[]> {
  const lines: string[] = []
  const spy = spyOn(console, "warn").mockImplementation((...args: unknown[]) => {
    lines.push(args.map(String).join(" "))
  })
  try {
    await fn()
  } finally {
    spy.mockRestore()
  }
  return lines
}

function failsOnce<T>(value: T): () => Promise<T> {
  let calls = 0
  return async () => {
    if (++calls === 1) throw Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" })
    return value
  }
}

describe("withRetry", () => {
  it("writes each retry to console.warn when no log is given", async () => {
    const lines = await capturingWarn(() =>
      withRetry(failsOnce("ok"), { event: "initial-connect", initialDelayMs: 1 }),
    )
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain("[kronos:resilience] initial-connect attempt 1/10 failed")
    expect(lines[0]).toContain("connection refused")
  })

  it("writes to the given log instead of console.warn", async () => {
    const given: string[] = []
    const lines = await capturingWarn(() =>
      withRetry(failsOnce("ok"), { event: "initial-connect", initialDelayMs: 1, log: (m) => given.push(m) }),
    )
    expect(lines).toHaveLength(0)
    expect(given).toHaveLength(1)
  })

  it("writes nothing when the first attempt succeeds", async () => {
    const lines = await capturingWarn(() => withRetry(async () => "ok", { event: "initial-connect" }))
    expect(lines).toHaveLength(0)
  })
})

describe("postgresPool start()", () => {
  it("warns about a failed connect it retries when no resilience.log is given", async () => {
    const connect = failsOnce(undefined)
    const adapter = { connect, disconnect: async () => {} } as unknown as PostgresAdapter
    const pool = postgresPool(adapter, { bootstrap: false, resilience: { initialDelayMs: 1 } })
    const lines = await capturingWarn(() => pool.start())
    expect(lines).toHaveLength(1)
    expect(lines[0]).toContain("initial-connect attempt 1/10 failed")
  })
})
