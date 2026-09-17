/**
 * `consoleLogger(options)` — one JSON line per record, and the two edges that
 * matter: a level filter cheap enough to cost nothing below threshold, and a
 * reserved-key collision that must never let a field clobber the record's own
 * `time`/`level`/`message`.
 */
import { describe, expect, it } from "bun:test"
import { consoleLogger } from "../console-logger.js"

function captured(): { lines: string[]; write: (line: string) => void } {
  const lines: string[] = []
  return { lines, write: (line) => lines.push(line) }
}

describe("consoleLogger", () => {
  it("writes one JSON object per record with the reserved keys at top level", () => {
    const { lines, write } = captured()
    consoleLogger({ write }).info("widget edited", { widgetId: "w-1", count: 3 })

    expect(lines).toHaveLength(1)
    const record = JSON.parse(lines[0]!)
    expect(record.level).toBe("info")
    expect(record.message).toBe("widget edited")
    expect(record.widgetId).toBe("w-1")
    expect(record.count).toBe(3)
    expect(typeof record.time).toBe("string")
    expect(new Date(record.time).toISOString()).toBe(record.time)
  })

  it("skips records below the configured level without writing anything", () => {
    const { lines, write } = captured()
    const log = consoleLogger({ level: "warn", write })

    log.debug("noise")
    log.info("also noise")
    log.warn("this one")

    expect(lines).toHaveLength(1)
    expect(JSON.parse(lines[0]!).message).toBe("this one")
  })

  it("nests a field that collides with a reserved key instead of overwriting it", () => {
    const { lines, write } = captured()
    consoleLogger({ write }).info("hello", { level: "not-a-real-level", time: 12345 })

    const record = JSON.parse(lines[0]!)
    expect(record.level).toBe("info")
    expect(typeof record.time).toBe("string")
    expect(record.fields).toEqual({ level: "not-a-real-level", time: 12345 })
  })

  it("with() merges fields into every subsequent record, later calls winning", () => {
    const { lines, write } = captured()
    const log = consoleLogger({ write }).with({ requestId: "r-1", tenant: "a" })
    const scoped = log.with({ tenant: "b" })

    scoped.info("scoped record")
    log.info("outer record")

    const [scopedRecord, outerRecord] = lines.map((line) => JSON.parse(line))
    expect(scopedRecord.requestId).toBe("r-1")
    expect(scopedRecord.tenant).toBe("b")
    expect(outerRecord.tenant).toBe("a")
  })

  it("defaults to console.log for debug/info and console.error for warn/error", () => {
    const logCalls: unknown[] = []
    const errorCalls: unknown[] = []
    const originalLog = console.log
    const originalError = console.error
    console.log = (line: unknown) => logCalls.push(line)
    console.error = (line: unknown) => errorCalls.push(line)
    try {
      const log = consoleLogger()
      log.info("info line")
      log.warn("warn line")
    } finally {
      console.log = originalLog
      console.error = originalError
    }

    expect(logCalls).toHaveLength(1)
    expect(errorCalls).toHaveLength(1)
  })
})
