import { describe, expect, it } from "bun:test"
import {
  backendOrder,
  deterministicUuid,
  makePayload,
  parseArgs,
} from "../benchmarks/kronosdb-postgres/config.js"
import {
  buildComparisons,
  multiplierLabel,
  percentile,
  stringifyDocument,
  summarize,
  type ScenarioSample,
} from "../benchmarks/kronosdb-postgres/report.js"

describe("KronosDB/Postgres benchmark support", () => {
  it("uses nearest-rank percentiles and stable summaries", () => {
    expect(percentile([10, 1, 7, 3], 50)).toBe(3)
    expect(percentile([10, 1, 7, 3], 99)).toBe(10)
    const result = summarize([1, 2, 3])
    expect(result.median).toBe(2)
    expect(result.mean).toBe(2)
    expect(result.values).toEqual([1, 2, 3])
  })

  it("calculates throughput and latency multipliers in the useful direction", () => {
    const sample = (
      backend: "kronosdb" | "postgres",
      metric: string,
      value: number,
      better: "higher" | "lower",
    ): ScenarioSample => ({
      backend,
      scenario: "append",
      parameters: { batchSize: 1 },
      sample: 0,
      metrics: [{ name: metric, value, unit: better === "higher" ? "events/s" : "ms", better }],
    })
    const comparisons = buildComparisons([
      sample("kronosdb", "eventsPerSec", 200, "higher"),
      sample("postgres", "eventsPerSec", 100, "higher"),
      sample("kronosdb", "latencyP50Ms", 5, "lower"),
      sample("postgres", "latencyP50Ms", 10, "lower"),
    ])
    expect(comparisons.find((entry) => entry.metric === "eventsPerSec")?.multiplier).toBe(2)
    expect(comparisons.find((entry) => entry.metric === "latencyP50Ms")?.multiplier).toBe(2)
    expect(multiplierLabel(0.75, "higher")).toContain("lower throughput")
    expect(multiplierLabel(0.75, "lower")).toContain("higher latency")
  })

  it("generates deterministic identifiers and payloads", () => {
    const first = deterministicUuid("seed")
    expect(first).toBe(deterministicUuid("seed"))
    expect(first).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-a[0-9a-f]{3}-[0-9a-f]{12}$/)
    expect(makePayload("aggregate", 7, "seed")).toEqual(makePayload("aggregate", 7, "seed"))
  })

  it("parses profiles, resource limits, and backend order", () => {
    const parsed = parseArgs(["--profile", "full", "--memory", "2", "--samples", "3", "--scenario", "append", "--postgres-adapter", "bun-sql"])
    expect(parsed.profileName).toBe("full")
    expect(parsed.postgresAdapter).toBe("bun-sql")
    expect(parsed.memoryBytes).toBe(2 * 1024 ** 3)
    expect(parsed.samples).toBe(3)
    expect(parsed.scenario).toBe("append")
    expect(backendOrder(0, "alternating")).toEqual(["kronosdb", "postgres"])
    expect(backendOrder(1, "alternating")).toEqual(["postgres", "kronosdb"])
    expect(() => parseArgs(["--scenario", "unknown"])).toThrow("unknown scenario")
  })

  it("serializes bigint diagnostics without losing precision", () => {
    const encoded = stringifyDocument({ diagnostics: { position: 9_007_199_254_740_993n } } as never)
    expect(JSON.parse(encoded).diagnostics.position).toBe("9007199254740993")
  })
})
