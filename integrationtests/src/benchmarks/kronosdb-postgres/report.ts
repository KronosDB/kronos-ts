import type { BackendMetadata, HostMetadata } from "./backends.js"
import type { BackendName, BenchmarkOptions, ScenarioName } from "./config.js"

export type BetterDirection = "higher" | "lower"

export interface MetricValue {
  readonly name: string
  readonly value: number
  readonly unit: string
  readonly better: BetterDirection
}

export interface ScenarioSample {
  readonly backend: BackendName
  readonly scenario: ScenarioName
  readonly parameters: Readonly<Record<string, string | number>>
  readonly sample: number
  readonly metrics: readonly MetricValue[]
  readonly diagnostics?: Readonly<Record<string, string | number | boolean>>
}

export interface Summary {
  readonly count: number
  readonly values: readonly number[]
  readonly median: number
  readonly mean: number
  readonly min: number
  readonly max: number
  readonly standardDeviation: number
  readonly coefficientOfVariation: number
}

export interface Comparison {
  readonly scenario: ScenarioName
  readonly parameters: Readonly<Record<string, string | number>>
  readonly metric: string
  readonly unit: string
  readonly better: BetterDirection
  readonly kronosdb: Summary
  readonly postgres: Summary
  readonly multiplier: number
  readonly label: string
}

export interface BenchmarkDocument {
  readonly schemaVersion: 1
  readonly run: {
    readonly startedAt: string
    readonly finishedAt: string
    readonly elapsedMs: number
    readonly profile: string
    readonly seed: string
    readonly order: string
    readonly scenario: string
    readonly postgresAdapter: string
    readonly samples: number
  }
  readonly environment: {
    readonly host: HostMetadata
    readonly backends: readonly BackendMetadata[]
    readonly git: { readonly sha: string; readonly dirty: boolean }
    readonly resourceLimits: { readonly cpu: number; readonly memoryBytes: number }
  }
  readonly results: readonly ScenarioSample[]
  readonly comparisons: readonly Comparison[]
  readonly highlights: readonly string[]
  readonly correctness: { readonly passed: true; readonly checks: number }
}

export function percentile(values: readonly number[], p: number): number {
  if (values.length === 0) return 0
  const sorted = [...values].sort((a, b) => a - b)
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length))
  return sorted[Math.min(sorted.length - 1, rank - 1)]!
}

export function summarize(values: readonly number[]): Summary {
  if (values.length === 0) throw new Error("cannot summarize an empty sample")
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length
  const standardDeviation = Math.sqrt(variance)
  return {
    count: values.length,
    values: [...values],
    median: percentile(values, 50),
    mean,
    min: Math.min(...values),
    max: Math.max(...values),
    standardDeviation,
    coefficientOfVariation: mean === 0 ? 0 : standardDeviation / mean,
  }
}

function parameterKey(parameters: Readonly<Record<string, string | number>>): string {
  return Object.entries(parameters)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${value}`)
    .join(",")
}

export function multiplierLabel(multiplier: number, better: BetterDirection): string {
  const formatted = `${multiplier.toFixed(2)}x`
  if (multiplier >= 1) {
    return `${formatted} — KronosDB ${better === "higher" ? "higher throughput" : "lower latency"}`
  }
  return `${formatted} — KronosDB ${better === "higher" ? "lower throughput" : "higher latency"}`
}

export function buildComparisons(samples: readonly ScenarioSample[]): Comparison[] {
  const groups = new Map<string, {
    scenario: ScenarioName
    parameters: Readonly<Record<string, string | number>>
    metric: string
    unit: string
    better: BetterDirection
    values: Record<BackendName, number[]>
  }>()

  for (const sample of samples) {
    for (const metric of sample.metrics) {
      const key = `${sample.scenario}|${parameterKey(sample.parameters)}|${metric.name}`
      let group = groups.get(key)
      if (!group) {
        group = {
          scenario: sample.scenario,
          parameters: sample.parameters,
          metric: metric.name,
          unit: metric.unit,
          better: metric.better,
          values: { kronosdb: [], postgres: [] },
        }
        groups.set(key, group)
      }
      group.values[sample.backend].push(metric.value)
    }
  }

  const comparisons: Comparison[] = []
  for (const group of groups.values()) {
    if (group.values.kronosdb.length === 0 || group.values.postgres.length === 0) continue
    const kronosdb = summarize(group.values.kronosdb)
    const postgres = summarize(group.values.postgres)
    const multiplier = group.better === "higher"
      ? kronosdb.median / postgres.median
      : postgres.median / kronosdb.median
    comparisons.push({
      scenario: group.scenario,
      parameters: group.parameters,
      metric: group.metric,
      unit: group.unit,
      better: group.better,
      kronosdb,
      postgres,
      multiplier,
      label: multiplierLabel(multiplier, group.better),
    })
  }
  return comparisons.sort((left, right) =>
    `${left.scenario}|${parameterKey(left.parameters)}|${left.metric}`
      .localeCompare(`${right.scenario}|${parameterKey(right.parameters)}|${right.metric}`),
  )
}

export function buildHighlights(comparisons: readonly Comparison[]): string[] {
  const primary = comparisons.filter((comparison) =>
    ["eventsPerSec", "commandsPerSec", "catchupEventsPerSec", "handledEventsPerSec", "latencyP50Ms"].includes(comparison.metric),
  )
  if (primary.length === 0) return []
  const sorted = [...primary].sort((left, right) => right.multiplier - left.multiplier)
  const best = sorted[0]!
  const worst = sorted[sorted.length - 1]!
  const highlights = [
    `Largest KronosDB advantage: ${best.scenario} (${parameterKey(best.parameters)}) ${best.metric}: ${best.label}.`,
    `Smallest KronosDB multiplier: ${worst.scenario} (${parameterKey(worst.parameters)}) ${worst.metric}: ${worst.label}.`,
  ]
  const variable = comparisons.filter((comparison) =>
    comparison.kronosdb.coefficientOfVariation > 0.15 || comparison.postgres.coefficientOfVariation > 0.15,
  )
  if (variable.length > 0) highlights.push(`${variable.length} comparison(s) exceeded 15% coefficient of variation.`)
  return highlights
}

function fmt(value: number): string {
  if (value >= 1_000) return value.toFixed(0)
  if (value >= 100) return value.toFixed(1)
  return value.toFixed(2)
}

export function comparisonTable(comparisons: readonly Comparison[]): string {
  const primary = comparisons.filter((comparison) =>
    ["eventsPerSec", "commandsPerSec", "catchupEventsPerSec", "handledEventsPerSec", "latencyP50Ms", "latencyP99Ms"].includes(comparison.metric),
  )
  const lines = [
    "scenario          parameters                    metric               kronosdb    postgres multiplier",
    "----------------- ----------------------------- -------------------- ---------- ---------- ----------",
  ]
  for (const comparison of primary) {
    lines.push(
      `${comparison.scenario.padEnd(17)} ${parameterKey(comparison.parameters).slice(0, 29).padEnd(29)} ${comparison.metric.padEnd(20)} ${fmt(comparison.kronosdb.median).padStart(10)} ${fmt(comparison.postgres.median).padStart(10)} ${comparison.multiplier.toFixed(2).padStart(9)}x`,
    )
  }
  return lines.join("\n")
}

export function stringifyDocument(document: BenchmarkDocument): string {
  return JSON.stringify(document, (_key, value) => typeof value === "bigint" ? value.toString() : value, 2)
}

export function createDocument(input: {
  options: BenchmarkOptions
  startedAt: Date
  finishedAt: Date
  host: HostMetadata
  backendMetadata: readonly BackendMetadata[]
  git: { sha: string; dirty: boolean }
  results: readonly ScenarioSample[]
  correctnessChecks: number
}): BenchmarkDocument {
  const comparisons = buildComparisons(input.results)
  return {
    schemaVersion: 1,
    run: {
      startedAt: input.startedAt.toISOString(),
      finishedAt: input.finishedAt.toISOString(),
      elapsedMs: input.finishedAt.getTime() - input.startedAt.getTime(),
      profile: input.options.profileName,
      seed: input.options.seed,
      order: input.options.order,
      scenario: input.options.scenario ?? "all",
      postgresAdapter: input.options.postgresAdapter,
      samples: input.options.samples,
    },
    environment: {
      host: input.host,
      backends: input.backendMetadata,
      git: input.git,
      resourceLimits: { cpu: input.options.cpu, memoryBytes: input.options.memoryBytes },
    },
    results: input.results,
    comparisons,
    highlights: buildHighlights(comparisons),
    correctness: { passed: true, checks: input.correctnessChecks },
  }
}
