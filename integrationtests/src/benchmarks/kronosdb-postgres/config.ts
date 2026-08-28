import { createHash } from "node:crypto"
import { emptyMetadata, qn, tag, type Metadata } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"

export type BackendName = "kronosdb" | "postgres"
export type ScenarioName = "append" | "append-c" | "workflow" | "dcb" | "rehydration" | "catchup-default" | "catchup-100" | "live"
export type ProfileName = "quick" | "full"
export type BackendOrder = "alternating" | "kronos-first" | "postgres-first"
export type PostgresAdapterName = "pg" | "bun-sql"

export const BENCH_EVENT_NAME = qn("benchmark", "StateChanged")
export const KRONOSDB_IMAGE = "ghcr.io/kronosdb/kronosdb:0.9.0"
export const POSTGRES_IMAGE = "postgres:16-alpine"

export type BenchmarkProfile = {
  readonly samples: number
  readonly warmups: number
  readonly append: ReadonlyArray<{ batchSize: number; commits: number }>
  readonly workflowPerWorker: number
  readonly workflowConcurrency: readonly number[]
  readonly workflowSeedDepth: number
  /** DCB scenario: commands per worker; each command sources one wide multi-type query. */
  readonly dcbPerWorker: number
  readonly dcbConcurrency: readonly number[]
  /** Unrelated events seeded up front so the query has to discriminate. */
  readonly dcbNoiseEvents: number
  /** Concurrency sweep: batch-1 appends split across this many workers. */
  readonly appendConcurrency: readonly number[]
  /** Total commits per concurrency level (divided among the workers). */
  readonly appendConcurrencyCommits: number
  readonly rehydrationDepths: readonly number[]
  readonly rehydrationReads: number
  readonly catchupEvents: number
  readonly liveSerialEvents: number
  readonly liveThroughputEvents: number
  readonly liveConcurrency: number
}

export const PROFILES: Record<ProfileName, BenchmarkProfile> = {
  quick: {
    samples: 3,
    warmups: 1,
    append: [
      { batchSize: 1, commits: 120 },
      { batchSize: 10, commits: 40 },
      { batchSize: 100, commits: 10 },
    ],
    workflowPerWorker: 40,
    workflowConcurrency: [1, 4, 16],
    workflowSeedDepth: 10,
    dcbPerWorker: 20,
    dcbConcurrency: [1, 4],
    dcbNoiseEvents: 800,
    appendConcurrency: [1, 4, 16],
    appendConcurrencyCommits: 96,
    rehydrationDepths: [10, 100, 1_000, 10_000],
    rehydrationReads: 3,
    catchupEvents: 2_000,
    liveSerialEvents: 80,
    liveThroughputEvents: 600,
    liveConcurrency: 16,
  },
  full: {
    samples: 5,
    warmups: 2,
    append: [
      { batchSize: 1, commits: 600 },
      { batchSize: 10, commits: 200 },
      { batchSize: 100, commits: 50 },
      { batchSize: 1_000, commits: 10 },
    ],
    workflowPerWorker: 200,
    workflowConcurrency: [1, 4, 16],
    workflowSeedDepth: 20,
    dcbPerWorker: 120,
    dcbConcurrency: [1, 4],
    dcbNoiseEvents: 4_000,
    appendConcurrency: [1, 4, 16, 64],
    appendConcurrencyCommits: 960,
    rehydrationDepths: [10, 100, 1_000, 10_000],
    rehydrationReads: 10,
    catchupEvents: 10_000,
    liveSerialEvents: 500,
    liveThroughputEvents: 5_000,
    liveConcurrency: 16,
  },
}

export type BenchmarkOptions = {
  readonly profileName: ProfileName
  readonly profile: BenchmarkProfile
  readonly seed: string
  readonly cpu: number
  readonly memoryBytes: number
  readonly order: BackendOrder
  readonly postgresAdapter: PostgresAdapterName
  readonly scenario?: ScenarioName
  readonly samples: number
  /** Only run this backend (default: both). */
  readonly backend?: BackendName
  /** KRONOSDB_GROUP_COMMIT_MS for the KronosDB container. 0 = strict inline fsync per append. */
  readonly kronosdbGroupCommitMs: number
  /** Override the KronosDB image, e.g. a locally built candidate. */
  readonly kronosdbImage: string
}

export type BenchPayload = {
  readonly aggregateId: string
  readonly ordinal: number
  readonly delta: number
  readonly commandId: string
  readonly description: string
  readonly attributes: {
    readonly region: string
    readonly priority: number
    readonly labels: readonly string[]
  }
}

export function deterministicUuid(input: string): string {
  const hex = createHash("sha256").update(input).digest("hex")
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.slice(20, 32)}`
}

export function makePayload(aggregateId: string, ordinal: number, seed: string): BenchPayload {
  return {
    aggregateId,
    ordinal,
    delta: (ordinal % 7) + 1,
    commandId: deterministicUuid(`${seed}:command:${aggregateId}:${ordinal}`),
    description: `state change ${ordinal} for ${aggregateId} ${"x".repeat(96)}`,
    attributes: {
      region: ["eu-north", "us-east", "ap-south"][ordinal % 3]!,
      priority: ordinal % 5,
      labels: ["benchmark", `bucket-${ordinal % 11}`, `seed-${seed}`],
    },
  }
}

export function makeEvent(
  aggregateId: string,
  ordinal: number,
  seed: string,
  extraTag?: { key: string; value: string },
): EventMessage {
  const tags = [tag("aggregateId", aggregateId)]
  if (extraTag) tags.push(tag(extraTag.key, extraTag.value))
  return {
    kind: "event",
    identifier: deterministicUuid(`${seed}:event:${aggregateId}:${ordinal}`),
    name: BENCH_EVENT_NAME,
    payload: makePayload(aggregateId, ordinal, seed),
    metadata: emptyMetadata() as Metadata,
    timestamp: 1_750_000_000_000 + ordinal,
    version: "1",
    tags,
  } as EventMessage
}

function positiveNumber(value: string | undefined, flag: string): number {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed <= 0) throw new Error(`${flag} must be a positive number`)
  return parsed
}

export function parseArgs(args: readonly string[]): BenchmarkOptions {
  let profileName: ProfileName = "quick"
  let seed = "kronosdb-postgres-v1"
  let cpu = 4
  let memoryGiB = 4
  let order: BackendOrder = "alternating"
  let postgresAdapter: PostgresAdapterName = "pg"
  let scenario: ScenarioName | undefined
  let sampleOverride: number | undefined
  let backend: BackendName | undefined
  let kronosdbGroupCommitMs = 2
  let kronosdbImage = KRONOSDB_IMAGE

  for (let i = 0; i < args.length; i++) {
    const flag = args[i]
    const value = args[++i]
    if (!flag?.startsWith("--")) throw new Error(`unexpected argument: ${flag}`)
    if (value === undefined) throw new Error(`missing value for ${flag}`)
    switch (flag) {
      case "--profile":
        if (value !== "quick" && value !== "full") throw new Error("--profile must be quick or full")
        profileName = value
        break
      case "--seed": seed = value; break
      case "--cpu": cpu = positiveNumber(value, flag); break
      case "--memory": memoryGiB = positiveNumber(value, flag); break
      case "--samples": sampleOverride = Math.floor(positiveNumber(value, flag)); break
      case "--postgres-adapter":
        if (value !== "pg" && value !== "bun-sql") throw new Error("--postgres-adapter must be pg or bun-sql")
        postgresAdapter = value
        break
      case "--order":
        if (value !== "alternating" && value !== "kronos-first" && value !== "postgres-first") {
          throw new Error("--order must be alternating, kronos-first, or postgres-first")
        }
        order = value
        break
      case "--backend":
        if (value !== "kronosdb" && value !== "postgres") throw new Error("--backend must be kronosdb or postgres")
        backend = value
        break
      case "--kronosdb-group-commit-ms": {
        const parsed = Number(value)
        if (!Number.isInteger(parsed) || parsed < 0) throw new Error("--kronosdb-group-commit-ms must be a non-negative integer")
        kronosdbGroupCommitMs = parsed
        break
      }
      case "--kronosdb-image": kronosdbImage = value; break
      case "--scenario": {
        const valid: ScenarioName[] = ["append", "append-c", "workflow", "dcb", "rehydration", "catchup-default", "catchup-100", "live"]
        if (!valid.includes(value as ScenarioName)) throw new Error(`unknown scenario: ${value}`)
        scenario = value as ScenarioName
        break
      }
      default: throw new Error(`unknown flag: ${flag}`)
    }
  }

  const profile = PROFILES[profileName]
  return {
    profileName,
    profile,
    seed,
    cpu,
    memoryBytes: Math.floor(memoryGiB * 1024 * 1024 * 1024),
    order,
    postgresAdapter,
    scenario,
    samples: sampleOverride ?? profile.samples,
    backend,
    kronosdbGroupCommitMs,
    kronosdbImage,
  }
}

export function backendOrder(groupIndex: number, order: BackendOrder): readonly BackendName[] {
  if (order === "kronos-first") return ["kronosdb", "postgres"]
  if (order === "postgres-first") return ["postgres", "kronosdb"]
  return groupIndex % 2 === 0 ? ["kronosdb", "postgres"] : ["postgres", "kronosdb"]
}
