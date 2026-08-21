/**
 * @kronos-ts/postgres conditional-append bench.
 *
 * Drives the event store directly via postgresEventStore — no framework
 * overhead, no command gateway, no projections. Measures the storage layer's
 * `source → append (with DCB precondition)` round-trip in four scenarios:
 *
 *   S1  Single-writer throughput + degradation curve
 *       One writer, N events, windowed ops/sec to see how the table ages.
 *
 *   S2  Disjoint concurrent writers (scaling)
 *       1 / 4 / 8 / 16 workers on disjoint tag spaces. Should scale near-
 *       linearly: advisory-lock taxonomy permits parallel commits on
 *       disjoint query tags.
 *
 *   S3  Same-tag contention (worst case)
 *       N workers fighting one tag with retry-on-AppendConditionError.
 *       Shows useful commits/sec + retry amplification.
 *
 *   S4  Adapter shootout
 *       Replays a short S1 against pgAdapter, postgresAdapter, bunSqlAdapter
 *       to expose driver-level overhead.
 *
 * This remains a Postgres adapter diagnostic. For the canonical durable
 * KronosDB-versus-Postgres event-sourcing and processing comparison, run
 * `bun run benchmark:kronosdb-postgres` from the repository root.
 *
 * Run: bun run integrationtests/examples/postgres-bench.ts
 * Requires docker for testcontainers and Bun >= 1.2 for bunSqlAdapter.
 */
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { qn, tag, type Metadata } from "@kronos-ts/core"
import { jsonSerializer } from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import type { EventStore } from "@kronos-ts/core"
import { descriptorBasedTagResolver } from "@kronos-ts/core"
import { postgresEventStore, postgresPool } from "@kronos-ts/postgres"
import type { PostgresAdapter, PostgresResource } from "@kronos-ts/postgres"
import { DEFAULT_TABLE_NAMES } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"

// ---------------------------------------------------------------------------
// Bench knobs — keep total wall-clock under ~5 minutes.
// ---------------------------------------------------------------------------

const S1_EVENTS = 50_000
const S1_WINDOW = 5_000
const S2_WORKER_COUNTS = [1, 4, 8, 16] as const
const S2_PER_WORKER = 5_000
const S3_WORKER_COUNTS = [2, 4, 8] as const
const S3_TARGET_COMMITS = 2_000
const S4_EVENTS = 20_000

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const EVENT_NAME = qn("bench", "Tick")

function makeEvent(entityKey: string, value: number): EventMessage {
  return {
    identifier: crypto.randomUUID(),
    name: EVENT_NAME,
    payload: { value },
    metadata: {} as Metadata,
    timestamp: Date.now(),
    version: "1",
    tags: [tag("entity", entityKey)],
  } as unknown as EventMessage
}

function fmt(n: number): string {
  if (n >= 1000) return n.toFixed(0)
  if (n >= 100) return n.toFixed(1)
  return n.toFixed(2)
}

function percentile(sortedMs: number[], p: number): number {
  if (sortedMs.length === 0) return 0
  const idx = Math.min(sortedMs.length - 1, Math.floor((p / 100) * sortedMs.length))
  return sortedMs[idx]!
}

/** The pool owns connect + bootstrap; the store is a function of it. */
async function buildStore(pool: PostgresResource): Promise<EventStore> {
  await pool.start()
  return postgresEventStore(pool, {
    serializer: jsonSerializer(),
    tagResolver: descriptorBasedTagResolver(),
  })
}

async function truncate(pool: PostgresResource): Promise<void> {
  await pool.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.events} RESTART IDENTITY`)
  await pool.query(`TRUNCATE TABLE ${DEFAULT_TABLE_NAMES.snapshots}`)
}

// ---------------------------------------------------------------------------
// Scenario 1 — single-writer throughput + degradation curve
// ---------------------------------------------------------------------------

type S1Result = {
  windows: Array<{ from: number; to: number; opsPerSec: number; p50: number; p95: number; p99: number }>
  totalMs: number
  opsPerSec: number
}

async function scenario1(store: EventStore, total: number, windowSize: number): Promise<S1Result> {
  const windows: S1Result["windows"] = []
  let windowLatencies: number[] = []
  let windowStart = performance.now()
  const t0 = performance.now()

  for (let i = 0; i < total; i++) {
    // Each iteration is one unique entity: source returns empty events + marker,
    // then we append one event under the precondition that nothing new matched
    // since marker. This is the cheapest possible "conditional append".
    const entityKey = `s1-${i}`
    const query = { tags: { entity: entityKey } }
    const t = performance.now()
    const { marker } = await store.source({ query })
    await store.append([makeEvent(entityKey, i)], { query, marker })
    windowLatencies.push(performance.now() - t)

    if ((i + 1) % windowSize === 0) {
      const dt = (performance.now() - windowStart) / 1000
      windowLatencies.sort((a, b) => a - b)
      windows.push({
        from: i + 1 - windowSize,
        to: i + 1,
        opsPerSec: windowSize / dt,
        p50: percentile(windowLatencies, 50),
        p95: percentile(windowLatencies, 95),
        p99: percentile(windowLatencies, 99),
      })
      windowLatencies = []
      windowStart = performance.now()
    }
  }
  const totalMs = performance.now() - t0
  return { windows, totalMs, opsPerSec: total / (totalMs / 1000) }
}

function printS1(r: S1Result): void {
  console.log(`  total: ${total(r.totalMs)} | overall: ${fmt(r.opsPerSec)} ops/sec`)
  console.log(`  window      | ops/sec | p50ms  | p95ms  | p99ms`)
  console.log(`  ------------+---------+--------+--------+--------`)
  for (const w of r.windows) {
    console.log(
      `  ${String(w.from).padStart(5)}-${String(w.to).padStart(5)} | ${fmt(w.opsPerSec).padStart(7)} | ${w.p50.toFixed(2).padStart(6)} | ${w.p95.toFixed(2).padStart(6)} | ${w.p99.toFixed(2).padStart(6)}`,
    )
  }
}

function total(ms: number): string {
  return ms > 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms.toFixed(0)}ms`
}

// ---------------------------------------------------------------------------
// Scenario 2 — disjoint concurrent writers
// ---------------------------------------------------------------------------

type S2Run = {
  workers: number
  totalMs: number
  totalOps: number
  opsPerSec: number
  perWorkerOpsPerSec: number
}

async function scenario2Run(store: EventStore, workers: number, perWorker: number): Promise<S2Run> {
  const t0 = performance.now()
  await Promise.all(
    Array.from({ length: workers }, (_, w) =>
      (async () => {
        for (let i = 0; i < perWorker; i++) {
          const entityKey = `s2-w${w}-${i}`
          const query = { tags: { entity: entityKey } }
          const { marker } = await store.source({ query })
          await store.append([makeEvent(entityKey, i)], { query, marker })
        }
      })(),
    ),
  )
  const totalMs = performance.now() - t0
  const totalOps = workers * perWorker
  return {
    workers,
    totalMs,
    totalOps,
    opsPerSec: totalOps / (totalMs / 1000),
    perWorkerOpsPerSec: perWorker / (totalMs / 1000),
  }
}

function printS2(runs: S2Run[]): void {
  console.log(`  workers | total ops | wall    | total ops/sec | per-worker | scaling`)
  console.log(`  --------+-----------+---------+---------------+------------+--------`)
  const baseline = runs[0]?.opsPerSec ?? 1
  for (const r of runs) {
    const scale = (r.opsPerSec / baseline).toFixed(2) + "x"
    console.log(
      `  ${String(r.workers).padStart(7)} | ${String(r.totalOps).padStart(9)} | ${total(r.totalMs).padStart(7)} | ${fmt(r.opsPerSec).padStart(13)} | ${fmt(r.perWorkerOpsPerSec).padStart(10)} | ${scale.padStart(6)}`,
    )
  }
}

// ---------------------------------------------------------------------------
// Scenario 3 — same-tag contention
// ---------------------------------------------------------------------------

type S3Run = {
  workers: number
  commits: number
  retries: number
  totalMs: number
  commitsPerSec: number
  retryRatio: number
}

async function scenario3Run(store: EventStore, workers: number, targetCommits: number): Promise<S3Run> {
  const sharedKey = "s3-shared"
  const query = { tags: { entity: sharedKey } }
  let commits = 0
  let retries = 0
  const t0 = performance.now()

  await Promise.all(
    Array.from({ length: workers }, (_, w) =>
      (async () => {
        while (commits < targetCommits) {
          const { marker } = await store.source({ query })
          try {
            await store.append([makeEvent(sharedKey, w)], { query, marker })
            commits++
          } catch (err) {
            // AppendConditionError — refresh and retry
            const msg = (err as Error).message
            if (!msg.includes("Append condition violated")) throw err
            retries++
          }
        }
      })(),
    ),
  )
  const totalMs = performance.now() - t0
  return {
    workers,
    commits,
    retries,
    totalMs,
    commitsPerSec: commits / (totalMs / 1000),
    retryRatio: retries / commits,
  }
}

function printS3(runs: S3Run[]): void {
  console.log(`  workers | commits | retries | retry/commit | wall    | commits/sec`)
  console.log(`  --------+---------+---------+--------------+---------+------------`)
  for (const r of runs) {
    console.log(
      `  ${String(r.workers).padStart(7)} | ${String(r.commits).padStart(7)} | ${String(r.retries).padStart(7)} | ${r.retryRatio.toFixed(2).padStart(12)} | ${total(r.totalMs).padStart(7)} | ${fmt(r.commitsPerSec).padStart(11)}`,
    )
  }
}

// ---------------------------------------------------------------------------
// Scenario 4 — adapter shootout
// ---------------------------------------------------------------------------

type AdapterRun = {
  name: string
  result: S1Result
}

async function scenario4(connectionString: string, events: number): Promise<AdapterRun[]> {
  const results: AdapterRun[] = []
  for (const { name, build } of [
    { name: "pgAdapter", build: () => pgAdapter({ connectionString }) },
    { name: "postgresAdapter", build: () => postgresAdapter({ connectionString }) },
    { name: "bunSqlAdapter", build: () => bunSqlAdapter({ connectionString }) },
  ]) {
    const pool = postgresPool(build())
    try {
      const store = await buildStore(pool)
      await truncate(pool)
      const result = await scenario1(store, events, Math.max(1000, Math.floor(events / 5)))
      results.push({ name, result })
    } finally {
      await pool.close()
    }
  }
  return results
}

function printS4(runs: AdapterRun[]): void {
  console.log(`  adapter         | total   | overall ops/sec | p50ms | p95ms | p99ms`)
  console.log(`  ----------------+---------+-----------------+-------+-------+-------`)
  for (const r of runs) {
    // Aggregate window percentiles by averaging across windows (good enough for comparison)
    const ws = r.result.windows
    const avgP50 = ws.reduce((a, w) => a + w.p50, 0) / ws.length
    const avgP95 = ws.reduce((a, w) => a + w.p95, 0) / ws.length
    const avgP99 = ws.reduce((a, w) => a + w.p99, 0) / ws.length
    console.log(
      `  ${r.name.padEnd(15)} | ${total(r.result.totalMs).padStart(7)} | ${fmt(r.result.opsPerSec).padStart(15)} | ${avgP50.toFixed(2).padStart(5)} | ${avgP95.toFixed(2).padStart(5)} | ${avgP99.toFixed(2).padStart(5)}`,
    )
  }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("== booting postgres:16-alpine ==")
  const container: StartedTestContainer = await new GenericContainer("postgres:16-alpine")
    .withEnvironment({
      POSTGRES_PASSWORD: "demo",
      POSTGRES_DB: "demo",
      POSTGRES_USER: "demo",
    })
    .withExposedPorts(5432)
    .withWaitStrategy(Wait.forLogMessage("database system is ready to accept connections", 2))
    .start()

  const connectionString =
    `postgresql://demo:demo@${container.getHost()}:${container.getMappedPort(5432)}/demo`

  // Primary adapter for S1/S2/S3 — bunSqlAdapter as requested.
  const pool = postgresPool(bunSqlAdapter({ connectionString }))

  try {
    const store = await buildStore(pool)

    console.log("\n== S1: single-writer throughput + degradation ==")
    console.log(`  ${S1_EVENTS} appends, windows of ${S1_WINDOW}`)
    await truncate(pool)
    const s1 = await scenario1(store, S1_EVENTS, S1_WINDOW)
    printS1(s1)

    console.log("\n== S2: disjoint concurrent writers ==")
    console.log(`  ${S2_PER_WORKER} appends per worker on disjoint tag spaces`)
    const s2Runs: S2Run[] = []
    for (const workers of S2_WORKER_COUNTS) {
      await truncate(pool)
      const run = await scenario2Run(store, workers, S2_PER_WORKER)
      s2Runs.push(run)
    }
    printS2(s2Runs)

    console.log("\n== S3: same-tag contention ==")
    console.log(`  ${S3_TARGET_COMMITS} target commits, all workers on one shared tag`)
    const s3Runs: S3Run[] = []
    for (const workers of S3_WORKER_COUNTS) {
      await truncate(pool)
      const run = await scenario3Run(store, workers, S3_TARGET_COMMITS)
      s3Runs.push(run)
    }
    printS3(s3Runs)

    // Close the primary pool before the shootout so each run owns its own.
    await pool.close()

    console.log("\n== S4: adapter shootout (S1 shape) ==")
    console.log(`  ${S4_EVENTS} appends per adapter`)
    const s4 = await scenario4(connectionString, S4_EVENTS)
    printS4(s4)
  } finally {
    try { await pool.close() } catch { /* already closed */ }
    await container.stop()
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
