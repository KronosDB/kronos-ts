/**
 * One replay, timed: a fresh processor reads the first `--limit` events of a log
 * that is already there, and the clock stops when its token is past the last one
 * it handles.
 *
 *   bun replay.ts --port 50051 --projection memory --stage filters --limit 1000000
 *
 * Under Node it is bundled first (`bun build replay.ts --target node`), because
 * the workspace packages are TypeScript. Prints one JSON line.
 *
 * Stages are what changes, nothing else:
 *   today        no filter, batch 1: the processor as it was
 *   today-tuned  no filter, batch 100: what somebody who knew to would run
 *   filters      the handlers' types sent to the server
 *   batch        + a replay batch of --replay-batch
 *   bulk         + the projection's writes buffered and flushed once per batch
 *   repository   + the same, through a repository: get, then save
 *   concurrent   + the repository's handler unchanged, the batch's buckets side by side
 *   upsert       + a repository handler that hands over a fold instead of reading the row
 */
import { concurrent, eventProcessor, jsonSerializer, kronos, localCommandBus, localQueryBus, unitOfWork, type EventMessage, type EventStore } from "@kronos-ts/core"
import { kronosDbConnection, kronosDbEventStore } from "@kronos-ts/kronosdb"
import { expected } from "./domain.js"
import { drizzleProjection, memoryProjection, type Projection } from "./projections.js"
import { bulkDrizzleProjection } from "./bulk.js"
import { repositoryProjection } from "./repository.js"

function arg(name: string, fallback?: string): string {
  const at = process.argv.indexOf(`--${name}`)
  const value = at >= 0 ? process.argv[at + 1] : fallback
  if (value === undefined) throw new Error(`--${name} is required`)
  return value
}

const port = Number(arg("port"))
const projectionName = arg("projection")
const stage = arg("stage")
const limit = Number(arg("limit", "1000000"))
const replayBatch = Number(arg("replay-batch", "1000"))
const all = process.argv.includes("--all")
const exact = process.argv.includes("--exact")
// A row per event instead of a row per account: nothing for the buffer to merge.
const uniqueKeys = process.argv.includes("--unique-keys")
// A server has other things on its event loop. Without any, Node leaves the
// unref'd hop the old processor used to the next socket read, and a quiet loop
// never takes it. `--heartbeat 1` puts a 1 ms timer on the loop, which is the
// kindest thing that can be said for that code.
const heartbeat = Number(arg("heartbeat", "0"))
if (heartbeat > 0) setInterval(() => {}, heartbeat)
const pgUrl = projectionName === "memory" ? "" : arg("pg")

const kdb = await kronosDbConnection({ componentName: "replay-bench", host: "127.0.0.1", port, serializer: jsonSerializer() })
const base: EventStore = kronosDbEventStore(kdb, "default")

/** The store as it was: whatever the processor asks, it asks for everything. */
const everything: EventStore = {
  ...base,
  open: (condition) => base.open({ position: condition.position, ...(condition.token ? { token: condition.token } : {}) }),
}

const config: Record<string, { filter: boolean; batchSize: number; replayBatchSize: number; writes: "each" | "bulk" | "repository" | "upsert"; concurrent?: boolean }> = {
  today: { filter: false, batchSize: 1, replayBatchSize: 1, writes: "each" },
  "today-tuned": { filter: false, batchSize: 100, replayBatchSize: 100, writes: "each" },
  filters: { filter: true, batchSize: 1, replayBatchSize: 1, writes: "each" },
  batch: { filter: true, batchSize: 1, replayBatchSize: replayBatch, writes: "each" },
  bulk: { filter: true, batchSize: 1, replayBatchSize: replayBatch, writes: "bulk" },
  repository: { filter: true, batchSize: 1, replayBatchSize: replayBatch, writes: "repository" },
  concurrent: { filter: true, batchSize: 1, replayBatchSize: replayBatch, writes: "repository", concurrent: true },
  upsert: { filter: true, batchSize: 1, replayBatchSize: replayBatch, writes: "upsert" },
}
const stageConfig = config[stage]
if (!stageConfig) throw new Error(`unknown stage ${stage}`)

const projection: Projection =
  projectionName === "memory"
    ? memoryProjection(all)
    : stageConfig.writes === "bulk"
      ? await bulkDrizzleProjection(pgUrl, uniqueKeys)
      : stageConfig.writes === "repository" || stageConfig.writes === "upsert"
        ? await repositoryProjection(pgUrl, uniqueKeys, stageConfig.writes === "upsert" ? "upsert" : "get-save")
        : await drizzleProjection(pgUrl, uniqueKeys)

const name = `replay-${stage}-${Date.now().toString(36)}`
const configured = eventProcessor({
  name,
  eventStore: stageConfig.filter ? base : everything,
  tokenStore: projection.tokenStore ?? (await import("@kronos-ts/core")).inMemoryTokenStore(),
  unitOfWork: projection.unitOfWork,
  batchSize: stageConfig.batchSize,
  replayBatchSize: stageConfig.replayBatchSize,
} as any)
// A bucket is what a handler touches: the account, or with a row per event the event.
const byRow = (e: EventMessage) => (uniqueKeys ? e.identifier : e.tags.find((t) => t.key === "account")?.value ?? e.identifier)
const processor = stageConfig.concurrent ? concurrent(configured, byRow) : configured

const { handled: expectedHandled, lastToken } = expected(limit, all)

// The clock is stopped by the handler that sees the last event, not by a timer
// polling the processor: a timer on the loop changes how fast the loop turns.
let lastHandled!: () => void
const reached = new Promise<void>((resolve) => (lastHandled = resolve))
const handlers = projection.handlers.map((definition) => ({
  ...definition,
  handler: async (message: { sequence: bigint }, ctx: unknown) => {
    await definition.handler(message as any, ctx as any)
    if (message.sequence === lastToken - 1n) lastHandled()
  },
}))

const cpu0 = process.cpuUsage()
const t0 = performance.now()
const app = kronos({
  eventHandlers: handlers.map((h) => ({ ...h, commandBus: localCommandBus(unitOfWork), queryBus: localQueryBus(unitOfWork), processor }) as any),
})
const running = app.processors.get(name)!

await Promise.race([reached, new Promise((_, fail) => setTimeout(() => fail(new Error("gave up waiting for the last event")), 20 * 60_000).unref())])
// Handled is not committed: the token moves when the batch has.
while (running.position < lastToken) {
  if (running.status().error) throw running.status().error
  await new Promise((r) => setTimeout(r, 0))
}
const ms = performance.now() - t0
const cpu = process.cpuUsage(cpu0)

await app.stop()
const handled = await projection.handled()
const result = {
  runtime: process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.versions.node}`,
  projection: projectionName,
  stage: `${stage}${all ? "+all" : ""}${uniqueKeys ? "+unique-keys" : ""}`,
  limit,
  expectedHandled,
  handled,
  ms: Math.round(ms),
  scannedPerSecond: Math.round(limit / (ms / 1000)),
  handledPerSecond: Math.round(expectedHandled / (ms / 1000)),
  cpuMs: Math.round((cpu.user + cpu.system) / 1000),
  rssMb: Math.round(process.memoryUsage().rss / 1048576),
}
console.log(JSON.stringify(result))
await projection.close()
await kdb.close()
// A prefix run keeps reading until it is stopped, so it may have handled a few more.
process.exit(exact ? (handled === expectedHandled ? 0 : 3) : handled >= expectedHandled ? 0 : 3)
