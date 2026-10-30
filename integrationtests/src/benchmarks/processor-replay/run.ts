/**
 * The replay benchmark: every runtime × projection × stage, medians of a few runs.
 *
 *   KRONOSDB_SERVER_BIN=/path/to/kronosdb-server bun run.ts
 *   BENCH_EVENTS=1000000 BENCH_REPS=3 BENCH_RUNTIMES=bun,node BENCH_PROJECTIONS=memory,drizzle bun run.ts
 *   BENCH_STAGES=today,filters,batch,bulk,repository,concurrent,upsert BENCH_REPLAY_BATCH=1000 bun run.ts
 *   BENCH_SEGMENTS=0,4,16 bun run.ts   (0: not segmented)
 *
 * One server, seeded once; one Postgres container (named for this run, removed
 * at the end) when a drizzle projection is asked for. Every run is a child
 * process (`replay.ts`), so a runtime's heap and JIT start cold each time, as
 * a deployment's do. Node runs the file bundled with `bun build --target node`.
 *
 * A stage that is too slow to run over the whole log reads only a prefix
 * (BENCH_PREFIXES) and is marked `extrapolated`: its rate is real, its 1M
 * figure is rate × size.
 */
import { spawnSync, execFileSync } from "node:child_process"
import { mkdirSync, writeFileSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { jsonSerializer } from "@kronos-ts/core"
import { kronosDbConnection, kronosDbEventStore } from "@kronos-ts/kronosdb"
import { startKronosDb } from "../../kronosdb-server.js"
import { seed } from "./domain.js"

const here = dirname(fileURLToPath(import.meta.url))
const out = resolve(here, "..", "..", "..", "..", ".kronosdb-data", "bench")
mkdirSync(out, { recursive: true })

const list = (name: string, fallback: string) => (process.env[name] ?? fallback).split(",").filter(Boolean)
const events = Number(process.env.BENCH_EVENTS ?? 1_000_000)
const reps = Number(process.env.BENCH_REPS ?? 3)
const runtimes = list("BENCH_RUNTIMES", "bun,node")
const projections = list("BENCH_PROJECTIONS", "memory,drizzle")
const stages = list("BENCH_STAGES", "today,filters,batch,bulk")
const replayBatch = process.env.BENCH_REPLAY_BATCH ?? "1000"
const segmentCounts = list("BENCH_SEGMENTS", "0").map(Number)
// "--all": a processor that handles every type, not one in ten.
const extra = [process.env.BENCH_ARGS, process.env.BENCH_HEARTBEAT_MS ? `--heartbeat ${process.env.BENCH_HEARTBEAT_MS}` : ""].filter(Boolean).join(" ")
// "projection:stage=events": stages too slow to read the whole log (drizzle at batch 1
// does about 300 scanned events a second) read only a prefix, and are marked as such.
const prefixes = new Map(
  list("BENCH_PREFIXES", "drizzle:today=10000,drizzle:filters=200000").map((pair) => {
    const [key, value] = pair.split("=") as [string, string]
    return [key, Number(value)] as const
  }),
)

type Row = {
  runtime: string
  projection: string
  stage: string
  limit: number
  expectedHandled: number
  handled: number
  ms: number
  scannedPerSecond: number
  handledPerSecond: number
  cpuMs: number
  rssMb: number
  extrapolated: boolean
}

function postgres(): { url: string; stop(): void } {
  const name = `kronos-replay-bench-${process.pid}`
  execFileSync("docker", [
    "run", "-d", "--name", name, "-p", "127.0.0.1::5432",
    "-e", "POSTGRES_PASSWORD=bench", "-e", "POSTGRES_DB=bench",
    "postgres:16-alpine",
  ])
  const stop = () => {
    try {
      execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" })
    } catch {
      // Already gone.
    }
  }
  try {
    for (let i = 0; ; i++) {
      const ready = spawnSync("docker", ["exec", name, "pg_isready", "-U", "postgres", "-d", "bench", "-h", "127.0.0.1"])
      // The image restarts the server once after initialisation; two ready answers in a row are the real one.
      if (ready.status === 0 && spawnSync("docker", ["exec", name, "sh", "-c", "sleep 1; pg_isready -U postgres -h 127.0.0.1"]).status === 0) break
      if (i > 240) throw new Error("postgres did not become ready")
      spawnSync("sleep", ["0.5"])
    }
    const mapping = execFileSync("docker", ["port", name, "5432/tcp"], { encoding: "utf8" }).trim().split("\n")[0]!
    const port = mapping.slice(mapping.lastIndexOf(":") + 1)
    return { url: `postgresql://postgres:bench@127.0.0.1:${port}/bench`, stop }
  } catch (error) {
    stop()
    throw error
  }
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)]!
}

const server = await startKronosDb()
const db = projections.includes("drizzle") ? postgres() : undefined
const rows: Row[] = []
const cleanup = async () => {
  db?.stop()
  await server.stop()
}
process.once("SIGINT", () => void cleanup().then(() => process.exit(130)))

try {
  const kdb = await kronosDbConnection({ componentName: "replay-seed", host: server.host, port: server.port, serializer: jsonSerializer() })
  const seeding = performance.now()
  await seed(kronosDbEventStore(kdb, "default"), events)
  console.error(`seeded ${events} events in ${Math.round(performance.now() - seeding)} ms`)
  await kdb.close()

  const bundle = join(out, "replay.mjs")
  if (runtimes.includes("node")) {
    const built = spawnSync(process.execPath, ["build", join(here, "replay.ts"), "--target", "node", "--outfile", bundle, "--external", "pg-native"], { stdio: "inherit" })
    if (built.status !== 0) throw new Error("bundling replay.ts for node failed")
  }

  for (const runtime of runtimes) {
    for (const projection of projections) {
      for (const stage of stages) for (const segments of segmentCounts) {
        if (projection === "memory" && ["bulk", "repository", "concurrent", "upsert"].includes(stage)) continue
        const prefix = prefixes.get(`${projection}:${stage}`)
        const slow = prefix !== undefined && prefix < events
        const limit = slow ? prefix : events
        const samples: Row[] = []
        for (let rep = 0; rep < reps; rep++) {
          const command = runtime === "bun" ? [process.execPath, join(here, "replay.ts")] : ["node", bundle]
          const ran = spawnSync(
            command[0]!,
            [...command.slice(1), "--port", String(server.port), "--projection", projection, "--stage", stage, "--limit", String(limit), "--replay-batch", replayBatch, ...(slow ? [] : ["--exact"]), ...(segments > 0 ? ["--segments", String(segments)] : []), ...(db ? ["--pg", db.url] : []), ...(extra ? extra.split(" ") : [])],
            { encoding: "utf8" },
          )
          const line = ran.stdout.trim().split("\n").at(-1)
          if (!line?.startsWith("{")) {
            console.error(`${runtime} ${projection} ${stage} failed (${ran.status}):\n${ran.stdout}\n${ran.stderr}`)
            continue
          }
          const row = { ...(JSON.parse(line) as Omit<Row, "extrapolated">), extrapolated: slow }
          if (ran.status !== 0) console.error(`${runtime} ${projection} ${stage}: WRONG RESULT handled ${row.handled} of ${row.expectedHandled}`)
          samples.push(row)
          console.error(`${row.runtime} ${projection} ${row.stage}${slow ? " (prefix)" : ""}: ${row.ms} ms, ${row.scannedPerSecond}/s scanned, ${row.handledPerSecond}/s handled, cpu ${row.cpuMs} ms, rss ${row.rssMb} MB`)
        }
        if (samples.length === 0) continue
        const mid = samples.sort((a, b) => a.ms - b.ms)[Math.floor(samples.length / 2)]!
        rows.push({ ...mid, ms: median(samples.map((s) => s.ms)) })
      }
    }
  }
} finally {
  await cleanup()
}

writeFileSync(join(out, `processor-replay-${Date.now()}.json`), JSON.stringify({ events, reps, replayBatch, rows }, null, 2))
console.log(`\nmedian of ${reps} runs, replay of ${events} events (10% handled)`)
console.log("runtime | projection | stage | ms | 1M-equivalent s | scanned/s | handled/s | cpu ms | rss MB")
for (const r of rows) {
  const million = r.extrapolated ? (1_000_000 / r.scannedPerSecond) : r.ms / 1000 * (1_000_000 / r.limit)
  console.log(`${r.runtime} | ${r.projection} | ${r.stage}${r.extrapolated ? " (prefix " + r.limit + ")" : ""} | ${r.ms} | ${million.toFixed(1)} | ${r.scannedPerSecond} | ${r.handledPerSecond} | ${r.cpuMs} | ${r.rssMb}`)
}
