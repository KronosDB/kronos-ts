/**
 * The client benchmark: every runtime × client × shape, against one server.
 *
 *   KRONOSDB_SERVER_BIN=/path/to/kronosdb-server bun matrix.ts
 *   KRONOSDB_SERVER_ARGS="--ack-mode written" KRONOSDB_SERVER_BIN=… bun matrix.ts
 *   BENCH_ENTRIES="native=native.ts,grpc=/path/to/grpc-entry.ts" bun matrix.ts
 *
 * An entry is a file that calls `main(name, connect)` from `harness.ts`. Under
 * Bun it runs as is; for Node it is first bundled with `bun build --target node`
 * into `.kronosdb-data/bench/`, because the workspace packages are TypeScript
 * source. The gRPC client is no longer in this tree: build an entry for it from
 * a checkout that has it (see the kronos-ts history before the native protocol)
 * and name it in BENCH_ENTRIES. Commands have a matrix of their own: commands-matrix.ts.
 */
import { spawnSync } from "node:child_process"
import { mkdirSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import { startKronosDb } from "../../kronosdb-server.js"
import type { CellResult } from "./harness.js"

const here = dirname(fileURLToPath(import.meta.url))
const out = resolve(here, "..", "..", "..", "..", ".kronosdb-data", "bench")
mkdirSync(out, { recursive: true })

const entries = (process.env.BENCH_ENTRIES ?? `native=${join(here, "native.ts")}`)
  .split(",")
  .map((pair) => {
    const [name, file] = pair.split("=") as [string, string]
    return { name, file }
  })
const runtimes = (process.env.BENCH_RUNTIMES ?? "bun,node").split(",")
const seconds = process.env.BENCH_SECONDS ?? "4"
const shapes = [
  { concurrency: 1, batch: 1 },
  { concurrency: 16, batch: 1 },
  { concurrency: 64, batch: 1 },
  { concurrency: 1, batch: 100 },
]

const server = await startKronosDb()
const results: CellResult[] = []
try {
  for (const runtime of runtimes) {
    for (const { name, file } of entries) {
      let command: string[]
      if (runtime === "bun") {
        command = [process.execPath, file]
      } else {
        const bundle = join(out, `${name}.mjs`)
        const built = spawnSync(process.execPath, ["build", file, "--target", "node", "--outfile", bundle], { stdio: "inherit" })
        if (built.status !== 0) throw new Error(`bundling ${file} for node failed`)
        command = ["node", bundle]
      }
      for (const shape of shapes) {
        const ran = spawnSync(
          command[0]!,
          [...command.slice(1), "--port", String(server.port), "--concurrency", String(shape.concurrency), "--batch", String(shape.batch), "--seconds", seconds],
          { encoding: "utf8" },
        )
        const line = ran.stdout.trim().split("\n").at(-1)
        if (ran.status !== 0 || !line?.startsWith("{")) {
          console.error(`${runtime} ${name} c=${shape.concurrency} batch=${shape.batch} failed:\n${ran.stdout}\n${ran.stderr}`)
          continue
        }
        const result = JSON.parse(line) as CellResult
        results.push(result)
        console.error(
          `${result.runtime} ${name} c=${shape.concurrency} b=${shape.batch}: ${result.opsPerSecond.toFixed(0)} ops/s, p50 ${result.p50Ms.toFixed(2)}ms, ${result.cpuMicrosPerOp.toFixed(1)}µs cpu/op`,
        )
      }
    }
  }
} finally {
  await server.stop()
}

console.log("| runtime | client | shape | ops/s | events/s | p50 ms | p99 ms | client CPU µs/op | µs/event |")
console.log("|---|---|---|---:|---:|---:|---:|---:|---:|")
for (const r of results) {
  const shape = r.batch > 1 ? `batch ${r.batch}` : `c=${r.concurrency}`
  console.log(
    `| ${r.runtime} | ${r.client} | ${shape} | ${r.opsPerSecond.toFixed(0)} | ${r.eventsPerSecond.toFixed(0)} | ${r.p50Ms.toFixed(2)} | ${r.p99Ms.toFixed(2)} | ${r.cpuMicrosPerOp.toFixed(1)} | ${r.cpuMicrosPerEvent.toFixed(1)} |`,
  )
}
