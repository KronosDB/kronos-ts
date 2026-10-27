/**
 * The command-dispatch benchmark: every runtime × client × concurrency.
 *
 *   BENCH_ENTRIES="native=commands-native.ts@/path/to/native-server,grpc=/path/to/commands-grpc.ts@/path/to/grpc-server" \
 *     bun commands-matrix.ts
 *
 * An entry is `name=file@server`: a file that calls `main(name, connect)` from
 * `command-harness.ts`, and the server binary it is measured against (a client
 * and the server build that speaks its protocol). Under Bun the file runs as
 * is; for Node it is first bundled with `bun build --target node` into
 * `.kronosdb-data/bench/`, because the workspace packages are TypeScript
 * source. The gRPC client is no longer in this tree: build an entry for it from
 * a checkout that has it and name it here.
 *
 * Each cell starts a fresh server, a handler process and a dispatcher process
 * of the same runtime and client, so no cell inherits another's warm sockets.
 */
import { spawn, spawnSync } from "node:child_process"
import { mkdirSync } from "node:fs"
import { dirname, join, resolve } from "node:path"
import { createInterface } from "node:readline"
import { fileURLToPath } from "node:url"
import { startKronosDb } from "../../kronosdb-server.js"
import type { CommandCell } from "./command-harness.js"

const here = dirname(fileURLToPath(import.meta.url))
const out = resolve(here, "..", "..", "..", "..", ".kronosdb-data", "bench")
mkdirSync(out, { recursive: true })

const entries = (process.env.BENCH_ENTRIES ?? `native=${join(here, "commands-native.ts")}@${process.env.KRONOSDB_SERVER_BIN ?? ""}`)
  .split(",")
  .map((triple) => {
    const [pair, server] = triple.split("@") as [string, string]
    const [name, file] = pair.split("=") as [string, string]
    return { name, file, server }
  })
const runtimes = (process.env.BENCH_RUNTIMES ?? "bun,node").split(",")
const seconds = process.env.BENCH_SECONDS ?? "4"
const shapes = (process.env.BENCH_CONCURRENCY ?? "1,16,64").split(",").map(Number)

const cells: Array<CommandCell & { handlerOps: number }> = []
for (const runtime of runtimes) {
  for (const { name, file, server: binary } of entries) {
    let command: string[]
    if (runtime === "bun") {
      command = [process.execPath, file]
    } else {
      const bundle = join(out, `commands-${name}.mjs`)
      const built = spawnSync(process.execPath, ["build", file, "--target", "node", "--outfile", bundle], { stdio: "inherit" })
      if (built.status !== 0) throw new Error(`bundling ${file} for node failed`)
      command = ["node", bundle]
    }
    for (const concurrency of shapes) {
      process.env.KRONOSDB_SERVER_BIN = binary
      const server = await startKronosDb()
      try {
        const common = ["--port", String(server.port)]
        const handler = spawn(command[0]!, [...command.slice(1), "--role", "handler", ...common], { stdio: ["pipe", "pipe", "inherit"] })
        const lines = createInterface({ input: handler.stdout! })[Symbol.asyncIterator]()
        const ready = await lines.next()
        if (ready.value !== "READY") throw new Error(`handler did not start: ${ready.value}`)

        const ran = spawnSync(command[0]!, [...command.slice(1), "--role", "dispatcher", ...common, "--concurrency", String(concurrency), "--seconds", seconds], { encoding: "utf8" })
        const line = ran.stdout.trim().split("\n").at(-1)
        handler.stdin!.write("report\n")
        const report = await lines.next()
        handler.stdin!.end()
        if (ran.status !== 0 || !line?.startsWith("{") || report.value === undefined) {
          console.error(`${runtime} ${name} c=${concurrency} failed:\n${ran.stdout}\n${ran.stderr}`)
          continue
        }
        const result = JSON.parse(line) as CommandCell
        const handled = JSON.parse(report.value) as { handled: number; cpuMicros: number }
        const cell = { ...result, handlerCpuMicrosPerOp: handled.cpuMicros / Math.max(1, handled.handled), handlerOps: handled.handled }
        cells.push(cell)
        console.error(
          `${result.runtime} ${name} c=${concurrency}: ${result.opsPerSecond.toFixed(0)} ops/s, p50 ${result.p50Ms.toFixed(2)}ms, p99 ${result.p99Ms.toFixed(2)}ms, ${result.cpuMicrosPerOp.toFixed(1)}µs dispatcher cpu/op, ${cell.handlerCpuMicrosPerOp.toFixed(1)}µs handler cpu/op`,
        )
      } finally {
        await server.stop()
      }
    }
  }
}

console.log("| runtime | client | concurrency | ops/s | p50 ms | p99 ms | dispatcher CPU µs/op | handler CPU µs/op |")
console.log("|---|---|---:|---:|---:|---:|---:|---:|")
for (const r of cells) {
  console.log(
    `| ${r.runtime} | ${r.client} | ${r.concurrency} | ${r.opsPerSecond.toFixed(0)} | ${r.p50Ms.toFixed(2)} | ${r.p99Ms.toFixed(2)} | ${r.cpuMicrosPerOp.toFixed(1)} | ${r.handlerCpuMicrosPerOp.toFixed(1)} |`,
  )
}
