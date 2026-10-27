/**
 * What a KronosDB CLIENT costs per append — the measurement, not the server.
 *
 * `runCell` drives one `EventStore.append` shape in a closed loop (`concurrency`
 * callers, each starting its next append when the last one answers) and reports
 * what it cost THIS process: operations per second, p50 and p99 latency, and
 * client CPU per operation from `process.cpuUsage` (user + system, every thread
 * the runtime owns — GC, the libuv pool and whatever the transport spawns).
 *
 * The harness is runtime-neutral: it imports only the framework and takes the
 * client under test as a function, so the same file measures the native client
 * (`native.ts`) and, from a tree that still has it, the gRPC one.
 *
 * Server latency is in the numbers: with the default durable acks, concurrency 1
 * waits on an fsync per append. CPU per operation is the figure that isolates
 * the client; `--ack-mode written` on the server takes fsync out of the rest.
 */
import { jsonSerializer, qn, type EventMessage, type EventStore, type Serializer } from "@kronos-ts/core"

export type BenchClient = {
  readonly store: EventStore
  close(): Promise<void>
}

export type Connect = (host: string, port: number, serializer: Serializer) => Promise<BenchClient>

export type CellResult = {
  readonly runtime: string
  readonly client: string
  readonly concurrency: number
  readonly batch: number
  readonly ops: number
  readonly opsPerSecond: number
  readonly eventsPerSecond: number
  readonly p50Ms: number
  readonly p99Ms: number
  readonly cpuMicrosPerOp: number
  readonly cpuMicrosPerEvent: number
}

const runtimeName = (): string => (process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.versions.node}`)

function eventSource(run: string): () => EventMessage {
  let counter = 0
  const name = qn("bench", "OrderPlaced")
  return () => ({
    kind: "event",
    identifier: `${run}-${counter++}`,
    name,
    version: "1",
    payload: { orderId: `order-${counter}`, customer: "customer-42", lines: [{ sku: "A-1", qty: 2 }, { sku: "B-7", qty: 1 }], total: 129.5 },
    metadata: { correlationId: "c-1", causationId: "c-0", actor: "bench" },
    timestamp: Date.now(),
    tags: [
      { key: "orderId", value: `order-${counter}` },
      { key: "customer", value: "customer-42" },
    ],
  })
}

function percentile(sorted: Float64Array, p: number): number {
  return sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]!
}

export async function runCell(
  client: BenchClient,
  options: { name: string; concurrency: number; batch: number; seconds: number; warmupSeconds: number },
): Promise<CellResult> {
  const next = eventSource(`${options.name}-${Date.now().toString(36)}`)
  const shape = (): EventMessage[] => {
    const events = new Array<EventMessage>(options.batch)
    for (let i = 0; i < options.batch; i++) events[i] = next()
    return events
  }

  async function drive(seconds: number, record: boolean): Promise<{ ops: number; latencies: Float64Array; cpu: number; wallMs: number }> {
    const deadline = performance.now() + seconds * 1000
    let latencies = new Float64Array(1 << 16)
    let ops = 0
    const worker = async () => {
      while (performance.now() < deadline) {
        const events = shape()
        const began = performance.now()
        await client.store.append(events)
        if (record) {
          if (ops === latencies.length) {
            const grown = new Float64Array(latencies.length * 2)
            grown.set(latencies)
            latencies = grown
          }
          latencies[ops] = performance.now() - began
        }
        ops++
      }
    }
    const cpuBefore = process.cpuUsage()
    const wallBefore = performance.now()
    await Promise.all(Array.from({ length: options.concurrency }, worker))
    const wallMs = performance.now() - wallBefore
    const cpu = process.cpuUsage(cpuBefore)
    return { ops, latencies: latencies.subarray(0, ops), cpu: cpu.user + cpu.system, wallMs }
  }

  await drive(options.warmupSeconds, false)
  const measured = await drive(options.seconds, true)
  const sorted = Float64Array.from(measured.latencies).sort()
  const perSecond = measured.ops / (measured.wallMs / 1000)

  return {
    runtime: runtimeName(),
    client: options.name,
    concurrency: options.concurrency,
    batch: options.batch,
    ops: measured.ops,
    opsPerSecond: perSecond,
    eventsPerSecond: perSecond * options.batch,
    p50Ms: percentile(sorted, 0.5),
    p99Ms: percentile(sorted, 0.99),
    cpuMicrosPerOp: measured.cpu / measured.ops,
    cpuMicrosPerEvent: measured.cpu / measured.ops / options.batch,
  }
}

/** The entry point a client's file calls: parse argv, connect, measure one cell, print one JSON line. */
export async function main(name: string, connect: Connect): Promise<void> {
  const args = new Map<string, string>()
  const argv = process.argv.slice(2)
  for (let i = 0; i + 1 < argv.length; i += 2) args.set(argv[i]!.replace(/^--/, ""), argv[i + 1]!)
  const number = (key: string, fallback: number) => Number(args.get(key) ?? fallback)

  const client = await connect(args.get("host") ?? "127.0.0.1", number("port", 50051), jsonSerializer())
  try {
    const result = await runCell(client, {
      name,
      concurrency: number("concurrency", 1),
      batch: number("batch", 1),
      seconds: number("seconds", 4),
      warmupSeconds: number("warmup", 1.5),
    })
    console.log(JSON.stringify(result))
  } finally {
    await client.close()
  }
}
