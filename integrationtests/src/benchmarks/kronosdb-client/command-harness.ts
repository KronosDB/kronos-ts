/**
 * What a KronosDB CLIENT costs per command — a round trip through the server,
 * from a dispatcher to a handler on another connection and back.
 *
 * Two roles, run as two processes so each side's CPU is its own:
 *
 *   handler      one connection, one `kronosDbCommandBus`, one handler that
 *                answers with its payload. Counts what it handled and reports
 *                its CPU when told to.
 *   dispatcher   one connection, one `kronosDbCommandBus`, a closed loop of
 *                `concurrency` callers each dispatching its next command when
 *                the last one answers. Reports what THIS process cost per
 *                command (process.cpuUsage: user + system, every thread the
 *                runtime owns) with p50/p99 latency and operations per second.
 *
 * The harness is client-neutral: it imports only the framework and takes the
 * client under test as a function, so the same file measures the native client
 * (`commands-native.ts`) and, from a tree that still has it, the gRPC one.
 * Commands do not touch the event store, so the server's ack mode is moot.
 */
import { createInterface } from "node:readline"
import { jsonSerializer, localCommandBus, qn, unitOfWork, type CommandBus, type CommandMessage, type Serializer } from "@kronos-ts/core"

export type CommandClient = {
  readonly bus: CommandBus
  /** Wait until the server has acknowledged this client's handlers. */
  ready(): Promise<void>
  close(): Promise<void>
}

export type ConnectCommands = (host: string, port: number, serializer: Serializer, role: "handler" | "dispatcher") => Promise<CommandClient>

export type CommandCell = {
  readonly runtime: string
  readonly client: string
  readonly concurrency: number
  readonly ops: number
  readonly opsPerSecond: number
  readonly p50Ms: number
  readonly p99Ms: number
  /** Dispatcher process CPU per command. */
  readonly cpuMicrosPerOp: number
  /** Handler process CPU per command, over the whole run (warm-up included). */
  readonly handlerCpuMicrosPerOp: number
}

const runtimeName = (): string => (process.versions.bun ? `bun ${process.versions.bun}` : `node ${process.versions.node}`)

const name = qn("bench", "Ping")
const NAME = `${name.namespace}.${name.name}`

function percentile(sorted: Float64Array, p: number): number {
  return sorted.length === 0 ? 0 : sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))]!
}

async function handlerRole(client: CommandClient): Promise<void> {
  let handled = 0
  client.bus.subscribe(NAME, async (message) => {
    handled++
    return message.payload
  })
  await client.ready()
  const cpuBefore = process.cpuUsage()
  console.log("READY")
  for await (const line of createInterface({ input: process.stdin })) {
    if (line.trim() !== "report") continue
    const cpu = process.cpuUsage(cpuBefore)
    console.log(JSON.stringify({ handled, cpuMicros: cpu.user + cpu.system }))
    break
  }
  await client.close()
}

async function dispatcherRole(
  client: CommandClient,
  options: { name: string; concurrency: number; seconds: number; warmupSeconds: number },
): Promise<Omit<CommandCell, "handlerCpuMicrosPerOp">> {
  let counter = 0
  const run = Date.now().toString(36)
  const next = (): CommandMessage => ({
    kind: "command",
    identifier: `${run}-${counter++}`,
    name,
    payload: { orderId: `order-${counter}`, customer: "customer-42", lines: [{ sku: "A-1", qty: 2 }, { sku: "B-7", qty: 1 }], total: 129.5 },
    metadata: { correlationId: "c-1", causationId: "c-0", actor: "bench" },
    timestamp: Date.now(),
  })

  async function drive(seconds: number, record: boolean) {
    const deadline = performance.now() + seconds * 1000
    let latencies = new Float64Array(1 << 16)
    let ops = 0
    const worker = async () => {
      while (performance.now() < deadline) {
        const message = next()
        const began = performance.now()
        await client.bus.dispatch(message)
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

  // The handler's registration is acknowledged before the loop starts, but the
  // first dispatches still pay for connecting and binding the bus.
  await drive(options.warmupSeconds, false)
  const measured = await drive(options.seconds, true)
  const sorted = Float64Array.from(measured.latencies).sort()
  return {
    runtime: runtimeName(),
    client: options.name,
    concurrency: options.concurrency,
    ops: measured.ops,
    opsPerSecond: measured.ops / (measured.wallMs / 1000),
    p50Ms: percentile(sorted, 0.5),
    p99Ms: percentile(sorted, 0.99),
    cpuMicrosPerOp: measured.cpu / measured.ops,
  }
}

/** The entry point a client's file calls: parse argv, connect, play one role, print one JSON line. */
export async function main(clientName: string, connect: ConnectCommands): Promise<void> {
  const args = new Map<string, string>()
  const argv = process.argv.slice(2)
  for (let i = 0; i + 1 < argv.length; i += 2) args.set(argv[i]!.replace(/^--/, ""), argv[i + 1]!)
  const number = (key: string, fallback: number) => Number(args.get(key) ?? fallback)
  const role = (args.get("role") ?? "dispatcher") as "handler" | "dispatcher"

  const client = await connect(args.get("host") ?? "127.0.0.1", number("port", 50051), jsonSerializer(), role)
  if (role === "handler") {
    await handlerRole(client)
    return
  }
  try {
    const result = await dispatcherRole(client, {
      name: clientName,
      concurrency: number("concurrency", 1),
      seconds: number("seconds", 4),
      warmupSeconds: number("warmup", 1.5),
    })
    console.log(JSON.stringify(result))
  } finally {
    await client.close()
  }
}

/** Builds the bus every client shares: the local segment is the in-process one. */
export const localBus = () => localCommandBus(unitOfWork)
