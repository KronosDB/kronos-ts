// What a bus test needs around a fake hub: a connection to it, the host a bus
// is built against, and a way to be a second client.

import { jsonSerializer, type Serializer } from "@kronos-ts/core"
import { connectToKronosDb, type KronosDbConnection, type KronosDbConnectionConfig } from "../connection.js"
import type { BusHost } from "../kronosdb.js"
import type { ShutdownLatch } from "../shutdown-latch.js"
import { FLAG_END, FrameKind, decodeSubscriptionQueryEvent, encodeCredit, encodeOpenSubscriptionQuery, reader } from "../protocol/kronosdb-protocol.js"
import { fakeBusServer, type FakeBusOptions, type FakeBusServer } from "./fake-bus-server.js"

export type Client = {
  readonly connection: KronosDbConnection
  readonly host: BusHost
  /** Latches buses registered on this client, in order. */
  readonly latches: ShutdownLatch[]
  /** What each bus asked `start()` to wait for. */
  readonly readiness: Array<() => Promise<void>>
  /** The latest latch a bus registered. */
  readonly latch: ShutdownLatch
}

export function clientOf(
  server: FakeBusServer,
  config: Partial<KronosDbConnectionConfig> = {},
  serializer: Serializer = jsonSerializer(),
): Client {
  const connection = connectToKronosDb({
    host: "127.0.0.1",
    port: server.port,
    componentName: "bus-test",
    reconnectIntervalMs: 5,
    connectTimeoutMs: 2000,
    ...config,
  })
  const latches: ShutdownLatch[] = []
  const readiness: Array<() => Promise<void>> = []
  return {
    connection,
    latches,
    readiness,
    get latch() {
      return latches.at(-1)!
    },
    host: {
      connection,
      serializer,
      registerShutdownLatch: (latch) => void latches.push(latch),
      registerReadiness: (acknowledged) => void readiness.push(acknowledged),
    },
  }
}

/** The server's end of the connection `client` made (the latest, if it reconnected). */
export function serverSide(server: FakeBusServer, client: Client) {
  const mine = server.connections.filter((c) => c.hello.clientId === client.connection.config.clientId)
  if (mine.length === 0) throw new Error("that client has not connected")
  return mine.at(-1)!
}

export type Rig = {
  readonly server: FakeBusServer
  /** The client under test. */
  readonly client: Client
  /** Another client, to be the remote side of a conversation. */
  another(config?: Partial<KronosDbConnectionConfig>, serializer?: Serializer): Client
  close(): Promise<void>
}

/** A fake hub, and a client connected to it. `close` stops everything started. */
export async function rig(options: { server?: FakeBusOptions; client?: Partial<KronosDbConnectionConfig> } = {}): Promise<Rig> {
  const server = await fakeBusServer(options.server)
  const clients: Client[] = []
  const make = (config?: Partial<KronosDbConnectionConfig>, serializer?: Serializer) => {
    const created = clientOf(server, config, serializer)
    clients.push(created)
    return created
  }
  const client = make(options.client)
  return {
    server,
    client,
    another: make,
    async close() {
      for (const c of clients) {
        for (const latch of c.latches) void latch.initiateShutdown()
        c.connection.close()
      }
      await server.close()
    },
  }
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void
  let reject!: (error: Error) => void
  const promise = new Promise<T>((a, b) => {
    resolve = a
    reject = b
  })
  return { promise, resolve, reject }
}

export const tick = () => new Promise<void>((r) => setTimeout(r, 0))

/** Poll until `probe` is true. */
export async function until(probe: () => boolean, what = "condition", timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (!probe()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`)
    await new Promise((r) => setTimeout(r, 2))
  }
}

/**
 * A subscriber that speaks the wire itself, so a test controls its credit and
 * sees every event, whatever the bus would have made of it.
 */
export async function rawSubscription(client: Client, id: string, name: string, payload: unknown, permits: bigint) {
  const transport = await client.connection.native.ready()
  const events: Array<{ kind: string; value: unknown }> = []
  const ended = deferred<Error | undefined>()
  const stream = transport.open(
    FrameKind.OpenSubscriptionQuery,
    (w) =>
      encodeOpenSubscriptionQuery(w, {
        bus: 0,
        subscription: {
          subscriptionId: id,
          permits,
          query: {
            messageId: id,
            name,
            timestamp: 1,
            metadata: [],
            instructions: [],
            clientId: "raw",
            componentName: "raw",
            payload: { typeName: name, revision: "", data: new TextEncoder().encode(JSON.stringify(payload)) },
          },
        },
      }),
    false,
    (_kind, flags, buf, start, end) => {
      const { event } = decodeSubscriptionQueryEvent(reader(buf, start, end))
      events.push({
        kind: event.kind,
        value: event.kind === "update" && event.value.payload ? JSON.parse(new TextDecoder().decode(event.value.payload.data)) : undefined,
      })
      if ((flags & FLAG_END) !== 0) ended.resolve(undefined)
    },
    (error) => ended.resolve(error),
  )
  return {
    events,
    ended: ended.promise,
    credit: (n: number) => stream.send(FrameKind.Credit, (w) => encodeCredit(w, { credits: n })),
    cancel: () => stream.cancel(),
  }
}
