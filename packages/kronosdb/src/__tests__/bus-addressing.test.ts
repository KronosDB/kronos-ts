/**
 * BUSES ARE NAMED, CONTEXTS ARE NOT INVOLVED (server ADR-0006).
 *
 * Every messaging RPC — the handler streams, dispatch, query, subscription
 * queries — carries the per-call `kronosdb-bus` header and NEVER
 * `kronosdb-context`: the server routes messaging by bus name with no fallback
 * to context, so a context header here would be a lie waiting for a reader.
 * The store plane is the mirror image and keeps `kronosdb-context`.
 */
import { describe, expect, it, afterEach } from "bun:test"
import { localCommandBus, localQueryBus, unitOfWork, jsonSerializer } from "@kronos-ts/core"
import { busMetadata, kronosMetadata, type KronosDbConnection } from "../connection.js"
import { kronosDbCommandBus, kronosDbQueryBus } from "../kronosdb.js"
import { shutdownLatch } from "../shutdown-latch.js"

describe("busMetadata — the messaging-plane header", () => {
  it("sets kronosdb-bus and never kronosdb-context", () => {
    const metadata = busMetadata("orders", { token: "" })
    expect(metadata.get("kronosdb-bus")).toBe("orders")
    expect(metadata.get("kronosdb-context")).toBeUndefined()
    expect(metadata.get("kronosdb-token")).toBeUndefined()
  })

  it("carries the auth token when one is configured", () => {
    const metadata = busMetadata("orders", { token: "secret" })
    expect(metadata.get("kronosdb-token")).toBe("secret")
  })

  it("kronosMetadata stays the store plane: context, no bus", () => {
    const metadata = kronosMetadata({ context: "orders-ctx", token: "" })
    expect(metadata.get("kronosdb-context")).toBe("orders-ctx")
    expect(metadata.get("kronosdb-bus")).toBeUndefined()
  })
})

// ---------------------------------------------------------------------------
// Through the factories: the header on the wire is the `bus` argument, and the
// connection's CONTEXT never leaks onto a messaging call.
// ---------------------------------------------------------------------------

type CapturedCall = { metadata?: { get(key: string): string | undefined } }

function fakeConnection(captured: { commandStream?: CapturedCall; queryStream?: CapturedCall }): KronosDbConnection {
  const never = (async function* () { await new Promise(() => {}) })()
  return {
    channel: {} as any,
    platform: {} as any,
    eventStore: {} as any,
    commands: {
      openStream(_outbound: AsyncIterable<unknown>, options: CapturedCall) {
        captured.commandStream = options
        return never
      },
    } as any,
    queries: {
      openStream(_outbound: AsyncIterable<unknown>, options: CapturedCall) {
        captured.queryStream = options
        return never
      },
    } as any,
    config: {
      host: "localhost", port: 50051,
      context: "orders-ctx",             // ← deliberately NOT the bus name
      componentName: "test-component", clientId: "test-client", token: "",
      reconnectIntervalMs: 0, maxReconnectAttempts: 0,
      keepAliveTimeMs: 0, keepAliveTimeoutMs: 0, keepAlivePermitWithoutCalls: false,
    },
    state: "connected",
    onReconnect() {}, onDisconnect() {}, close() {}, reconnect: async () => {},
  } as unknown as KronosDbConnection
}

describe("kronosDb buses — bus-name addressing on the wire", () => {
  let latch = shutdownLatch()
  afterEach(() => latch.initiateShutdown())

  function handleOf(connection: KronosDbConnection) {
    return {
      connection,
      serializer: jsonSerializer,
      registerShutdownLatch: (l: ReturnType<typeof shutdownLatch>) => { latch = l },
    }
  }

  it("command stream opens with the named bus, not the connection's context", () => {
    const captured: { commandStream?: CapturedCall } = {}
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), handleOf(fakeConnection(captured)), "orders")
    bus.subscribe("test.Cmd", async () => undefined)          // arms the stream
    expect(captured.commandStream?.metadata?.get("kronosdb-bus")).toBe("orders")
    expect(captured.commandStream?.metadata?.get("kronosdb-context")).toBeUndefined()
  })

  it("omitting the bus name means the server's default bus", () => {
    const captured: { commandStream?: CapturedCall } = {}
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), handleOf(fakeConnection(captured)))
    bus.subscribe("test.Cmd", async () => undefined)
    expect(captured.commandStream?.metadata?.get("kronosdb-bus")).toBe("default")
  })

  it("query stream opens with the named bus, not the connection's context", () => {
    const captured: { queryStream?: CapturedCall } = {}
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), handleOf(fakeConnection(captured)), "reads")
    bus.subscribe("test.Query", async () => undefined)
    expect(captured.queryStream?.metadata?.get("kronosdb-bus")).toBe("reads")
    expect(captured.queryStream?.metadata?.get("kronosdb-context")).toBeUndefined()
  })
})
