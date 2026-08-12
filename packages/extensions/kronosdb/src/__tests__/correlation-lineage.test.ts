/**
 * Correlation lineage on the KronosDB data path.
 *
 * The KronosDB command bus REPLACES `@kronos-ts/app`'s default in-memory bus in
 * `components`, and that default bus was the only place
 * `correlationDataDispatchInterceptor` was ever registered. So before the fix,
 * every command an app dispatched over KronosDB left the process with no
 * `correlationId` / `causationId` — while the inbound side faithfully rebuilt a
 * UnitOfWork from `message.metadata` that no longer carried any lineage.
 *
 * These tests assert on the PROTO REQUEST handed to the gRPC client, i.e. the
 * bytes that actually leave the process.
 */
import { describe, expect, it } from "bun:test"
import {
  emptyMetadata,
  generateIdentifier,
  qn,
  type Metadata,
  type SerializedObject,
  type Serializer,
} from "@kronos-ts/common"
import {
  contributeCorrelationData,
  runInNewUoW,
  type CommandMessage,
  type QueryMessage,
} from "@kronos-ts/messaging"
import { distributedCommandBus, distributedQueryBus } from "../kronosdb.js"
import { metadataFromProto } from "../metadata-conversion.js"
import { shutdownLatch } from "../shutdown-latch.js"
import type { KronosDbConnection } from "../connection.js"

const jsonSerializer: Serializer = {
  serialize(value, type, revision = ""): SerializedObject {
    return { type, revision, data: new TextEncoder().encode(JSON.stringify(value)) }
  },
  deserialize<T>({ data }: SerializedObject): T {
    return JSON.parse(new TextDecoder().decode(data)) as T
  },
  canConvert() {
    return true
  },
}

/** Captures the proto requests that reach the wire. */
function capturingConnection() {
  const commandRequests: any[] = []
  const queryRequests: any[] = []

  const connection = {
    channel: {} as any,
    platform: {} as any,
    snapshotStore: {} as any,
    eventStore: {} as any,
    commands: {
      openStream() {
        return (async function* () {})()
      },
      async dispatch(request: any) {
        commandRequests.push(request)
        return { errorCode: "", payload: jsonSerializer.serialize(null, "null", "") }
      },
    } as any,
    queries: {
      openStream() {
        return (async function* () {})()
      },
      query(request: any) {
        queryRequests.push(request)
        return (async function* () {
          yield { errorCode: "", payload: jsonSerializer.serialize(null, "null", "") }
        })()
      },
    } as any,
    config: {
      host: "localhost",
      port: 50051,
      context: "default",
      componentName: "lineage-test",
      clientId: "lineage-client",
      token: "",
      reconnectIntervalMs: 0,
      maxReconnectAttempts: 0,
      keepAliveTimeMs: 0,
      keepAliveTimeoutMs: 0,
      keepAlivePermitWithoutCalls: false,
    },
    state: "connected",
    onReconnect() {},
    onDisconnect() {},
    close() {},
    reconnect: async () => {},
  } as unknown as KronosDbConnection

  return { connection, commandRequests, queryRequests }
}

function commandMessage(): CommandMessage {
  return {
    kind: "command",
    identifier: generateIdentifier(),
    name: qn("lineage", "Finish"),
    payload: { id: "x" },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

function queryMessage(): QueryMessage {
  return {
    kind: "query",
    identifier: generateIdentifier(),
    name: qn("lineage", "FindThing"),
    payload: { id: "x" },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

describe("KronosDB distributed command bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched proto", async () => {
    const { connection, commandRequests } = capturingConnection()
    const latch = shutdownLatch()
    const bus = distributedCommandBus(
      connection,
      async (_metadata, run) => run(),
      latch,
      jsonSerializer,
    )

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.dispatch(commandMessage())
    })

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no active UnitOfWork) untouched", async () => {
    const { connection, commandRequests } = capturingConnection()
    const latch = shutdownLatch()
    const bus = distributedCommandBus(
      connection,
      async (_metadata, run) => run(),
      latch,
      jsonSerializer,
    )

    await bus.dispatch(commandMessage())

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })
})

describe("KronosDB distributed query bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { connection, queryRequests } = capturingConnection()
    const latch = shutdownLatch()
    const bus = distributedQueryBus(
      connection,
      async (_metadata, run) => run(),
      latch,
      jsonSerializer,
    )

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.query(queryMessage())
    })

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })
})
