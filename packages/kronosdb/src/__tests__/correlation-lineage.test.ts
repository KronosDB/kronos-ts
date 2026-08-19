/**
 * Correlation lineage on the KronosDB data path.
 *
 * The KronosDB command bus REPLACES `@kronos-ts/core`'s default in-memory bus in
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
} from "@kronos-ts/core"
import {
  handlerContext,
  simpleCommandBus,
  simpleQueryBus,
  unitOfWork,
  type CommandMessage,
  type QueryMessage,
} from "@kronos-ts/core"
import { kronosDbCommandBus, kronosDbQueryBus } from "../kronosdb.js"
import { metadataFromProto } from "../metadata-conversion.js"
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

/**
 * The connection HANDLE the buses take. Only the three fields they read are
 * here — the serializer, the live connection, and the latch registry the
 * handle keeps so `close()` can drain every bus opened on it.
 */
function handleOf(connection: KronosDbConnection) {
  return { connection, serializer: jsonSerializer, registerShutdownLatch() {} }
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

/**
 * Lineage now rides on the message: `ctx.send` / `ctx.query` stamp the unit of
 * work's correlation data BEFORE the bus sees the message, so it is on
 * `metadata` for the local and the remote branch alike. These descriptors are
 * the minimum shape those capabilities read.
 */
const Finish = { name: qn("lineage", "Finish") } as never
const FindThing = { name: qn("lineage", "FindThing") } as never

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
    const bus = kronosDbCommandBus(handleOf(connection), simpleCommandBus(unitOfWork))

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no handler, no lineage) untouched", async () => {
    const { connection, commandRequests } = capturingConnection()
    const bus = kronosDbCommandBus(handleOf(connection), simpleCommandBus(unitOfWork))

    await bus.dispatch(commandMessage())

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })
})

describe("KronosDB distributed query bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { connection, queryRequests } = capturingConnection()
    const bus = kronosDbQueryBus(handleOf(connection), simpleQueryBus(unitOfWork))

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, queryBus: bus }).query(FindThing, { id: "x" })
    })

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })
})
