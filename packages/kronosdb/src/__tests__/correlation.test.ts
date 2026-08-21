/**
 * correlation on the KronosDB data path.
 *
 * The KronosDB command bus REPLACES `@kronos-ts/core`'s default in-memory bus in
 * `components`, and that default bus was the only place
 * `correlationDataDispatchInterceptor` was ever registered. So before the fix,
 * every command an app dispatched over KronosDB left the process with no
 * `correlationId` / `causationId` — while the inbound side faithfully rebuilt a
 * UnitOfWork from `message.metadata` that no longer carried any correlation.
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
  correlating,
  correlatingHandler,
  handlerContext,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  type CommandMessage,
  type QueryMessage,
} from "@kronos-ts/core"
import { kronosDbCommandBus, kronosDbQueryBus } from "../kronosdb.js"
import { metadataFromProto } from "../metadata-conversion.js"
import type { KronosDbConnection } from "../connection.js"
import type { Message, Metadata } from "@kronos-ts/core"

// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

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
      componentName: "correlation-test",
      clientId: "correlation-client",
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
    name: qn("correlation", "Finish"),
    payload: { id: "x" },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

/**
 * Correlation rides on the message: `correlatingHandler` overlays the task's
 * carried map onto what `ctx.send` / `ctx.query` give birth to, BEFORE the bus
 * sees the message, so it is on `metadata` for the local and the remote branch
 * alike. These descriptors are the minimum shape those capabilities read.
 */
const Finish = { name: qn("correlation", "Finish") } as never
const FindThing = { name: qn("correlation", "FindThing") } as never

/**
 * The command a handler is handling — the PARENT of whatever it gives birth to.
 * `correlationFrom` reads its `correlationId` as the chain and its identifier
 * as the cause, which is what the assertions below name.
 */
function causingCommand(): CommandMessage {
  return {
    kind: "command",
    identifier: "cause-1",
    name: qn("correlation", "Start"),
    payload: { id: "x" },
    metadata: { correlationId: "corr-root" },
    timestamp: Date.now(),
  }
}

function queryMessage(): QueryMessage {
  return {
    kind: "query",
    identifier: generateIdentifier(),
    name: qn("correlation", "FindThing"),
    payload: { id: "x" },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

describe("KronosDB distributed command bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched proto", async () => {
    const { connection, commandRequests } = capturingConnection()
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), handleOf(connection))

    const parent = causingCommand()
    const uow = correlating(unitOfWork())
    await uow.execute(async () => {
      const ctx = handlerContext({ uow, commandBus: bus })
      const handler = correlatingHandler(async (_m, c: typeof ctx) => {
        await c.send(Finish, { id: "x" })
      }, correlationFrom)
      await handler(parent, ctx)
    })

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no handler, nothing carrying) untouched", async () => {
    const { connection, commandRequests } = capturingConnection()
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), handleOf(connection))

    await bus.dispatch(commandMessage())

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })
})

describe("KronosDB distributed query bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { connection, queryRequests } = capturingConnection()
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), handleOf(connection))

    const parent = causingCommand()
    const uow = correlating(unitOfWork())
    await uow.execute(async () => {
      const ctx = handlerContext({ uow, queryBus: bus })
      const handler = correlatingHandler(async (_m, c: typeof ctx) => {
        await c.query(FindThing, { id: "x" })
      }, correlationFrom)
      await handler(parent, ctx)
    })

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })
})
