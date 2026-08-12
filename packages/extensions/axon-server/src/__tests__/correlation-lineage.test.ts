/**
 * Correlation lineage on the Axon Server data path.
 *
 * `axonServer().components` REPLACES `@kronos-ts/app`'s default in-memory buses,
 * and that default command bus was the only place
 * `correlationDataDispatchInterceptor` was ever registered. Before the fix, an
 * Axon-backed service therefore lost `correlationId` / `causationId` on EVERY
 * command — this bus always routes through Axon Server, so there was no local
 * branch to accidentally preserve the chain.
 *
 * These tests assert on the PROTO REQUEST handed to the gRPC client.
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
import { distributedCommandBus, distributedQueryBus } from "../axon-server.js"
import { metadataFromProto } from "../metadata-conversion.js"
import { shutdownLatch } from "../shutdown-latch.js"
import type { AxonServerConnection } from "../connection.js"

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
    events: {} as any,
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
      port: 8124,
      context: "default",
      componentName: "lineage-test",
      clientId: "lineage-client",
      token: "",
    },
    state: "connected",
    onReconnect() {},
    onDisconnect() {},
    close() {},
    reconnect: async () => {},
  } as unknown as AxonServerConnection

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

describe("Axon Server distributed command bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched proto", async () => {
    const { connection, commandRequests } = capturingConnection()
    const bus = distributedCommandBus(
      connection,
      async (_metadata, run) => run(),
      shutdownLatch(),
      jsonSerializer,
    )

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.dispatch(commandMessage())
    })

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no active UnitOfWork) untouched", async () => {
    const { connection, commandRequests } = capturingConnection()
    const bus = distributedCommandBus(
      connection,
      async (_metadata, run) => run(),
      shutdownLatch(),
      jsonSerializer,
    )

    await bus.dispatch(commandMessage())

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })
})

describe("Axon Server distributed query bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { connection, queryRequests } = capturingConnection()
    const bus = distributedQueryBus(
      connection,
      async (_metadata, run) => run(),
      shutdownLatch(),
      jsonSerializer,
    )

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.query(queryMessage())
    })

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("stamps lineage on the LOCAL-SHORTCUT branch too", async () => {
    // shortcutQueriesToLocalHandlers routes to a co-located handler instead of
    // the server. Because interception is outside the routing decision, both
    // branches see identical metadata — the AF property this fix is modelled on.
    const { connection, queryRequests } = capturingConnection()
    const bus = distributedQueryBus(
      connection,
      async (_metadata, run) => run(),
      shutdownLatch(),
      jsonSerializer,
      undefined,
      true,
    )

    let seen: Metadata | undefined
    bus.subscribe("lineage.FindThing", async (message) => {
      seen = message.metadata
      return null
    })

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.query(queryMessage())
    })

    expect(queryRequests).toHaveLength(0)
    expect(seen?.correlationId).toBe("corr-root")
    expect(seen?.causationId).toBe("cause-1")
  })
})
