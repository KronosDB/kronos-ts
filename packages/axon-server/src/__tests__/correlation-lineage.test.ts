/**
 * Correlation lineage on the Axon Server data path.
 *
 * Axon Server is a smart hub: `dispatch` ALWAYS goes to the server, so there is
 * no local branch to accidentally preserve the chain. Before interception was
 * lifted outside the transport, an Axon-backed service therefore lost
 * `correlationId` / `causationId` on EVERY command — the only wrap with
 * `interceptingCommandBus(bus, lineage)` lived inside core's in-memory default
 * bus, which these buses replace wholesale.
 *
 * These tests assert on the PROTO REQUEST handed to the gRPC client — the only
 * place the old wiring could be caught out.
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
  interceptingCommandBus,
  interceptingQueryBus,
  lineage,
  simpleCommandBus,
  simpleQueryBus,
  unitOfWork,
  type CommandBus,
  type CommandMessage,
  type QueryBus,
  type QueryMessage,
} from "@kronos-ts/core"
import { axonServerCommandBus, axonServerQueryBus } from "../axon-server.js"
import { metadataFromProto } from "../metadata-conversion.js"
import { shutdownLatch } from "../shutdown-latch.js"
import type { AxonServerBusSource, AxonServerConnection } from "../connection.js"

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

  /** Everything a bus borrows from the shared connection, and nothing else. */
  const source: AxonServerBusSource = {
    connection,
    serializer: jsonSerializer,
    shutdown: shutdownLatch(),
  }

  return { source, commandRequests, queryRequests }
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
 * Lineage rides on the message: `ctx.send` / `ctx.query` stamp the unit of
 * work's correlation data BEFORE the bus sees the message, so it is on
 * `metadata` for the local and the remote branch alike. These descriptors are
 * the minimum shape those capabilities read.
 */
const Finish = { name: qn("lineage", "Finish") } as never
const FindThing = { name: qn("lineage", "FindThing") } as never

/** The app's default local buses: simple bus wrapped for lineage. */
function localCommandBus(): CommandBus {
  return interceptingCommandBus(simpleCommandBus(unitOfWork), lineage)
}

function localQueryBus(): QueryBus {
  return interceptingQueryBus(simpleQueryBus(unitOfWork), lineage)
}

describe("Axon Server command bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched proto", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = axonServerCommandBus(source, localCommandBus())

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no handler, no lineage) untouched", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = axonServerCommandBus(source, localCommandBus())

    await bus.dispatch(commandMessage())

    // The transport bus stamps nothing of its own — that is the interceptor's
    // job, and this bus is unwrapped. A host that wants lineage writes
    // `interceptingCommandBus(axonServerCommandBus(conn, local), lineage)`.
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })

  it("seeds a wrapped PRIMARY dispatch with self-referential correlation/causation", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = interceptingCommandBus(axonServerCommandBus(source, localCommandBus()), lineage)

    const message = commandMessage()
    await bus.dispatch(message)

    // A message born at an edge with no cause at all: `lineage` starts the
    // chain at the message itself, on BOTH fields.
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe(message.identifier)
    expect(onTheWire.causationId).toBe(message.identifier)
  })

  it("preserves a contributed causationId across the hop instead of clobbering it", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = interceptingCommandBus(axonServerCommandBus(source, localCommandBus()), lineage)

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    // `lineage` SEEDS causationId and never clobbers it. This message arrived at
    // the bus with a cause already on it — `ctx.send` stamped the handled
    // message's lineage outward — so "cause-1" crosses the wire intact. The old
    // unconditional `causationId: message.identifier` overwrote it here, and
    // every message in a chain ended up claiming to have caused itself.
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("wrapping twice equals wrapping once", async () => {
    // The evidence behind wrapping the transport bus WITHOUT unwrapping the
    // local segment: both of `lineage`'s fields are `??` seeds, so the second
    // application finds them already set and changes nothing. That matters now
    // that `local` is a real bus which may itself be intercepting — a
    // server-routed command can legitimately meet `lineage` twice.
    const once = capturingConnection()
    const twice = capturingConnection()

    async function send(bus: CommandBus) {
      await unitOfWork().execute(async (uow) => {
        uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
        await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
      })
    }

    await send(
      interceptingCommandBus(axonServerCommandBus(once.source, localCommandBus()), lineage),
    )
    await send(
      interceptingCommandBus(
        interceptingCommandBus(axonServerCommandBus(twice.source, localCommandBus()), lineage),
        lineage,
      ),
    )

    // Each invocation mints its own Finish identifier, so the two protos are
    // not literally identical — what "twice equals once" means is that BOTH
    // obey the same rule: correlationId is the contributed root, and causationId
    // is the contributed cause, preserved rather than re-derived.
    for (const requests of [once.commandRequests, twice.commandRequests]) {
      const onTheWire: Metadata = metadataFromProto(requests[0]!.metaData)
      expect(onTheWire.correlationId).toBe("corr-root")
      expect(onTheWire.causationId).toBe("cause-1")
    }
  })
})

describe("Axon Server query bus — correlation lineage", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { source, queryRequests } = capturingConnection()
    const bus = axonServerQueryBus(source, localQueryBus())

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, queryBus: bus }).query(FindThing, { id: "x" })
    })

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("stamps lineage on the LOCAL-SHORTCUT branch too", async () => {
    // shortcutQueriesToLocalHandlers routes to a co-located handler instead of
    // the server. Because interception is outside the routing decision, both
    // branches see identical metadata — the AF property this is modelled on.
    // The handler is reached THROUGH the local bus, which is where it was
    // subscribed; the shortcut only decides whether the server is consulted.
    const { source, queryRequests } = capturingConnection()
    const bus = axonServerQueryBus(source, localQueryBus(), {
      shortcutQueriesToLocalHandlers: true,
    })

    let seen: Metadata | undefined
    bus.subscribe("lineage.FindThing", async (message: QueryMessage) => {
      seen = message.metadata
      return null
    })

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, queryBus: bus }).query(FindThing, { id: "x" })
    })

    expect(queryRequests).toHaveLength(0)
    expect(seen?.correlationId).toBe("corr-root")
    expect(seen?.causationId).toBe("cause-1")
  })
})
