/**
 * correlation on the Axon Server data path.
 *
 * Axon Server is a smart hub: `dispatch` ALWAYS goes to the server, so there is
 * no local branch to accidentally preserve the chain. Before interception was
 * lifted outside the transport, an Axon-backed service therefore lost
 * `correlationId` / `causationId` on EVERY command — the only wrap with
 * `interceptingCommandBus(bus, correlation)` lived inside core's in-memory default
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
  correlating,
  correlatingHandler,
  handlerContext,
  interceptingCommandBus,
  interceptingQueryBus,
  correlation,
  localCommandBus,
  localQueryBus,
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
      componentName: "correlation-test",
      clientId: "correlation-client",
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
 * Its `correlationId` is the chain; its IDENTIFIER is the cause. Every
 * assertion below that names "cause-1" is naming this message's identifier,
 * which is the hop rule: a child is caused by its parent, always.
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

/** One handler invocation that gives birth to a `Finish` command through `bus`. */
async function sendFinishFrom(bus: CommandBus): Promise<void> {
  const uow = correlating(unitOfWork())
  await uow.execute(async () => {
    const ctx = handlerContext({ uow, commandBus: bus })
    const handler = correlatingHandler(async (_m, c: typeof ctx) => {
      await c.send(Finish, { id: "x" })
    }, correlationFrom)
    await handler(causingCommand(), ctx)
  })
}

/** The app's default local buses: the local bus wrapped for correlation. */
function correlatedCommandBus(): CommandBus {
  return interceptingCommandBus(localCommandBus(unitOfWork), correlation)
}

function correlatedQueryBus(): QueryBus {
  return interceptingQueryBus(localQueryBus(unitOfWork), correlation)
}

describe("Axon Server command bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched proto", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = axonServerCommandBus(correlatedCommandBus(), source)

    await sendFinishFrom(bus)

    expect(commandRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no handler, nothing carrying) untouched", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = axonServerCommandBus(correlatedCommandBus(), source)

    await bus.dispatch(commandMessage())

    // The transport bus stamps nothing of its own — that is the intercept's
    // job, and this bus is unwrapped. A host that wants correlation writes
    // `interceptingCommandBus(axonServerCommandBus(local, conn), correlation)`.
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })

  it("seeds a wrapped PRIMARY dispatch with self-referential correlation/causation", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = interceptingCommandBus(axonServerCommandBus(correlatedCommandBus(), source), correlation)

    const message = commandMessage()
    await bus.dispatch(message)

    // A message born at an edge with no cause at all: `correlation` starts the
    // chain at the message itself, on BOTH fields.
    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe(message.identifier)
    expect(onTheWire.causationId).toBe(message.identifier)
  })

  it("preserves the carried causationId across the hop instead of clobbering it", async () => {
    const { source, commandRequests } = capturingConnection()
    const bus = interceptingCommandBus(axonServerCommandBus(correlatedCommandBus(), source), correlation)

    await sendFinishFrom(bus)

    const onTheWire: Metadata = metadataFromProto(commandRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    // `correlation` SEEDS causationId and never clobbers it. This message
    // arrived at the bus with a cause already on it — the wrapper overlaid the
    // handled message's identifier onto it — so "cause-1" crosses the wire
    // intact. The old unconditional `causationId: message.identifier` overwrote
    // it here, and every message in a chain ended up claiming to have caused
    // itself.
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("wrapping twice equals wrapping once", async () => {
    // The evidence behind wrapping the transport bus WITHOUT unwrapping the
    // local segment: both of `correlation`'s fields are `??` seeds, so the second
    // application finds them already set and changes nothing. That matters now
    // that `local` is a real bus which may itself be intercepting — a
    // server-routed command can legitimately meet `correlation` twice.
    const once = capturingConnection()
    const twice = capturingConnection()

    await sendFinishFrom(
      interceptingCommandBus(axonServerCommandBus(correlatedCommandBus(), once.source), correlation),
    )
    await sendFinishFrom(
      interceptingCommandBus(
        interceptingCommandBus(axonServerCommandBus(correlatedCommandBus(), twice.source), correlation),
        correlation,
      ),
    )

    // Each invocation mints its own Finish identifier, so the two protos are
    // not literally identical — what "twice equals once" means is that BOTH
    // obey the same rule: correlationId is the carried root, and causationId is
    // the carried cause, preserved rather than re-derived.
    for (const requests of [once.commandRequests, twice.commandRequests]) {
      const onTheWire: Metadata = metadataFromProto(requests[0]!.metaData)
      expect(onTheWire.correlationId).toBe("corr-root")
      expect(onTheWire.causationId).toBe("cause-1")
    }
  })
})

/** One handler invocation that gives birth to a `FindThing` query through `bus`. */
async function askFindThingFrom(bus: QueryBus): Promise<void> {
  const uow = correlating(unitOfWork())
  await uow.execute(async () => {
    const ctx = handlerContext({ uow, queryBus: bus })
    const handler = correlatingHandler(async (_m, c: typeof ctx) => {
      await c.query(FindThing, { id: "x" })
    }, correlationFrom)
    await handler(causingCommand(), ctx)
  })
}

describe("Axon Server query bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched query proto", async () => {
    const { source, queryRequests } = capturingConnection()
    const bus = axonServerQueryBus(correlatedQueryBus(), source)

    await askFindThingFrom(bus)

    expect(queryRequests).toHaveLength(1)
    const onTheWire: Metadata = metadataFromProto(queryRequests[0]!.metaData)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("stamps correlation on the LOCAL-SHORTCUT branch too", async () => {
    // shortcutQueriesToLocalHandlers routes to a co-located handler instead of
    // the server. Because interception is outside the routing decision, both
    // branches see identical metadata — the AF property this is modelled on.
    // The handler is reached THROUGH the local bus, which is where it was
    // subscribed; the shortcut only decides whether the server is consulted.
    const { source, queryRequests } = capturingConnection()
    const bus = axonServerQueryBus(correlatedQueryBus(), source, {
      shortcutQueriesToLocalHandlers: true,
    })

    let seen: Metadata | undefined
    bus.subscribe("correlation.FindThing", async (message: QueryMessage) => {
      seen = message.metadata
      return null
    })

    await askFindThingFrom(bus)

    expect(queryRequests).toHaveLength(0)
    expect(seen?.correlationId).toBe("corr-root")
    expect(seen?.causationId).toBe("cause-1")
  })
})
