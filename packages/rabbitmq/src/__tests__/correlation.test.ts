/**
 * correlation across the DISTRIBUTED dispatch boundary.
 *
 * The regression this pins: `interceptingCommandBus` used to be
 * registered ONLY inside `@kronos-ts/core`'s default in-memory command bus. The
 * distributed bus wraps that bus as its local segment and routes each command to
 * EITHER the local segment (interceptor present) OR the broker transport
 * (interceptor absent) — so a remotely-routed command left the process with no
 * `correlationId` / `causationId` at all, and the chain died at the process
 * edge.
 *
 * The fix follows AxonFramework: dispatch interception sits OUTSIDE the routing
 * decision (AF4 `AxonServerCommandBus.dispatch` =
 * `doDispatch(dispatchInterceptors.intercept(cmd), cb)`; AF5 stacks
 * `InterceptingCommandBus → DistributedCommandBus → LocalCommandBus` by
 * decorator order). These tests assert against the envelope that reaches the
 * TRANSPORT, which is the only place the old wiring could be caught out.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  emptyMetadata,
  generateIdentifier,
  qn,
  send,
  type Metadata,
  type QualifiedName,
  type UnitOfWork,
} from "@kronos-ts/core"
import { kronos } from "@kronos-ts/core"
import { inMemoryEventStore } from "@kronos-ts/core"
import {
  command,
  commandHandler,
  correlating,
  correlatingHandler,
  commandHandlerContext,
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
  type CommandBus,
  type CommandMessage,
} from "@kronos-ts/core"
import {
  rabbitMqCommandBus,
  type RabbitMqCommandEnvelope,
  type RabbitMqCommandReplyEnvelope,
  type RabbitMqCommandTransport,
} from "../command-bus.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"
import type { Message, Metadata } from "@kronos-ts/core"

// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `localCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow = () => correlating(unitOfWork())) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}


const Start = command({
  name: qn("correlation", "Start"),
  payload: z.object({ id: z.string() }),
})

const Finish = command({
  name: qn("correlation", "Finish"),
  payload: z.object({ id: z.string() }),
})

/**
 * A transport that records every envelope handed to it and then loops it back to
 * the subscribed handler — so a test can assert both what crossed the "wire" and
 * what the far side received.
 */
type RecordingLoopbackTransport = RabbitMqCommandTransport & {
  readonly envelopes: RabbitMqCommandEnvelope[]
  /** Metadata of the nth envelope that reached the transport. */
  metadataAt(index: number): Metadata
}

function recordingLoopbackTransport(): RecordingLoopbackTransport {
  const envelopes: RabbitMqCommandEnvelope[] = []
  const handlers = new Map<
    string,
    (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>
  >()

  return {
    envelopes,

    async dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope> {
      envelopes.push(envelope)
      const key = `${envelope.message.name.namespace}.${envelope.message.name.name}`
      const handler = handlers.get(key)
      if (!handler) return { requestId: envelope.requestId, ok: true, result: undefined }
      return handler(envelope)
    },

    subscribe(
      commandName: string,
      handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
    ): void {
      handlers.set(commandName, handler)
    },

    metadataAt(index: number): Metadata {
      const envelope = envelopes[index]
      if (!envelope) throw new Error(`No envelope at index ${index}`)
      return envelope.message.metadata
    },
  }
}

/**
 * The composition this file exists to pin: correlation wraps the TRANSPORT
 * bus, which then forks between the local segment and the broker. Both
 * branches are therefore downstream of the same single interception.
 */
function busOver(
  transport: RabbitMqCommandTransport,
  localSegment: CommandBus,
  options?: { preferLocalHandlers?: boolean },
): CommandBus {
  const preferLocal = options?.preferLocalHandlers ?? false
  return interceptingCommandBus(
    rabbitMqCommandBus(
      localSegment,
      {
        config: resolveRabbitMqConfig({
          identity: { serviceName: "correlation-test", instanceId: "inst-1" },
          url: "amqp://loopback",
        }),
        commandTransport: transport,
      },
      { preferLocal },
    ),
    correlation,
  )
}

/** The app's default command bus: simple bus wrapped for correlation. */
function defaultLocalBus(): CommandBus {
  return interceptingCommandBus(localCommandBus(unitOfWork), correlation)
}

function commandMessage(name: QualifiedName, payload: unknown): CommandMessage {
  return {
    kind: "command",
    identifier: generateIdentifier(),
    name,
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

/**
 * The command a handler is handling — the PARENT of whatever it gives birth to.
 * Its `correlationId` is the chain; its IDENTIFIER is the cause. "cause-1"
 * below is this message's identifier, not a value anybody contributed: a child
 * is caused by its parent, always.
 */
function causingCommand(): CommandMessage {
  return {
    kind: "command",
    identifier: "cause-1",
    name: Start.name,
    payload: { id: "x" },
    metadata: { correlationId: "corr-root" },
    timestamp: Date.now(),
  }
}

/**
 * One handler invocation that gives birth to a `Finish` command through `bus`.
 * `attach` is the mid-handling hook the precedence test uses.
 */
async function sendFinishFrom(
  bus: CommandBus,
  attach?: (uow: { attachCorrelationData(p: Record<string, string>): void }) => void,
): Promise<void> {
  const uow = correlating(unitOfWork())
  await uow.execute(async () => {
    const ctx = commandHandlerContext({ uow, commandBus: bus })
    const handler = correlatingHandler(async (_m, c: typeof ctx) => {
      attach?.(c.unitOfWork)
      await c.send(Finish, { id: "x" })
    }, correlationFrom)
    await handler(causingCommand(), ctx)
  })
}

describe("RabbitMQ command bus — correlation reaches the transport", () => {
  it("stamps correlationId/causationId on a command routed to the BROKER", async () => {
    const transport = recordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    // A real handler invocation: the wrapper attaches the handled message's
    // cargo to the task, and `ctx.send` carries it onto the new message before
    // any bus sees it.
    await sendFinishFrom(bus)

    expect(transport.envelopes).toHaveLength(1)
    expect(transport.metadataAt(0).correlationId).toBe("corr-root")
    // `correlation` SEEDS causationId and never clobbers it. This message
    // arrived at the bus with a cause already on it — the parent's identifier,
    // overlaid by the wrapper — so "cause-1" crosses the wire intact. The old
    // unconditional `causationId: message.identifier` overwrote it here, and
    // every message in a chain ended up claiming to have caused itself.
    expect(transport.metadataAt(0).causationId).toBe("cause-1")
  })

  it("stamps a PRIMARY dispatch (no prior correlation) with self-referential correlation/causation", async () => {
    const transport = recordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    const message = commandMessage(Finish.name, { id: "x" })
    await bus.dispatch(message)

    // `correlation` runs unconditionally at the bus edge: a message with no
    // correlationId of its own starts a new chain at itself, and causationId
    // is always its own identifier too.
    expect(transport.metadataAt(0).correlationId).toBe(message.identifier)
    expect(transport.metadataAt(0).causationId).toBe(message.identifier)
  })

  it("lets a LATER attach win over the cargo the wrapper put on the task", async () => {
    const transport = recordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    // The map is read PER CALL, not captured at wrap time, so a handler that
    // attaches mid-handling — a `traceparent`, a corrected chain id — has it on
    // the next birth. Later keys win over earlier ones.
    await sendFinishFrom(bus, (uow) => {
      uow.attachCorrelationData({ correlationId: "from-uow" })
    })

    expect(transport.metadataAt(0).correlationId).toBe("from-uow")
    // …and what it did not override is still the wrapper's cargo.
    expect(transport.metadataAt(0).causationId).toBe("cause-1")
  })

  it("wrapping twice equals wrapping once", async () => {
    // The evidence behind wrapping the distributed bus WITHOUT unwrapping the
    // local segment: `correlation` re-derives correlationId/causationId from the
    // MESSAGE ITSELF (its own metadata.correlationId, if any, and its own
    // identifier) — never from anything a previous wrap layer invented — so
    // stacking the wrapper on the SAME message is a no-op past the first
    // application.
    async function seenBy(
      bus: (delegate: CommandBus) => CommandBus,
    ): Promise<CommandMessage | undefined> {
      let seen: CommandMessage | undefined
      const inner: CommandBus = {
        async dispatch(message) { seen = message; return undefined },
        subscribe() {},
      }
      await sendFinishFrom(bus(inner))
      return seen
    }

    const once = await seenBy((d) => interceptingCommandBus(d, correlation))
    const twice = await seenBy((d) => interceptingCommandBus(interceptingCommandBus(d, correlation), correlation))

    // Each invocation mints its own Finish identifier, so the two envelopes
    // are not literally identical — what "twice equals once" means is that
    // BOTH obey the same rule: correlationId is the carried root, and
    // causationId is the carried cause, preserved rather than re-derived.
    // Both fields are `??` seeds, so the second application sees them already
    // set and changes nothing.
    expect(once?.metadata.correlationId).toBe("corr-root")
    expect(once?.metadata.causationId).toBe("cause-1")
    expect(twice?.metadata.correlationId).toBe("corr-root")
    expect(twice?.metadata.causationId).toBe("cause-1")
  })

  it("is idempotent in situ when the local segment also carries the interceptor", async () => {
    // Local route: the outer wrap runs the interceptor, then the app's default
    // bus (the local segment) runs the SAME interceptor again. AF makes the same
    // trade — AxonServerCommandBus deliberately does not push
    // registerDispatchInterceptor down to its local segment, but nothing in the
    // framework guards a user who registers on both layers.
    const transport = recordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus(), { preferLocalHandlers: true })

    let seen: CommandMessage | undefined
    bus.subscribe("correlation.Finish", async (message) => {
      seen = message
      return undefined
    })

    await sendFinishFrom(bus)

    // Routed locally, so nothing hit the transport …
    expect(transport.envelopes).toHaveLength(0)
    // … and two applications of `correlation` (outer wrap + local segment's own)
    // produced exactly what one produces: the carried cause, untouched.
    expect(seen?.metadata.correlationId).toBe("corr-root")
    expect(seen?.metadata.causationId).toBe("cause-1")
  })
})

describe("RabbitMQ command bus — correlation survives a nested send over the wire", () => {
  it("a command sent from a handler carries the outer command's correlation to the transport", async () => {
    const transport = recordingLoopbackTransport()

    let outerIdentifier: string | undefined
    let finishMetadata: Metadata | undefined

    // `correlatingHandler` is what makes `ctx.send` carry: it reads the command
    // this invocation is handling, attaches `correlationFrom(it)` to the task,
    // and overlays that onto the nested command.
    const start = commandHandler(Start, async (message, ctx) => {
      outerIdentifier = message.identifier
      await ctx.send(Finish, { id: message.payload.id })
    })

    const finish = commandHandler(Finish, async ({ metadata }) => {
      finishMetadata = metadata
    })

    const carrying = <H extends { handler: any }>(h: H): H => ({
      ...h,
      handler: correlatingHandler(h.handler, correlationFrom),
    })

    const buses = inMemoryBuses()
    const commandBus = busOver(transport, buses.commandBus)
    const eventStore = inMemoryEventStore()
    const app = kronos({
      commandHandlers: [
        { ...carrying(start), eventStore, commandBus, queryBus: buses.queryBus },
        { ...carrying(finish), eventStore, commandBus, queryBus: buses.queryBus },
      ],
    })

    try {
      await send(commandBus, Start, { id: "x" }, { correlationId: "corr-root" })

      // Two envelopes crossed the transport: the outer command and the nested one.
      expect(transport.envelopes).toHaveLength(2)

      const nested = transport.envelopes.find((e) => e.message.name.name === "Finish")
      expect(nested).toBeDefined()

      // THE ASSERTION THAT FAILS ON THE OLD WIRING: correlationId on the wire.
      expect(nested!.message.metadata.correlationId).toBe("corr-root")
      // And the one the `??` seed fixes: the wrapper overlays Finish's
      // causationId from Start's identifier — the TRUE cause — and crossing
      // the `correlation`-wrapped bus preserves it instead of re-stamping it to
      // Finish's own identifier. The causal graph is a chain, not self-loops.
      expect(nested!.message.metadata.causationId).toBe(outerIdentifier)
      expect(nested!.message.identifier).not.toBe(outerIdentifier)

      // And it survives the round trip into the far-side handler.
      expect(finishMetadata?.correlationId).toBe("corr-root")
      expect(finishMetadata?.causationId).toBe(outerIdentifier)
    } finally {
      await app.stop()
    }
  })
})
