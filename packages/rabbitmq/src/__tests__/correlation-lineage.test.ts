/**
 * Correlation lineage across the DISTRIBUTED dispatch boundary.
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
 * `InterceptingCommandBus → DistributedCommandBus → SimpleCommandBus` by
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
  handlerContext,
  lineage,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  simpleCommandBus,
  simpleQueryBus,
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

/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `simpleCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(simpleCommandBus(uow), lineage),
    queryBus: interceptingQueryBus(simpleQueryBus(uow), lineage),
  }
}


const Start = command({
  name: qn("lineage", "Start"),
  payload: z.object({ id: z.string() }),
})

const Finish = command({
  name: qn("lineage", "Finish"),
  payload: z.object({ id: z.string() }),
})

/**
 * A transport that records every envelope handed to it and then loops it back to
 * the subscribed handler — so a test can assert both what crossed the "wire" and
 * what the far side received.
 */
class RecordingLoopbackTransport implements RabbitMqCommandTransport {
  readonly envelopes: RabbitMqCommandEnvelope[] = []
  private readonly handlers = new Map<
    string,
    (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>
  >()

  async dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope> {
    this.envelopes.push(envelope)
    const key = `${envelope.message.name.namespace}.${envelope.message.name.name}`
    const handler = this.handlers.get(key)
    if (!handler) return { requestId: envelope.requestId, ok: true, result: undefined }
    return handler(envelope)
  }

  subscribe(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): void {
    this.handlers.set(commandName, handler)
  }

  /** Metadata of the nth envelope that reached the transport. */
  metadataAt(index: number): Metadata {
    const envelope = this.envelopes[index]
    if (!envelope) throw new Error(`No envelope at index ${index}`)
    return envelope.message.metadata
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
      {
        config: resolveRabbitMqConfig({
          identity: { serviceName: "lineage-test", instanceId: "inst-1" },
          url: "amqp://loopback",
        }),
        commandTransport: transport,
      },
      localSegment,
      { preferLocal },
    ),
    lineage,
  )
}

/** The app's default command bus: simple bus wrapped for lineage. */
function defaultLocalBus(): CommandBus {
  return interceptingCommandBus(simpleCommandBus(unitOfWork), lineage)
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

describe("RabbitMQ command bus — correlation lineage reaches the transport", () => {
  it("stamps correlationId/causationId on a command routed to the BROKER", async () => {
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    // Model a handler's UnitOfWork: lineage on the unit of work, then out
    // through `ctx.send` — which stamps it onto the message before any bus
    // sees it, exactly as the invocation wrapper + handler context do.
    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    expect(transport.envelopes).toHaveLength(1)
    expect(transport.metadataAt(0).correlationId).toBe("corr-root")
    // `lineage` SEEDS causationId and never clobbers it. This message arrived
    // at the bus with a cause already on it — `ctx.send` stamped the handled
    // message's lineage outward — so "cause-1" crosses the wire intact. The
    // old unconditional `causationId: message.identifier` overwrote it here,
    // and every message in a chain ended up claiming to have caused itself.
    expect(transport.metadataAt(0).causationId).toBe("cause-1")
  })

  it("stamps a PRIMARY dispatch (no prior lineage) with self-referential correlation/causation", async () => {
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    const message = commandMessage(Finish.name, { id: "x" })
    await bus.dispatch(message)

    // `lineage` runs unconditionally at the bus edge: a message with no
    // correlationId of its own starts a new chain at itself, and causationId
    // is always its own identifier too.
    expect(transport.metadataAt(0).correlationId).toBe(message.identifier)
    expect(transport.metadataAt(0).causationId).toBe(message.identifier)
  })

  it("gives the UnitOfWork's lineage precedence over the message's own metadata", async () => {
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    // The unit of work's metadata IS the incoming message's; `ctx.send` merges
    // the unit of work's lineage OVER it — mergeMetadata(base, correlationData).
    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "from-uow", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    expect(transport.metadataAt(0).correlationId).toBe("from-uow")
  })

  it("wrapping twice equals wrapping once", async () => {
    // The evidence behind wrapping the distributed bus WITHOUT unwrapping the
    // local segment: `lineage` re-derives correlationId/causationId from the
    // MESSAGE ITSELF (its own metadata.correlationId, if any, and its own
    // identifier) — never from anything a previous wrap layer invented — so
    // stacking the wrapper on the SAME message is a no-op past the first
    // application.
    function seenBy(bus: (delegate: CommandBus) => CommandBus): Promise<CommandMessage | undefined> {
      let seen: CommandMessage | undefined
      const inner: CommandBus = {
        async dispatch(message) { seen = message; return undefined },
        subscribe() {},
      }
      const wrapped = bus(inner)
      return unitOfWork().execute(async (uow) => {
        uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
        await handlerContext({ uow, commandBus: wrapped }).send(Finish, { id: "x" })
        return seen
      })
    }

    const once = await seenBy((d) => interceptingCommandBus(d, lineage))
    const twice = await seenBy((d) => interceptingCommandBus(interceptingCommandBus(d, lineage), lineage))

    // Each invocation mints its own Finish identifier, so the two envelopes
    // are not literally identical — what "twice equals once" means is that
    // BOTH obey the same rule: correlationId is the contributed root, and
    // causationId is the contributed cause, preserved rather than re-derived.
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
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus(), { preferLocalHandlers: true })

    let seen: CommandMessage | undefined
    bus.subscribe("lineage.Finish", async (message) => {
      seen = message
      return undefined
    })

    await unitOfWork().execute(async (uow) => {
      uow.contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await handlerContext({ uow, commandBus: bus }).send(Finish, { id: "x" })
    })

    // Routed locally, so nothing hit the transport …
    expect(transport.envelopes).toHaveLength(0)
    // … and two applications of `lineage` (outer wrap + local segment's own)
    // produced exactly what one produces: the contributed cause, untouched.
    expect(seen?.metadata.correlationId).toBe("corr-root")
    expect(seen?.metadata.causationId).toBe("cause-1")
  })
})

describe("RabbitMQ command bus — lineage survives a nested send over the wire", () => {
  it("a command sent from a handler carries the outer command's lineage to the transport", async () => {
    const transport = new RecordingLoopbackTransport()

    let outerIdentifier: string | undefined
    let finishMetadata: Metadata | undefined

    // The invocation wrapper already seeded the unit of work's lineage from the
    // incoming command, so `ctx.send` alone carries it onward.
    const start = commandHandler(Start, async (message, ctx) => {
      outerIdentifier = message.identifier
      await ctx.send(Finish, { id: message.payload.id })
    })

    const finish = commandHandler(Finish, async ({ metadata }) => {
      finishMetadata = metadata
    })

    const buses = inMemoryBuses()
    const commandBus = busOver(transport, buses.commandBus)
    const eventStore = inMemoryEventStore()
    const app = kronos({
      commandHandlers: [
        { ...start, eventStore, commandBus, queryBus: buses.queryBus },
        { ...finish, eventStore, commandBus, queryBus: buses.queryBus },
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
      // And the one the `??` seed fixes: `ctx.send` stamps Finish's
      // causationId from Start's identifier — the TRUE cause — and crossing
      // the `lineage`-wrapped bus preserves it instead of re-stamping it to
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
