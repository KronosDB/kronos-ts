/**
 * Correlation lineage across the DISTRIBUTED dispatch boundary.
 *
 * The regression this pins: `correlationDataDispatchInterceptor` used to be
 * registered ONLY inside `@kronos-ts/app`'s default in-memory command bus. The
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
  type Metadata,
  type QualifiedName,
} from "@kronos-ts/common"
import { kronos, inMemoryComponents, module } from "@kronos-ts/app"
import {
  applyCorrelationData,
  command,
  commandHandler,
  contributeCorrelationData,
  correlationDataDispatchInterceptor,
  messageOriginProvider,
  runInNewUoW,
  type CommandBus,
  type CommandMessage,
} from "@kronos-ts/messaging"
import {
  rabbitMqCommandBus,
  type RabbitMqCommandEnvelope,
  type RabbitMqCommandReplyEnvelope,
  type RabbitMqCommandTransport,
} from "../command-bus.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"

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

function busOver(
  transport: RabbitMqCommandTransport,
  localSegment: CommandBus,
  options?: { preferLocalHandlers?: boolean },
): CommandBus {
  const preferLocalHandlers = options?.preferLocalHandlers ?? false
  return rabbitMqCommandBus({
    localSegment,
    transport,
    config: resolveRabbitMqConfig({
      identity: { serviceName: "lineage-test", instanceId: "inst-1" },
      url: "amqp://loopback",
      commands: { preferLocalHandlers, alwaysUseDistributedBus: !preferLocalHandlers },
    }),
  })
}

/** The app's default command bus: simple bus + correlation dispatch interceptor. */
function defaultLocalBus(): CommandBus {
  return inMemoryComponents().commandBus
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

    // Model a handler's UnitOfWork: correlation data seeded on the active UoW,
    // exactly as correlationDataHandlerInterceptor / the event processors do.
    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.dispatch(commandMessage(Finish.name, { id: "x" }))
    })

    expect(transport.envelopes).toHaveLength(1)
    expect(transport.metadataAt(0).correlationId).toBe("corr-root")
    expect(transport.metadataAt(0).causationId).toBe("cause-1")
  })

  it("leaves a PRIMARY dispatch (no active UnitOfWork) untouched", async () => {
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    await bus.dispatch(commandMessage(Finish.name, { id: "x" }))

    expect(transport.metadataAt(0).correlationId).toBeUndefined()
    expect(transport.metadataAt(0).causationId).toBeUndefined()
  })

  it("gives the UnitOfWork's lineage precedence over the message's own metadata", async () => {
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus())

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "from-uow", causationId: "cause-1" })
      const message = commandMessage(Finish.name, { id: "x" })
      // Same precedence as the in-process bus: mergeMetadata(base, correlationData).
      await bus.dispatch({ ...message, metadata: { correlationId: "from-message" } })
    })

    expect(transport.metadataAt(0).correlationId).toBe("from-uow")
  })

  it("applies the interceptor idempotently — twice equals once", async () => {
    // The evidence behind wrapping the distributed bus WITHOUT unwrapping the
    // local segment: `mergeMetadata` is `{ ...base, ...override }`, and both
    // applications read the same correlation data off the same UnitOfWork.
    const interceptor = correlationDataDispatchInterceptor<CommandMessage>()

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      const message = commandMessage(Finish.name, { id: "x" })
      const once = await interceptor(message)
      const twice = await interceptor(once)
      expect(twice.metadata).toEqual(once.metadata)
    })
  })

  it("is idempotent in situ when the local segment also carries the interceptor", async () => {
    // Local route: the outer wrap runs the interceptor, then the app's default
    // bus (the local segment) runs the SAME interceptor again. AF makes the same
    // trade — AxonServerCommandBus deliberately does not push
    // registerDispatchInterceptor down to its local segment, but nothing in the
    // framework guards a user who registers on both layers.
    const transport = new RecordingLoopbackTransport()
    const bus = busOver(transport, defaultLocalBus(), { preferLocalHandlers: true })

    let seen: Metadata | undefined
    bus.subscribe("lineage.Finish", async (message) => {
      seen = message.metadata
      return undefined
    })

    await runInNewUoW(emptyMetadata(), async () => {
      contributeCorrelationData({ correlationId: "corr-root", causationId: "cause-1" })
      await bus.dispatch(commandMessage(Finish.name, { id: "x" }))
    })

    // Routed locally, so nothing hit the transport …
    expect(transport.envelopes).toHaveLength(0)
    // … and two applications produced exactly what one produces.
    expect(seen?.correlationId).toBe("corr-root")
    expect(seen?.causationId).toBe("cause-1")
  })
})

describe("RabbitMQ command bus — lineage survives a nested send over the wire", () => {
  it("a command sent from a handler carries the outer command's lineage to the transport", async () => {
    const transport = new RecordingLoopbackTransport()

    let outerIdentifier: string | undefined
    let finishMetadata: Metadata | undefined

    // Stands in for correlationDataHandlerInterceptor: seed the UoW's
    // correlation data from the incoming command before dispatching onward.
    const start = commandHandler(Start, async (message, ctx) => {
      outerIdentifier = message.identifier
      applyCorrelationData(message, [messageOriginProvider()])
      await ctx.send(Finish, { id: message.payload.id })
    })

    const finish = commandHandler(Finish, async ({ metadata }) => {
      finishMetadata = metadata
    })

    const base = inMemoryComponents()
    const app = kronos({
      components: { ...base, commandBus: busOver(transport, base.commandBus) },
      modules: [module("lineage", start, finish)],
    })

    try {
      await app.commandGateway.send(Start, { id: "x" }, { correlationId: "corr-root" })

      // Two envelopes crossed the transport: the outer command and the nested one.
      expect(transport.envelopes).toHaveLength(2)

      const nested = transport.envelopes.find((e) => e.message.name.name === "Finish")
      expect(nested).toBeDefined()

      // THE ASSERTION THAT FAILS ON THE OLD WIRING: lineage on the wire.
      expect(nested!.message.metadata.correlationId).toBe("corr-root")
      expect(nested!.message.metadata.causationId).toBe(outerIdentifier)

      // And it survives the round trip into the far-side handler.
      expect(finishMetadata?.correlationId).toBe("corr-root")
      expect(finishMetadata?.causationId).toBe(outerIdentifier)
    } finally {
      await app.stop()
    }
  })
})
