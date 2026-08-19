import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, send, type UnitOfWork } from "@kronos-ts/core"
import { kronos } from "@kronos-ts/core"
import {
  lineage,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  simpleCommandBus,
  simpleQueryBus,
} from "@kronos-ts/core"
import { command, event, commandHandler, type CommandBus } from "@kronos-ts/core"
import { state } from "@kronos-ts/core"
import {
  inMemoryEventStore,
  type EventStore,
  type AppendCondition
} from "@kronos-ts/core"
import { rabbitMqCommandBus, type RabbitMqCommandEnvelope, type RabbitMqCommandTransport } from "../command-bus.js"
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
  name: qn("ctx", "Start"),
  payload: z.object({ aId: z.string(), bId: z.string() }),
})

const Finish = command({
  name: qn("ctx", "Finish"),
  payload: z.object({ bId: z.string() }),
})

const ASeen = event({
  name: qn("ctx", "ASeen"),
  payload: z.object({ aId: z.string() }),
  tags: { aId: (p) => p.aId },
})

const BFinished = event({
  name: qn("ctx", "BFinished"),
  payload: z.object({ bId: z.string() }),
  tags: { bId: (p) => p.bId },
})

const StateA = state({
  name: "StateA",
  id: { aId: z.string() },
  initial: () => ({}),
  tags: (id) => ({ aId: id.aId }),
  evolve: [[ASeen, (s) => s]],
})

const StateB = state({
  name: "StateB",
  id: { bId: z.string() },
  initial: () => ({}),
  tags: (id) => ({ bId: id.bId }),
  evolve: [[BFinished, (s) => s]],
})

class LoopbackTransport implements RabbitMqCommandTransport {
  private handlers = new Map<string, (envelope: RabbitMqCommandEnvelope) => Promise<any>>()

  async dispatch(envelope: RabbitMqCommandEnvelope): Promise<any> {
    const handler = this.handlers.get(`${envelope.message.name.namespace}.${envelope.message.name.name}`)
    if (!handler) throw new Error("No loopback handler")
    return handler(envelope)
  }

  subscribe(commandName: string, handler: (envelope: RabbitMqCommandEnvelope) => Promise<any>): void {
    this.handlers.set(commandName, handler)
  }
}

function probeEventStore() {
  const inner = inMemoryEventStore()
  const records: Array<{ condition: AppendCondition | undefined }> = []
  const wrapped: EventStore = {
    ...inner,
    async append(events, condition) {
      records.push({ condition })
      return inner.append(events, condition)
    },
  }
  return { eventStore: wrapped, records }
}

/**
 * The loopback stand-in for the real backend: same wrapped command bus, a
 * transport that hands the envelope straight back to the subscribed handler.
 * Every command is forced through it, so each handler runs in the fresh UoW an
 * inbound distributed command gets.
 */
function loopbackCommandBus(transport: LoopbackTransport, localSegment: CommandBus): CommandBus {
  return interceptingCommandBus(
    rabbitMqCommandBus(
      {
        config: resolveRabbitMqConfig({
          identity: { serviceName: "ctx-test", instanceId: "inst-1" },
          url: "amqp://loopback",
        }),
        commandTransport: transport,
      },
      localSegment,
      { preferLocal: false },
    ),
    lineage,
  )
}

describe("RabbitMQ remote command handling e2e", () => {
  it("a remote command handler appends against only its own loaded state", async () => {
    const probe = probeEventStore()
    const transport = new LoopbackTransport()

    const start = commandHandler(Start, async ({ payload: cmd }, ctx) => {
      await ctx.load(StateA, { aId: cmd.aId })
      await ctx.send(Finish, { bId: cmd.bId })
    })

    const finish = commandHandler(Finish, async ({ payload: cmd }, ctx) => {
      await ctx.load(StateB, { bId: cmd.bId })
      ctx.append(BFinished, { bId: cmd.bId })
    })

    const buses = inMemoryBuses()
    const commandBus = loopbackCommandBus(transport, buses.commandBus)
    const eventStore = probe.eventStore
    const app = kronos({
      states: [
        { ...StateA, eventStore },
        { ...StateB, eventStore },
      ],
      commandHandlers: [
        { ...start, eventStore, commandBus, queryBus: buses.queryBus },
        { ...finish, eventStore, commandBus, queryBus: buses.queryBus },
      ],
    })

    try {
      await send(commandBus, Start, { aId: "a-1", bId: "b-1" }, emptyMetadata())
      const json = JSON.stringify(probe.records.at(-1)?.condition?.query)
      expect(json).not.toContain("a-1")
      expect(json).toContain("b-1")
    } finally {
      await app.stop()
    }
  })
})
