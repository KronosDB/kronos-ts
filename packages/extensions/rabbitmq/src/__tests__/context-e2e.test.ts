import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, tag } from "@kronos-ts/common"
import { kronos, inMemoryComponents, module } from "@kronos-ts/app"
import {
  command,
  event,
  commandHandler,
  EventCriteria,
  type CommandBus,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  inMemoryEventStore,
  type EventStore,
  type AppendCondition
} from "@kronos-ts/eventsourcing"
import { rabbitMqCommandBus, type RabbitMqCommandEnvelope, type RabbitMqCommandTransport } from "../command-bus.js"
import { resolveRabbitMqConfig } from "../rabbitmq.js"

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
  tags: (p) => [tag("aId", p.aId)],
})

const BFinished = event({
  name: qn("ctx", "BFinished"),
  payload: z.object({ bId: z.string() }),
  tags: (p) => [tag("bId", p.bId)],
})

const StateA = state({
  name: "StateA",
  id: { aId: z.string() },
  initial: () => ({}),
  criteria: (id) => EventCriteria.havingTags(tag("aId", id.aId)),
  evolve: (on) => [on(ASeen, (s) => s)],
})

const StateB = state({
  name: "StateB",
  id: { bId: z.string() },
  initial: () => ({}),
  criteria: (id) => EventCriteria.havingTags(tag("bId", id.bId)),
  evolve: (on) => [on(BFinished, (s) => s)],
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
  return rabbitMqCommandBus({
    localSegment,
    transport,
    config: resolveRabbitMqConfig({
      identity: { serviceName: "ctx-test", instanceId: "inst-1" },
      url: "amqp://loopback",
      commands: { alwaysUseDistributedBus: true },
    }),
  })
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

    const base = inMemoryComponents({ eventStore: probe.eventStore })
    const app = kronos({
      components: { ...base, commandBus: loopbackCommandBus(transport, base.commandBus) },
      modules: [module("ctx", StateA, StateB, start, finish)],
    })

    try {
      await app.commandGateway.send(Start, { aId: "a-1", bId: "b-1" }, emptyMetadata())
      const json = JSON.stringify(probe.records.at(-1)?.condition?.criteria)
      expect(json).not.toContain("a-1")
      expect(json).toContain("b-1")
    } finally {
      await app.stop()
    }
  })
})
