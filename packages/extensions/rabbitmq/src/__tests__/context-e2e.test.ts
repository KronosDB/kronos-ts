import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, tag } from "@kronos-ts/common"
import { kronos } from "@kronos-ts/app"
import type { App } from "@kronos-ts/app"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  append,
  createInMemoryEventStore,
  load,
  type EventStore,
  type AppendCondition,
} from "@kronos-ts/eventsourcing"
import { createRabbitMqCommandBus, type RabbitMqCommandEnvelope, type RabbitMqCommandTransport } from "../command-bus.js"
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
  evolve: [on(ASeen, (s) => s)],
})

const StateB = state({
  name: "StateB",
  id: { bId: z.string() },
  initial: () => ({}),
  criteria: (id) => EventCriteria.havingTags(tag("bId", id.bId)),
  evolve: [on(BFinished, (s) => s)],
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
  const inner = createInMemoryEventStore()
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

function installLoopbackRabbit(transport: LoopbackTransport): (app: App) => void {
  return (app) => {
    const resolved = resolveRabbitMqConfig(app, {
      url: "amqp://loopback",
      commands: { alwaysUseDistributedBus: true },
    })
    app.decorate("commandBus", (localSegment) =>
      createRabbitMqCommandBus({ localSegment, transport, config: resolved }),
    )
  }
}

describe("RabbitMQ remote command handling e2e", () => {
  it("a remote command handler appends against only its own loaded state", async () => {
    const probe = probeEventStore()
    const transport = new LoopbackTransport()

    const start = commandHandler(Start, async ({ payload: cmd }) => {
      await load(StateA, { aId: cmd.aId })
      const { send } = await import("@kronos-ts/messaging")
      await send(Finish, { bId: cmd.bId })
    })

    const finish = commandHandler(Finish, async ({ payload: cmd }) => {
      await load(StateB, { bId: cmd.bId })
      append(BFinished, { bId: cmd.bId })
    })

    const running = await kronos({ serviceName: "ctx-test", quiet: true })
      .use(installLoopbackRabbit(transport))
      .set("eventStore", () => probe.eventStore)
      .states(StateA, StateB)
      .commands(start, finish)
      .start()

    try {
      await running.commandGateway.send(Start, { aId: "a-1", bId: "b-1" }, emptyMetadata())
      const json = JSON.stringify(probe.records.at(-1)?.condition?.criteria)
      expect(json).not.toContain("a-1")
      expect(json).toContain("b-1")
    } finally {
      await running.stop()
    }
  })
})
