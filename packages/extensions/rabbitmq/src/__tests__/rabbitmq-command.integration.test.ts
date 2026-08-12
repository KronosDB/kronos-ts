import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, tag } from "@kronos-ts/common"
import { kronos, inMemoryComponents, module, type Registration } from "@kronos-ts/app"
import {
  command,
  commandHandler,
  event,
  EventCriteria,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  inMemoryEventStore,
  type AppendCondition,
  type EventStore
} from "@kronos-ts/eventsourcing"
import { rabbitMq } from "../rabbitmq.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

const StartWithSend = command({
  name: qn("rabbitCtx", "StartWithSend"),
  payload: z.object({ aId: z.string(), bId: z.string() }),
})

const Finish = command({
  name: qn("rabbitCtx", "Finish"),
  payload: z.object({ bId: z.string() }),
})

const AObserved = event({
  name: qn("rabbitCtx", "AObserved"),
  payload: z.object({ aId: z.string() }),
  tags: (p) => [tag("aId", p.aId)],
})

const BFinished = event({
  name: qn("rabbitCtx", "BFinished"),
  payload: z.object({ bId: z.string() }),
  tags: (p) => [tag("bId", p.bId)],
})

const StateA = state({
  name: "RabbitStateA",
  id: { aId: z.string() },
  initial: () => ({}),
  criteria: (id) => EventCriteria.havingTags(tag("aId", id.aId)),
  evolve: (on) => [on(AObserved, (s) => s)],
})

const StateB = state({
  name: "RabbitStateB",
  id: { bId: z.string() },
  initial: () => ({}),
  criteria: (id) => EventCriteria.havingTags(tag("bId", id.bId)),
  evolve: (on) => [on(BFinished, (s) => s)],
})

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

function conditionJson(records: Array<{ condition: AppendCondition | undefined }>): string {
  return JSON.stringify(records.at(-1)?.condition?.criteria)
}

describe("RabbitMQ command transport integration", () => {
  let broker: RunningRabbitMq

  beforeAll(async () => {
    broker = await startRabbitMqContainer()
  }, 60_000)

  afterAll(async () => {
    await broker?.stop()
  }, 30_000)

  /**
   * One process, composed by hand: in-memory components, the RabbitMQ backend
   * wrapping their buses, then the app on top of the merged record. `start()`
   * after `kronos` is what the "processors" lifecycle stage used to be — it
   * waits until every handler registered above is bound and consuming.
   */
  async function startNode(params: {
    serviceName: string
    prefix: string
    eventStore: EventStore
    registrations: Registration[]
  }) {
    const base = inMemoryComponents({ eventStore: params.eventStore })
    const backend = await rabbitMq({
      url: broker.url,
      identity: { serviceName: params.serviceName, instanceId: `${params.prefix}-${params.serviceName}` },
      topology: { prefix: params.prefix },
      localCommandBus: base.commandBus,
      localQueryBus: base.queryBus,
    })
    const app = kronos({
      components: { ...base, ...backend.components },
      modules: [module(params.serviceName, ...params.registrations)],
    })
    await backend.start()
    return {
      app,
      async stop() {
        await app.stop()
        await backend.close()
      },
    }
  }

  it("a remote command handler appends against only its own loaded state", async () => {
    const prefix = `kronos.it.${Date.now()}.send`
    const probe = probeEventStore()

    const worker = await startNode({
      serviceName: "worker",
      prefix,
      eventStore: probe.eventStore,
      registrations: [
        StateA,
        StateB,
        commandHandler(Finish, async ({ payload: cmd }, ctx) => {
          await ctx.load(StateB, { bId: cmd.bId })
          ctx.append(BFinished, { bId: cmd.bId })
        }),
      ],
    })

    const starter = await startNode({
      serviceName: "starter",
      prefix,
      eventStore: probe.eventStore,
      registrations: [
        StateA,
        StateB,
        commandHandler(StartWithSend, async ({ payload: cmd }, ctx) => {
          await ctx.load(StateA, { aId: cmd.aId })
          await ctx.send(Finish, { bId: cmd.bId })
        }),
      ],
    })

    try {
      await starter.app.commandGateway.send(
        StartWithSend,
        { aId: "a-real", bId: "b-real" },
        emptyMetadata(),
      )
      const json = conditionJson(probe.records)
      expect(json).not.toContain("a-real")
      expect(json).toContain("b-real")
    } finally {
      await Promise.all([starter.stop(), worker.stop()])
    }
  }, 30_000)
})
