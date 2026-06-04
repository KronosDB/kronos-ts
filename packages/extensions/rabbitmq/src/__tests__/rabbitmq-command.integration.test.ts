import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, tag } from "@kronos-ts/common"
import { kronos, type RunningApp } from "@kronos-ts/app"
import {
  command,
  commandHandler,
  event,
  EventCriteria,
  send,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import {
  append,
  createInMemoryEventStore,
  load,
  type AppendCondition,
  type EventStore,
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

function conditionJson(records: Array<{ condition: AppendCondition | undefined }>): string {
  return JSON.stringify(records.at(-1)?.condition?.criteria)
}

describe("RabbitMQ command transport integration", () => {
  let rabbit: RunningRabbitMq

  beforeAll(async () => {
    rabbit = await startRabbitMqContainer()
  }, 60_000)

  afterAll(async () => {
    await rabbit?.stop()
  }, 30_000)

  async function startApps(prefix: string, probe: ReturnType<typeof probeEventStore>): Promise<RunningApp[]> {
    const worker = await kronos({ serviceName: "worker", instanceId: `${prefix}-worker`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .set("eventStore", () => probe.eventStore)
      .states(StateA, StateB)
      .commands(
        commandHandler(Finish, async ({ payload: cmd }) => {
          await load(StateB, { bId: cmd.bId })
          append(BFinished, { bId: cmd.bId })
        }),
      )
      .start()

    const starter = await kronos({ serviceName: "starter", instanceId: `${prefix}-starter`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .set("eventStore", () => probe.eventStore)
      .states(StateA, StateB)
      .commands(
        commandHandler(StartWithSend, async ({ payload: cmd }) => {
          await load(StateA, { aId: cmd.aId })
          await send(Finish, { bId: cmd.bId })
        }),
      )
      .start()

    return [starter, worker]
  }

  it("a remote command handler appends against only its own loaded state", async () => {
    const prefix = `kronos.it.${Date.now()}.send`
    const probe = probeEventStore()
    const apps = await startApps(prefix, probe)
    try {
      await apps[0]!.commandGateway.send(StartWithSend, { aId: "a-real", bId: "b-real" }, emptyMetadata())
      const json = conditionJson(probe.records)
      expect(json).not.toContain("a-real")
      expect(json).toContain("b-real")
    } finally {
      await Promise.all(apps.map((app) => app.stop()))
    }
  }, 30_000)
})
