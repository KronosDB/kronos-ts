import { afterAll, beforeAll, describe, expect, it } from "bun:test"
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
  type CommandHandlerDefinition,
} from "@kronos-ts/core"
import { command, commandHandler, event } from "@kronos-ts/core"
import { state, type StateModule } from "@kronos-ts/core"
import {
  inMemoryEventStore,
  type AppendCondition,
  type EventStore
} from "@kronos-ts/core"
import { rabbitMqConnection } from "../connection.js"
import { rabbitMqCommandBus } from "../command-bus.js"
import { rabbitMqQueryBus } from "../query-bus.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

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
  tags: { aId: (p) => p.aId },
})

const BFinished = event({
  name: qn("rabbitCtx", "BFinished"),
  payload: z.object({ bId: z.string() }),
  tags: { bId: (p) => p.bId },
})

const StateA = state({
  name: "RabbitStateA",
  id: { aId: z.string() },
  initial: () => ({}),
  tags: (id) => ({ aId: id.aId }),
  evolve: [[AObserved, (s) => s]],
})

const StateB = state({
  name: "RabbitStateB",
  id: { bId: z.string() },
  initial: () => ({}),
  tags: (id) => ({ bId: id.bId }),
  evolve: [[BFinished, (s) => s]],
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
  return JSON.stringify(records.at(-1)?.condition?.query)
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
    states: StateModule<any, any>[]
    commandHandlers: CommandHandlerDefinition<any, any>[]
  }) {
    const buses = inMemoryBuses()
    const rabbit = await rabbitMqConnection(broker.url, {
      serviceName: params.serviceName,
      instanceId: `${params.prefix}-${params.serviceName}`,
      topology: { prefix: params.prefix },
    })
    const commandBus = interceptingCommandBus(
      rabbitMqCommandBus(rabbit, buses.commandBus), lineage)
    const queryBus = interceptingQueryBus(
      rabbitMqQueryBus(rabbit, buses.queryBus), lineage)
    const app = kronos({
      states: params.states.map((s) => ({ ...s, eventStore: params.eventStore })),
      commandHandlers: params.commandHandlers.map((h) => ({ ...h, eventStore: params.eventStore, commandBus, queryBus })),
    })
    await rabbit.start()
    return {
      app,
      commandBus,
      async stop() {
        await app.stop()
        await rabbit.close()
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
      states: [StateA, StateB],
      commandHandlers: [
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
      states: [StateA, StateB],
      commandHandlers: [
        commandHandler(StartWithSend, async ({ payload: cmd }, ctx) => {
          await ctx.load(StateA, { aId: cmd.aId })
          await ctx.send(Finish, { bId: cmd.bId })
        }),
      ],
    })

    try {
      await send(
        starter.commandBus,
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

  /**
   * Correlation lineage over a REAL broker, not a loopback transport.
   *
   * This is what the composition order buys: `interceptingCommandBus` wraps the
   * DISTRIBUTED bus, so the stamp lands above the local-vs-remote fork and a
   * command that leaves the process over AMQP arrives on the far side with its
   * `correlationId` / `causationId` intact. Wrapping inside the fork — the old
   * shape — loses both the moment the command is routed to the broker.
   */
  it("carries correlationId/causationId across the broker into the remote handler", async () => {
    const prefix = `kronos.it.${Date.now()}.lineage`

    let finishMetadata: Record<string, unknown> | undefined
    let finishIdentifier: string | undefined
    let outerIdentifier: string | undefined

    const worker = await startNode({
      serviceName: "worker",
      prefix,
      eventStore: inMemoryEventStore(),
      states: [],
      commandHandlers: [
        commandHandler(Finish, async ({ metadata, identifier }) => {
          finishMetadata = metadata as Record<string, unknown>
          finishIdentifier = identifier
        }),
      ],
    })

    const starter = await startNode({
      serviceName: "starter",
      prefix,
      eventStore: inMemoryEventStore(),
      states: [],
      commandHandlers: [
        commandHandler(StartWithSend, async (message, ctx) => {
          outerIdentifier = message.identifier
          await ctx.send(Finish, { bId: message.payload.bId })
        }),
      ],
    })

    try {
      await send(
        starter.commandBus,
        StartWithSend,
        { aId: "a-1", bId: "b-1" },
        { correlationId: "corr-over-the-wire" },
      )

      expect(finishMetadata?.correlationId).toBe("corr-over-the-wire")
      // `lineage` keeps correlationId across every hop AND preserves the
      // causationId `ctx.send` stamped — so across a real broker, Finish's
      // cause is StartWithSend, not Finish itself (see
      // correlation-lineage.test.ts for the in-process version of this same
      // finding).
      expect(finishMetadata?.causationId).toBe(outerIdentifier)
      expect(finishIdentifier).not.toBe(outerIdentifier)
    } finally {
      await Promise.all([starter.stop(), worker.stop()])
    }
  }, 30_000)
})
