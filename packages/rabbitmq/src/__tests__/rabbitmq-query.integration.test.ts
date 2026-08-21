import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn, send, subscriptionQuery, type UnitOfWork } from "@kronos-ts/core"
import { kronos, type CommandHandlerEntry, type QueryHandlerEntry } from "@kronos-ts/core"
import { inMemoryEventStore } from "@kronos-ts/core"
import {
  correlation,
  interceptingCommandBus,
  interceptingQueryBus,
  unitOfWork,
  localCommandBus,
  localQueryBus,
} from "@kronos-ts/core"
import {
  command,
  commandHandler,
  payloadEquals,
  query,
  queryHandler
} from "@kronos-ts/core"
import { rabbitMqConnection } from "../connection.js"
import { rabbitMqCommandBus } from "../command-bus.js"
import { rabbitMqQueryBus } from "../query-bus.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

/**
 * The three things `kronos` needs that are not modules. The UoW runner is named
 * once and handed to BOTH `localCommandBus` (which captures it at construction)
 * and `kronos` — writing them on adjacent lines is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}


const GetGreeting = query({
  name: qn("rabbitQry", "GetGreeting"),
  payload: z.object({ name: z.string() }),
})

const WatchValue = query({
  name: qn("rabbitQry", "WatchValue"),
  payload: z.object({ id: z.string() }),
})

const PublishUpdate = command({
  name: qn("rabbitQry", "PublishUpdate"),
  payload: z.object({ id: z.string(), value: z.string() }),
})

describe("RabbitMQ query transport integration", () => {
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
    commandHandlers?: Array<Omit<CommandHandlerEntry, "commandBus" | "queryBus" | "eventStore">>
    queryHandlers?: Array<Omit<QueryHandlerEntry, "queryBus" | "eventStore">>
  }) {
    const buses = inMemoryBuses()
    const rabbit = await rabbitMqConnection(broker.url, {
      serviceName: params.serviceName,
      instanceId: `${params.prefix}-${params.serviceName}`,
      topology: { prefix: params.prefix },
    })
    const eventStore = inMemoryEventStore()
    const commandBus = interceptingCommandBus(
      rabbitMqCommandBus(buses.commandBus, rabbit), correlation)
    const queryBus = interceptingQueryBus(
      rabbitMqQueryBus(buses.queryBus, rabbit), correlation)
    const app = kronos({
      commandHandlers: (params.commandHandlers ?? []).map((h) => ({ ...h, eventStore, commandBus, queryBus })),
      queryHandlers: (params.queryHandlers ?? []).map((h) => ({ ...h, eventStore, queryBus })),
    })
    await rabbit.start()
    return {
      app,
      commandBus,
      queryBus,
      async stop() {
        await app.stop()
        await rabbit.close()
      },
    }
  }

  it("routes a query to a remote handler and returns its result", async () => {
    const prefix = `kronos.it.${Date.now()}.query`

    const worker = await startNode({
      serviceName: "worker",
      prefix,
      queryHandlers: [queryHandler(GetGreeting, async ({ payload: q }) => `hello, ${q.name}`)],
    })
    const caller = await startNode({ serviceName: "caller", prefix })

    try {
      const result = await query(caller.queryBus, GetGreeting, { name: "kronos" }, emptyMetadata())
      expect(result).toBe("hello, kronos")
    } finally {
      await Promise.all([caller.stop(), worker.stop()])
    }
  }, 30_000)

  it("delivers a subscription-query update emitted on one instance to a subscriber on another", async () => {
    const prefix = `kronos.it.${Date.now()}.subq`

    // The worker owns the query handler (for initial result) and the command
    // handler that emits the update. Subscriber lives on the caller.
    const worker = await startNode({
      serviceName: "worker",
      prefix,
      queryHandlers: [queryHandler(WatchValue, async () => "initial")],
      commandHandlers: [
        commandHandler(PublishUpdate, async ({ payload: cmd }, ctx) => {
          ctx.emitUpdate(WatchValue, payloadEquals({ id: cmd.id }), cmd.value)
        }),
      ],
    })
    const caller = await startNode({ serviceName: "caller", prefix })

    try {
      const sub = subscriptionQuery(
        caller.queryBus,
        WatchValue,
        { id: "x" },
        emptyMetadata(),
      )

      expect(await sub.initialResult).toBe("initial")

      // Give RabbitMQ a moment to settle the queue bindings before the worker emits.
      await new Promise((r) => setTimeout(r, 200))

      await send(caller.commandBus, PublishUpdate, { id: "x", value: "v-1" }, emptyMetadata())
      await send(caller.commandBus, PublishUpdate, { id: "y", value: "should-not-arrive" }, emptyMetadata())
      await send(caller.commandBus, PublishUpdate, { id: "x", value: "v-2" }, emptyMetadata())

      const received: unknown[] = []
      const reader = (async () => {
        for await (const u of sub.updates) {
          received.push(u)
          if (received.length >= 2) break
        }
      })()

      await Promise.race([
        reader,
        new Promise((_, reject) => setTimeout(() => reject(new Error("timeout waiting for updates")), 5_000)),
      ])

      expect(received).toEqual(["v-1", "v-2"])
      sub.close()
    } finally {
      await Promise.all([caller.stop(), worker.stop()])
    }
  }, 30_000)

  it("delivers across instances when the filter is a function predicate", async () => {
    const prefix = `kronos.it.${Date.now()}.fn-filter`

    const worker = await startNode({
      serviceName: "worker",
      prefix,
      queryHandlers: [queryHandler(WatchValue, async () => "initial")],
      commandHandlers: [
        commandHandler(PublishUpdate, async ({ payload: cmd }, ctx) => {
          // Function filter — only IDs starting with "hi-" match. This case
          // could not cross the wire under the broadcast model because JS
          // functions don't serialize. Under the gossip-mirror model the
          // filter runs on the emitter against the cluster-wide mirror.
          ctx.emitUpdate(
            WatchValue,
            (p) => (p as { id: string }).id.startsWith("hi-"),
            cmd.value,
          )
        }),
      ],
    })
    const caller = await startNode({ serviceName: "caller", prefix })

    try {
      const subHi = subscriptionQuery(
        caller.queryBus,
        WatchValue,
        { id: "hi-one" },
        emptyMetadata(),
      )
      const subLo = subscriptionQuery(
        caller.queryBus,
        WatchValue,
        { id: "lo-one" },
        emptyMetadata(),
      )
      await Promise.all([subHi.initialResult, subLo.initialResult])

      // Let the gossip claims propagate to the worker.
      await new Promise((r) => setTimeout(r, 300))

      await send(caller.commandBus, PublishUpdate, { id: "ignored", value: "match-1" }, emptyMetadata())
      await send(caller.commandBus, PublishUpdate, { id: "ignored", value: "match-2" }, emptyMetadata())

      const received: unknown[] = []
      const reader = (async () => {
        for await (const u of subHi.updates) {
          received.push(u)
          if (received.length >= 2) break
        }
      })()

      await Promise.race([
        reader,
        new Promise((_, reject) => setTimeout(() => reject(new Error("timeout waiting for updates")), 5_000)),
      ])

      expect(received).toEqual(["match-1", "match-2"])

      // subLo's payload didn't match the predicate, so it never received.
      // Close cleanly — bun will flag if there's a hung promise here.
      subHi.close()
      subLo.close()
    } finally {
      await Promise.all([caller.stop(), worker.stop()])
    }
  }, 30_000)
})
