import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { kronos } from "@kronos-ts/app"
import {
  command,
  commandHandler,
  payloadEquals,
  query,
  queryHandler
} from "@kronos-ts/messaging"
import { rabbitMq } from "../rabbitmq.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

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
  let rabbit: RunningRabbitMq

  beforeAll(async () => {
    rabbit = await startRabbitMqContainer()
  }, 60_000)

  afterAll(async () => {
    await rabbit?.stop()
  }, 30_000)

  it("routes a query to a remote handler and returns its result", async () => {
    const prefix = `kronos.it.${Date.now()}.query`

    const worker = await kronos({ serviceName: "worker", instanceId: `${prefix}-worker`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .queries(
        queryHandler(GetGreeting, async ({ payload: q }) => `hello, ${q.name}`),
      )
      .start()

    const caller = await kronos({ serviceName: "caller", instanceId: `${prefix}-caller`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .start()

    try {
      const result = await caller.queryGateway.query(GetGreeting, { name: "kronos" }, emptyMetadata())
      expect(result).toBe("hello, kronos")
    } finally {
      await Promise.all([caller.stop(), worker.stop()])
    }
  }, 30_000)

  it("delivers a subscription-query update emitted on one instance to a subscriber on another", async () => {
    const prefix = `kronos.it.${Date.now()}.subq`

    // The worker owns the query handler (for initial result) and the command
    // handler that emits the update. Subscriber lives on the caller.
    const worker = await kronos({ serviceName: "worker", instanceId: `${prefix}-worker`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .queries(queryHandler(WatchValue, async () => "initial"))
      .commands(
        commandHandler(PublishUpdate, async ({ payload: cmd }, ctx) => {
          ctx.emitUpdate(WatchValue, payloadEquals({ id: cmd.id }), cmd.value)
        }),
      )
      .start()

    const caller = await kronos({ serviceName: "caller", instanceId: `${prefix}-caller`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .start()

    try {
      const sub = caller.queryGateway.subscriptionQuery(
        WatchValue,
        { id: "x" },
        emptyMetadata(),
      )

      expect(await sub.initialResult).toBe("initial")

      // Give RabbitMQ a moment to settle the queue bindings before the worker emits.
      await new Promise((r) => setTimeout(r, 200))

      await caller.commandGateway.send(PublishUpdate, { id: "x", value: "v-1" }, emptyMetadata())
      await caller.commandGateway.send(PublishUpdate, { id: "y", value: "should-not-arrive" }, emptyMetadata())
      await caller.commandGateway.send(PublishUpdate, { id: "x", value: "v-2" }, emptyMetadata())

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

    const worker = await kronos({ serviceName: "worker", instanceId: `${prefix}-worker`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .queries(queryHandler(WatchValue, async () => "initial"))
      .commands(
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
      )
      .start()

    const caller = await kronos({ serviceName: "caller", instanceId: `${prefix}-caller`, quiet: true })
      .use(rabbitMq({ url: rabbit.url, topology: { prefix } }))
      .start()

    try {
      const subHi = caller.queryGateway.subscriptionQuery(
        WatchValue,
        { id: "hi-one" },
        emptyMetadata(),
      )
      const subLo = caller.queryGateway.subscriptionQuery(
        WatchValue,
        { id: "lo-one" },
        emptyMetadata(),
      )
      await Promise.all([subHi.initialResult, subLo.initialResult])

      // Let the gossip claims propagate to the worker.
      await new Promise((r) => setTimeout(r, 300))

      await caller.commandGateway.send(PublishUpdate, { id: "ignored", value: "match-1" }, emptyMetadata())
      await caller.commandGateway.send(PublishUpdate, { id: "ignored", value: "match-2" }, emptyMetadata())

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
