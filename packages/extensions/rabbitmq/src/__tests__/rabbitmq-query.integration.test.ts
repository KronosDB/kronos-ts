import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { kronos, type RunningApp } from "@kronos-ts/app"
import { query, queryHandler } from "@kronos-ts/messaging"
import { rabbitMq } from "../rabbitmq.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

const GetGreeting = query({
  name: qn("rabbitQry", "GetGreeting"),
  payload: z.object({ name: z.string() }),
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
        queryHandler(GetGreeting, async (q) => `hello, ${q.name}`),
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
})
