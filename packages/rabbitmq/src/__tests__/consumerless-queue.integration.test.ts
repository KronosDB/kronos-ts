import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { isNoHandler, isUnavailable, localCommandBus, localQueryBus, qn, unitOfWork } from "@kronos-ts/core"
import { rabbitMqConnection } from "../connection.js"
import { rabbitMqCommandBus } from "../command-bus.js"
import { rabbitMqQueryBus } from "../query-bus.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

const TIMEOUT_MS = 1000

describe("RabbitMQ: a name nothing is consuming", () => {
  let broker: RunningRabbitMq
  beforeAll(async () => {
    broker = await startRabbitMqContainer()
  }, 60_000)
  afterAll(async () => {
    await broker?.stop()
  }, 30_000)

  for (const kind of ["command", "query"] as const) {
    /** One process: a connection and a bus over it. `call` sends `payload` and waits for the reply. */
    async function node(prefix: string, serviceName: string) {
      const connection = await rabbitMqConnection(broker.url, {
        serviceName,
        instanceId: crypto.randomUUID(),
        topology: { prefix },
      })
      const bus: any =
        kind === "command"
          ? rabbitMqCommandBus(localCommandBus(unitOfWork), connection, { timeoutMs: TIMEOUT_MS })
          : rabbitMqQueryBus(localQueryBus(unitOfWork), connection, { timeoutMs: TIMEOUT_MS })
      return {
        connection,
        bus,
        call: (payload: string): Promise<unknown> =>
          bus[kind === "command" ? "dispatch" : "query"]({
            kind,
            identifier: crypto.randomUUID(),
            name: qn("qa", "Work"),
            payload,
            metadata: {},
          }),
      }
    }

    /** A process that handles `qa.Work` and records each payload it was given. */
    async function worker(prefix: string, handled: string[]) {
      const started = await node(prefix, "worker")
      started.bus.subscribe("qa.Work", async (message: any) => {
        handled.push(message.payload)
        return `done ${message.payload}`
      })
      await started.connection.start()
      return started
    }

    it(`${kind}: a name no queue is bound for fails at once with no-handler`, async () => {
      const caller = await node(`qa-${crypto.randomUUID()}`, "caller")
      try {
        const sent = Date.now()
        const error = await caller.call("never").then(
          () => undefined,
          (e) => e,
        )
        expect(isNoHandler(error)).toBe(true)
        expect(Date.now() - sent).toBeLessThan(TIMEOUT_MS / 2)
      } finally {
        await caller.connection.close()
      }
    }, 30_000)

    it(`${kind}: a stopped handler's queue fails at the timeout with unavailable, and the request is not run later`, async () => {
      const prefix = `qa-${crypto.randomUUID()}`
      const handled: string[] = []
      const caller = await node(prefix, "caller")
      const first = await worker(prefix, handled)
      let second: Awaited<ReturnType<typeof worker>> | undefined
      try {
        expect(await caller.call("while running")).toBe("done while running")

        // The handler stops. Its queue and the queue's binding stay on the broker.
        await first.connection.close()

        const sent = Date.now()
        const error = await caller.call("while stopped").then(
          () => undefined,
          (e) => e,
        )
        expect(Date.now() - sent).toBeGreaterThanOrEqual(TIMEOUT_MS - 50)
        expect(isUnavailable(error)).toBe(true)
        expect(isNoHandler(error)).toBe(false)
        expect((error as Error).message).toContain(`timed out after ${TIMEOUT_MS}ms`)
        expect((error as Error).message).toContain("A queue is bound for the name")

        // The request expired in the queue: a handler that starts afterwards
        // is not given it, and handles what is sent from then on.
        await new Promise((r) => setTimeout(r, 200))
        second = await worker(prefix, handled)
        expect(await caller.call("after restart")).toBe("done after restart")
        expect(handled).toEqual(["while running", "after restart"])
      } finally {
        await Promise.allSettled([caller.connection.close(), first.connection.close(), second?.connection.close()])
      }
    }, 30_000)
  }
})
