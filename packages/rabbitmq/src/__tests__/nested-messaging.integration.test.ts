import assert from "node:assert/strict"
import * as amqp from "amqplib"
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { localCommandBus, localQueryBus, qn, unitOfWork } from "@kronos-ts/core"
import { rabbitMqConnection } from "../connection.js"
import { rabbitMqCommandBus } from "../command-bus.js"
import { rabbitMqQueryBus } from "../query-bus.js"
import { startRabbitMqContainer, type RunningRabbitMq } from "./testcontainers-setup.js"

describe("RabbitMQ nested messaging without local shortcuts", () => {
  let broker: RunningRabbitMq
  beforeAll(async () => {
    broker = await startRabbitMqContainer()
  }, 60_000)
  afterAll(async () => {
    await broker?.stop()
  }, 30_000)

  for (const kind of ["command", "query"] as const) {
    it(`${kind}: saturated parents receive overload instead of stranding children`, async () => {
      const connection = await rabbitMqConnection(broker.url, {
        serviceName: `saturated-${kind}`,
        instanceId: crypto.randomUUID(),
        topology: { prefix: `qa-${crypto.randomUUID()}` },
        limits: { maxConcurrentHandlers: 1 },
      })
      const bus: any =
        kind === "command"
          ? rabbitMqCommandBus(localCommandBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 1000,
            })
          : rabbitMqQueryBus(localQueryBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 1000,
            })
      const call = (depth: number) =>
        bus[kind === "command" ? "dispatch" : "query"]({
          kind,
          identifier: crypto.randomUUID(),
          name: qn("qa", "Saturated"),
          payload: depth,
          metadata: {},
        })
      bus.subscribe("qa.Saturated", async (message: any) => (message.payload ? call(0) : 42))
      try {
        await connection.start()
        await assert.rejects(call(1), /overloaded/)
        expect(await call(0)).toBe(42)
      } finally {
        await connection.close()
      }
    })
    it(`${kind}: load is bounded and every accepted reply retains its request`, async () => {
      let peak = 0
      const connection = await rabbitMqConnection(broker.url, {
        serviceName: `load-${kind}`,
        instanceId: crypto.randomUUID(),
        topology: { prefix: `qa-${crypto.randomUUID()}` },
        limits: {
          maxConcurrentHandlers: 8,
          observe: (s) => {
            if (s.name === "inbound handlers") peak = Math.max(peak, s.active)
          },
        },
      })
      const bus: any =
        kind === "command"
          ? rabbitMqCommandBus(localCommandBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 2000,
            })
          : rabbitMqQueryBus(localQueryBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 2000,
            })
      bus.subscribe("qa.Load", async (message: any) => {
        await new Promise((r) => setTimeout(r, 10))
        return message.payload
      })
      try {
        await connection.start()
        const results = await Promise.all(
          Array.from({ length: 256 }, async (_, payload) => {
            try {
              const result = await bus[kind === "command" ? "dispatch" : "query"]({
                kind,
                identifier: crypto.randomUUID(),
                name: qn("qa", "Load"),
                payload,
                metadata: {},
              })
              expect(result).toBe(payload)
              return "ok"
            } catch (error) {
              expect(String(error)).toContain("overloaded")
              return "overloaded"
            }
          }),
        )
        expect(results).toContain("ok")
        expect(results).toContain("overloaded")
        expect(peak).toBe(8)
      } finally {
        await connection.close()
      }
    })
    it(`${kind}: connection loss settles callers and an explicit replacement resumes service`, async () => {
      let socket!: amqp.ChannelModel
      const prefix = `qa-${crypto.randomUUID()}`
      const connection = await rabbitMqConnection(broker.url, {
        serviceName: `fault-${kind}`,
        instanceId: crypto.randomUUID(),
        topology: { prefix },
        amqpConnect: async (url) => {
          socket = await amqp.connect(url)
          return socket
        },
      })
      const makeBus = (c: typeof connection): any =>
        kind === "command"
          ? rabbitMqCommandBus(localCommandBus(unitOfWork), c, {
              preferLocal: false,
              timeoutMs: 2000,
            })
          : rabbitMqQueryBus(localQueryBus(unitOfWork), c, { preferLocal: false, timeoutMs: 2000 })
      const bus = makeBus(connection)
      let entered!: () => void, release!: () => void
      const started = new Promise<void>((r) => {
        entered = r
      })
      const blocked = new Promise<void>((r) => {
        release = r
      })
      bus.subscribe("qa.Fault", async () => {
        entered()
        await blocked
        return 1
      })
      const message = {
        kind,
        identifier: crypto.randomUUID(),
        name: qn("qa", "Fault"),
        payload: {},
        metadata: {},
      }
      await connection.start()
      const pending = bus[kind === "command" ? "dispatch" : "query"](message)
      void pending.catch(() => {})
      await started
      await socket.close()
      await assert.rejects(pending, /closed|unknown/i)
      release()
      await connection.close()
      const replacement = await rabbitMqConnection(broker.url, {
        serviceName: `fault-${kind}`,
        instanceId: crypto.randomUUID(),
        topology: { prefix },
      })
      const recovered = makeBus(replacement)
      recovered.subscribe("qa.Fault", async () => 42)
      try {
        await replacement.start()
        expect(
          await recovered[kind === "command" ? "dispatch" : "query"]({
            ...message,
            identifier: crypto.randomUUID(),
          }),
        ).toBe(42)
      } finally {
        await replacement.close()
      }
    })
    it(`${kind}: recursive work on the same consumer completes and child failures propagate`, async () => {
      const connection = await rabbitMqConnection(broker.url, {
        serviceName: `nested-${kind}`,
        instanceId: crypto.randomUUID(),
        topology: { prefix: `qa-${crypto.randomUUID()}` },
      })
      const bus =
        kind === "command"
          ? rabbitMqCommandBus(localCommandBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 1000,
            })
          : rabbitMqQueryBus(localQueryBus(unitOfWork), connection, {
              preferLocal: false,
              timeoutMs: 1000,
            })
      const call = (depth: number, fail = false): Promise<unknown> => {
        const message = {
          identifier: crypto.randomUUID(),
          name: qn("qa", "Recurse"),
          payload: { depth, fail },
          metadata: {},
        }
        return kind === "command" ? (bus as any).dispatch(message) : (bus as any).query(message)
      }
      const units = new Set()
      bus.subscribe("qa.Recurse", async (message, uow) => {
        units.add(uow)
        const { depth, fail } = message.payload as { depth: number; fail: boolean }
        if (depth === 0) {
          if (fail) throw new Error("child failed")
          return 0
        }
        return 1 + Number(await call(depth - 1, fail))
      })
      try {
        await connection.start()
        expect(await call(4)).toBe(4)
        expect(units.size).toBe(5)
        await assert.rejects(call(2, true), /child failed/)
        expect(await call(0)).toBe(0)
      } finally {
        await connection.close()
      }
    })
  }
})
