import assert from "node:assert/strict"
/**
 * BUSES ARE NAMED, SERVER-SCOPED, AND INDEPENDENT OF CONTEXTS (ADR-0006, 0.9).
 *
 * Two properties, each against the real server:
 *
 *   1. ISOLATION IS THE NAME. The same command name subscribed on bus "a" is
 *      unreachable from bus "b" — a typo'd or merely different bus name is an
 *      empty bus, and dispatch answers "no handler" rather than crossing over.
 *   2. ONE BUS SPANS CONTEXTS. Two connections opened on DIFFERENT event store
 *      contexts share a bus by naming the same string — the handler subscribes
 *      through one connection, the dispatch arrives through the other. The
 *      pre-0.9 per-context bus isolation is gone; contexts address logs only.
 */
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { z } from "zod"
import {
  command,
  jsonSerializer,
  localCommandBus,
  localQueryBus,
  qn,
  query as queryDescriptorOf,
  send,
  query,
  unitOfWork,
} from "@kronos-ts/core"
import {
  kronosDbConnection,
  kronosDbCommandBus,
  kronosDbQueryBus,
  type KronosDbConnectionHandle,
} from "@kronos-ts/kronosdb"

const Ping = command({ name: qn("topology", "Ping"), payload: z.object({ nonce: z.string() }) })
const Ask = queryDescriptorOf({
  name: qn("topology", "Ask"),
  payload: z.object({ nonce: z.string() }),
  result: z.string(),
})

/**
 * Subscription registration rides the handler stream and the server registers
 * it asynchronously — there is no ack to await at this level. Dispatching in a
 * retry loop until the handler is reachable is the readiness barrier, the same
 * one the full e2e gets from kronos()'s processor waits.
 */
async function untilRouted<T>(dispatch: () => Promise<T>, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await dispatch()
    } catch (error) {
      if (Date.now() > deadline) throw error
      if (!String(error).includes("no handler available")) throw error
      await new Promise((r) => setTimeout(r, 100))
    }
  }
}

describe("KronosDB bus topology (0.9, ADR-0006)", () => {
  let container: StartedTestContainer
  let handlerSide: KronosDbConnectionHandle   // context "ctx-one"
  let callerSide: KronosDbConnectionHandle    // context "ctx-two"

  beforeAll(async () => {
    container = await new GenericContainer("ghcr.io/kronosdb/kronosdb:0.9.0")
      .withExposedPorts(50051, 9240)
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()
    const host = container.getHost()
    const port = container.getMappedPort(50051)

    handlerSide = await kronosDbConnection({
      componentName: "topology-handler",
      host, port,
      context: "ctx-one",
      serializer: jsonSerializer(),
    })
    callerSide = await kronosDbConnection({
      componentName: "topology-caller",
      host, port,
      context: "ctx-two",
      serializer: jsonSerializer(),
    })
  }, 120_000)

  afterAll(async () => {
    await handlerSide?.close()
    await callerSide?.close()
    await container?.stop()
  })

  it("a handler on bus 'shared' answers a dispatch from ANOTHER connection on ANOTHER context", async () => {
    const seen: string[] = []
    const handlerBus = kronosDbCommandBus(localCommandBus(unitOfWork), handlerSide, "shared")
    handlerBus.subscribe(`${Ping.name.namespace}.${Ping.name.name}`, async (m) => {
      seen.push((m.payload as { nonce: string }).nonce)
      return undefined
    })

    const callerBus = kronosDbCommandBus(localCommandBus(unitOfWork), callerSide, "shared")
    await untilRouted(() => send(callerBus, Ping, { nonce: "across-contexts" }))
    expect(seen).toEqual(["across-contexts"])
  }, 30_000)

  it("the SAME command name on a DIFFERENT bus is unreachable — isolation is the name", async () => {
    const isolated = kronosDbCommandBus(localCommandBus(unitOfWork), callerSide, "somewhere-else")
    await assert.rejects(send(isolated, Ping, { nonce: "lost" }), /no handler available/i)
  }, 30_000)

  it("queries route on the named bus across connections too", async () => {
    const answering = kronosDbQueryBus(localQueryBus(unitOfWork), handlerSide, "shared")
    answering.subscribe(`${Ask.name.namespace}.${Ask.name.name}`, async (m) => {
      return `pong:${(m.payload as { nonce: string }).nonce}`
    })

    const asking = kronosDbQueryBus(localQueryBus(unitOfWork), callerSide, "shared")
    const answer = await untilRouted(() => query(asking, Ask, { nonce: "q1" }))
    expect(answer).toBe("pong:q1")
  }, 30_000)
  for (const kind of ["command", "query"] as const) {
    it(`${kind}: nested dispatch returns through the same real server connection`, async () => {
      const options = { flowControl: { permits: 1, refillThreshold: 0 }, timeoutMs: 1000, limits: { maxConcurrentHandlers: 8 } }
      const bus = kind === "command"
        ? kronosDbCommandBus(localCommandBus(unitOfWork), handlerSide, "qa-nested", options)
        : kronosDbQueryBus(localQueryBus(unitOfWork), handlerSide, "qa-nested", options)
      const call = (depth: number): Promise<unknown> => {
        const message = {
          identifier: crypto.randomUUID(), name: qn("qa", `Nested${kind}`), payload: depth,
          metadata: { processingInstructions: { timeoutMs: 1000 } },
        }
        return kind === "command" ? (bus as any).dispatch(message) : (bus as any).query(message)
      }
      bus.subscribe(`qa.Nested${kind}`, async (message) => {
        const depth = message.payload as number
        return depth === 0 ? 0 : 1 + Number(await call(depth - 1))
      })
      // A successful leaf dispatch is the readiness barrier for this handler.
      const deadline = Date.now() + 5000
      for (;;) {
        try { await call(0); break } catch (error) {
          if (Date.now() > deadline || !/no.?handler|no handler/i.test(String(error))) throw error
          await new Promise((r) => setTimeout(r, 25))
        }
      }
      expect(await call(4)).toBe(4)
      for (let round = 0; round < 20; round++) {
        await assert.rejects(call(8), /overloaded/)
        expect(await call(4)).toBe(4)
      }
      expect(await call(0)).toBe(0)
      const oldChannel = handlerSide.connection.channel
      await handlerSide.platform.start()
      await handlerSide.connection.reconnect()
      expect(handlerSide.connection.channel).not.toBe(oldChannel)
      expect(handlerSide.platform.connected).toBe(true)
      const recoveredBy = Date.now() + 5000
      for (;;) {
        try { expect(await call(2)).toBe(2); break } catch (error) {
          if (Date.now() > recoveredBy || !/no.?handler|no handler/i.test(String(error))) throw error
          await new Promise((r) => setTimeout(r, 25))
        }
      }
    }, 15_000)
  }

  for (const kind of ["command", "query"] as const) {
  it(`${kind}: keeps separately named buses reachable on one connection`, async () => {
    const makeBus = (name: string): any => kind === "command"
      ? kronosDbCommandBus(localCommandBus(unitOfWork), handlerSide, name, { timeoutMs: 1000 })
      : kronosDbQueryBus(localQueryBus(unitOfWork), handlerSide, name, { timeoutMs: 1000 })
    const first = makeBus("qa-first")
    const second = makeBus("qa-second")
    first.subscribe("qa.Identity", async () => "first")
    second.subscribe("qa.Identity", async () => "second")
    const call = (bus: typeof first): Promise<string> => bus[kind === "command" ? "dispatch" : "query"]({ kind, identifier: crypto.randomUUID(), name: qn("qa", "Identity"), payload: {}, metadata: {} })
    expect(await untilRouted(() => call(first))).toBe("first")
    expect(await untilRouted(() => call(second))).toBe("second")
    expect(await call(first)).toBe("first")
    await handlerSide.connection.reconnect()
    expect(await untilRouted(() => call(first))).toBe("first")
    expect(await untilRouted(() => call(second))).toBe("second")
  }, 15000)
  }

  it("subscription queries return an initial result and refill update credits", async () => {
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), handlerSide, "qa-subscriptions", { flowControl: { permits: 1, refillThreshold: 0 }, timeoutMs: 1000 })
    bus.subscribe("qa.WatchCredits", async () => 0)
    const makeMessage = () => ({ kind: "query" as const, identifier: crypto.randomUUID(), name: qn("qa", "WatchCredits"), payload: {}, metadata: {} })
    const deadline = Date.now() + 5000
    for (;;) {
      try { await bus.query(makeMessage()); break } catch (error) {
        if (Date.now() > deadline || !/no.?handler|no handler/i.test(String(error))) throw error
        await new Promise((r) => setTimeout(r, 25))
      }
    }
    async function within<T>(work: Promise<T>, phase: string): Promise<T> {
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        return await Promise.race([work, new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error(`subscription ${phase} timed out`)), 2000)
        })])
      } finally { clearTimeout(timer) }
    }
    const sub = bus.subscriptionQuery(makeMessage(), 1)
    try {
      expect(await within(sub.initialResult, "initial result")).toBe(0)
      const updates = sub.updates[Symbol.asyncIterator]()
      for (let value = 1; value <= 384; value++) {
        const pending = updates.next()
        await bus.emitUpdate("qa.WatchCredits", () => true, value)
        expect((await within(pending, `update ${value}`)).value).toBe(value)
      }
    } finally { sub.close() }
  }, 15_000)

})
