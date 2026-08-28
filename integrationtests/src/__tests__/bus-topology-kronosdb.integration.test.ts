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
    await expect(send(isolated, Ping, { nonce: "lost" })).rejects.toThrow()
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
})
