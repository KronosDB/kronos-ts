/**
 * THE MESSAGING FABRIC, AGAINST A REAL QUORUM (ADR-0007, 0.9).
 *
 * Three voters — the smallest cluster that can lose a node and keep a
 * majority; two would need both alive to agree on anything. The properties
 * under test are Tier 2's: the handler registry rides Raft, so a bus name is a
 * CLUSTER-WIDE identity —
 *
 *   subscribe on node 1 · dispatch via node 3 · the server forwards.
 *
 * Nothing here uses any client API beyond what a single node uses: the fabric
 * deliberately changed no client contract, and this test is the proof.
 *
 * The cluster is three local processes when KRONOSDB_SERVER_BIN is set, three
 * containers otherwise (see ../kronosdb-server.ts).
 */
import assert from "node:assert/strict"
import { describe, expect, it, beforeAll, afterAll } from "bun:test"
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
import { startKronosDbCluster, type KronosDbServer } from "../kronosdb-server.js"

const Ping = command({ name: qn("fabric", "Ping"), payload: z.object({ nonce: z.string() }) })
const Ask = queryDescriptorOf({
  name: qn("fabric", "Ask"),
  payload: z.object({ nonce: z.string() }),
  result: z.string(),
})

/** Registration is asynchronous and now also Raft-replicated; retry until routed. */
async function untilRouted<T>(dispatch: () => Promise<T>, timeoutMs = 30_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await dispatch()
    } catch (error) {
      if (Date.now() > deadline) throw error
      if (!String(error).includes("no handler available")) throw error
      await new Promise((r) => setTimeout(r, 200))
    }
  }
}

describe("KronosDB messaging fabric — 3-voter cluster (0.9, ADR-0007)", () => {
  let nodes: KronosDbServer[] = []
  let onNodeOne: KronosDbConnectionHandle
  let onNodeThree: KronosDbConnectionHandle

  beforeAll(async () => {
    nodes = await startKronosDbCluster(3)

    onNodeOne = await kronosDbConnection({
      componentName: "fabric-handler",
      host: nodes[0]!.host,
      port: nodes[0]!.port,
      serializer: jsonSerializer(),
    })
    onNodeThree = await kronosDbConnection({
      componentName: "fabric-caller",
      host: nodes[2]!.host,
      port: nodes[2]!.port,
      serializer: jsonSerializer(),
    })
  }, 300_000)

  afterAll(async () => {
    await onNodeOne?.close()
    await onNodeThree?.close()
    await Promise.all(nodes.map((n) => n.stop()))
  })

  it("a command subscribed via node 1 is reachable from node 3 — dispatch forwards across the fabric", async () => {
    const seen: string[] = []
    const handlerBus = kronosDbCommandBus(localCommandBus(unitOfWork), onNodeOne, "fabric")
    handlerBus.subscribe(`${Ping.name.namespace}.${Ping.name.name}`, async (m) => {
      seen.push((m.payload as { nonce: string }).nonce)
      return undefined
    })

    const callerBus = kronosDbCommandBus(localCommandBus(unitOfWork), onNodeThree, "fabric")
    await untilRouted(() => send(callerBus, Ping, { nonce: "cross-node" }))
    expect(seen).toEqual(["cross-node"])
  }, 60_000)

  it("a query answered via node 1 serves a caller on node 3", async () => {
    const answering = kronosDbQueryBus(localQueryBus(unitOfWork), onNodeOne, "fabric")
    answering.subscribe(`${Ask.name.namespace}.${Ask.name.name}`, async (m) => {
      return `pong:${(m.payload as { nonce: string }).nonce}`
    })

    const asking = kronosDbQueryBus(localQueryBus(unitOfWork), onNodeThree, "fabric")
    const answer = await untilRouted(() => query(asking, Ask, { nonce: "q1" }))
    expect(answer).toBe("pong:q1")
  }, 60_000)

  it("a subscription query opened via node 3 is served by a handler on node 1, updates and all", async () => {
    const Watch = qn("fabric", "Watch")
    const name = `${Watch.namespace}.${Watch.name}`
    const serving = kronosDbQueryBus(localQueryBus(unitOfWork), onNodeOne, "fabric")
    serving.subscribe(name, async (m) => `initial:${(m.payload as { id: string }).id}`)

    const watching = kronosDbQueryBus(localQueryBus(unitOfWork), onNodeThree, "fabric")
    const open = () => watching.subscriptionQuery({ kind: "query", identifier: crypto.randomUUID(), name: Watch, payload: { id: "w" }, metadata: {}, timestamp: Date.now() })
    const subscription = await untilRouted(async () => {
      const sub = open()
      await sub.initialResult
      return sub
    })
    expect(await subscription.initialResult).toBe("initial:w")

    const updates = subscription.updates[Symbol.asyncIterator]()
    await serving.emitUpdate(name, () => true, "relayed")
    expect((await updates.next()).value).toBe("relayed")
    await serving.completeSubscription(name)
    expect(await updates.next()).toEqual({ value: undefined, done: true })
  }, 60_000)

  it("bus isolation holds across the fabric too — another bus name is another world", async () => {
    const isolated = kronosDbCommandBus(localCommandBus(unitOfWork), onNodeThree, "elsewhere")
    await assert.rejects(send(isolated, Ping, { nonce: "lost" }), /no handler available/i)
  }, 60_000)
})
