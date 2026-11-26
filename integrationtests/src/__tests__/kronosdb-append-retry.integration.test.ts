/**
 * Appends that ride through a node going away, against a real three-node
 * cluster.
 *
 * A node is stopped while writers append through it. Appends in flight lose
 * their connection, or are refused while the cluster elects a leader, and the
 * client sends each one again. Every append must resolve, and every event must
 * be in the log exactly once: a retry of an append that was already written is
 * answered, not written again.
 *
 * Needs a server that recognises a retried append (KronosDB 0.17 or later):
 * KRONOSDB_SERVER_BIN or KRONOSDB_IMAGE (see ../kronosdb-server.ts). Against an
 * older server the client sends each append once, and there is nothing to test.
 */
import { afterAll, beforeAll, describe, expect, it } from "bun:test"
import { generateIdentifier, jsonSerializer, qn, type TaggedEvent } from "@kronos-ts/core"
import { kronosDbConnection, kronosDbEventStore, type KronosDbConnectionHandle } from "@kronos-ts/kronosdb"
import { startKronosDbCluster, type KronosDbServer } from "../kronosdb-server.js"

const run = generateIdentifier().slice(0, 8)
const WRITERS = 4
const PER_WRITER = 150

const written = (writer: number, n: number): TaggedEvent => ({
  event: {
    kind: "event",
    identifier: `${run}-${writer}-${n}`,
    name: qn("retry-it", "Counted"),
    version: "1",
    payload: { writer, n },
    metadata: {},
    timestamp: Date.now(),
  },
  tags: [{ key: "run", value: run }],
})

describe("KronosDB event store — appends through a node going away", () => {
  let nodes: KronosDbServer[] = []
  let kdb: KronosDbConnectionHandle
  let recognisesRetries = false

  beforeAll(async () => {
    nodes = await startKronosDbCluster(3)
    kdb = await kronosDbConnection({
      componentName: "retry-it",
      servers: nodes.map((node) => `${node.host}:${node.port}`),
      serializer: jsonSerializer(),
    })
    recognisesRetries = (await kdb.connection.native.ready()).welcome.retrySafeAppends
  }, 300_000)

  afterAll(async () => {
    await kdb?.close()
    await Promise.all(nodes.map((node) => node.stop()))
  })

  it("every append resolves and every event is in the log once", async () => {
    if (!recognisesRetries) {
      console.warn("skipped: this KronosDB server does not recognise a retried append")
      return
    }
    const store = kronosDbEventStore(kdb, "default")
    let done = 0
    const writers = Array.from({ length: WRITERS }, async (_, writer) => {
      for (let n = 0; n < PER_WRITER; n++) {
        await store.append([written(writer, n)])
        done++
      }
    })

    // Stop the node the client is connected to once the writers are under way.
    const deadline = Date.now() + 30_000
    while (done < WRITERS * 10 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5))
    await nodes[0]!.stop()

    await Promise.all(writers)

    const { events } = await store.source({ query: { tags: { run } } })
    const seen = new Map<string, number>()
    for (const event of events) seen.set(event.identifier, (seen.get(event.identifier) ?? 0) + 1)
    const twice = [...seen].filter(([, count]) => count > 1).map(([identifier]) => identifier)

    expect(twice).toEqual([])
    expect(seen.size).toBe(WRITERS * PER_WRITER)
    for (let writer = 0; writer < WRITERS; writer++) {
      const order = events.filter((e) => (e.payload as { writer: number }).writer === writer).map((e) => (e.payload as { n: number }).n)
      expect(order).toEqual(Array.from({ length: PER_WRITER }, (_, n) => n))
    }
  }, 180_000)
})
