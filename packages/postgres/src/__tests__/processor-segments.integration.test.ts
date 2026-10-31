/**
 * Segments against a live postgres, with two apps in one process standing in
 * for two nodes: segments move between them on release, and a node that lost
 * its claim has its whole batch — projection rows and token — rolled back.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import {
  concurrent,
  correlation,
  emptyMetadata,
  eventHandler,
  eventProcessor,
  generateIdentifier,
  interceptingCommandBus,
  interceptingQueryBus,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  segmented,
  unitOfWork,
  type App,
  type EventDescriptor,
  type EventMessage,
  type RunningProcessor,
  type TokenStore,
} from "@kronos-ts/core"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresTokenStore } from "../postgres-token-store.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

// The payload is not validated on this path, so no schema library is needed.
const Counted = { kind: "event", name: qn("segments", "Counted"), version: "1.0", payload: {} } as unknown as EventDescriptor<any>
type Payload = { account: string; n: number }

let pg: RunningPostgres
let pool: PostgresResource

beforeAll(async () => {
  pg = await startPostgresContainer()
  pool = postgresPool(pg.connectionString)
  await pool.start()
  await pool.query(`CREATE TABLE seg_counted (account text, n integer, node text, PRIMARY KEY (account, n))`)
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await pool.query(`TRUNCATE seg_counted, ${pool.tables.tokens}, ${pool.tables.events}`)
})

function counted(account: string, n: number): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: Counted.name,
    version: "1.0",
    payload: { account, n },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: [{ key: "account", value: account }],
  }
}

const byAccount = (e: EventMessage) => (e.payload as Payload).account

/** One node: the projection inserts each event once — a second insert would fail its batch. */
function boot(node: string, tokenStore: TokenStore, segments: number): { app: App; processor: RunningProcessor } {
  const eventStore = postgresEventStore(pool)
  const uow = postgresUnitOfWork(unitOfWork, pool)
  const onCounted = eventHandler(Counted, async ({ payload }: { payload: Payload }, ctx) => {
    const tx = await postgresTransaction(ctx.unitOfWork)
    await tx.query(`INSERT INTO seg_counted (account, n, node) VALUES ($1, $2, $3)`, [payload.account, payload.n, node])
  })
  const processor = segmented(
    concurrent(eventProcessor("seg-counted", eventStore, tokenStore, uow), byAccount),
    segments,
    byAccount,
  )
  const app = kronos({
    eventHandlers: [
      {
        ...onCounted,
        eventStore,
        commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
        queryBus: interceptingQueryBus(localQueryBus(unitOfWork), correlation),
        processor,
      },
    ],
  })
  return { app, processor: app.processors.get("seg-counted")! }
}

async function rows(): Promise<Array<{ account: string; n: number; node: string }>> {
  return pool.query(`SELECT account, n, node FROM seg_counted ORDER BY account, n`)
}

async function until(done: () => Promise<boolean> | boolean, what: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await done()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error(`timed out waiting for ${what}`)
}

async function append(accounts: number, from: number, to: number) {
  const events: EventMessage[] = []
  for (let n = from; n < to; n++) for (let a = 0; a < accounts; a++) events.push(counted(`acc-${a}`, n))
  await postgresEventStore(pool).append(events)
}

describe("segments on postgres", () => {
  it("move to another node on release, with every event handled once", async () => {
    const tokens = postgresTokenStore(pool)
    const a = boot("a", tokens, 4)
    await until(() => a.processor.segments().length === 4, "a to claim all four")
    const b = boot("b", tokens, 4)
    await append(20, 0, 5)
    await until(async () => (await rows()).length === 100, "the first rounds")

    await a.processor.releaseSegment(1, 60_000)
    await a.processor.releaseSegment(3, 60_000)
    // b claims on its next round, at most a claim interval away.
    await until(() => b.processor.segments().length === 2, "b to claim the released segments", 10_000)
    await append(20, 5, 10)
    await until(async () => (await rows()).length === 200, "every event")
    await Promise.all([a.app.stop(), b.app.stop()])

    const all = await rows()
    expect(all).toHaveLength(200)
    expect(new Set(all.filter((r) => r.n >= 5).map((r) => r.node))).toEqual(new Set(["a", "b"]))
    expect(b.processor.segments()).toEqual([])
  }, 60_000)

  it("roll back a batch whose node lost the claim — rows and token together", async () => {
    // Claims lapse after 300 ms. Node a stands in for a node that stalled: it is
    // told the timeout is 10 s, so it renews only every 5 s and b can take over
    // in between. The store still expires claims by its own 300 ms.
    const tokens = postgresTokenStore(pool, { claimTimeoutMs: 300 })
    const a = boot("a", { ...tokens, claimTimeoutMs: 10_000 }, 1)
    await until(() => a.processor.segments().length === 1, "a to claim the segment")
    await append(1, 0, 1)
    await until(async () => (await rows()).length === 1, "a's first event")

    await new Promise((r) => setTimeout(r, 400))
    const b = boot("b", tokens, 1)
    await until(() => b.processor.segments().length === 1, "b to take the lapsed claim")
    await append(1, 1, 2)
    await until(async () => (await rows()).length === 2, "the second event")
    await until(() => a.processor.segments().length === 0, "a to let go")
    await Promise.all([a.app.stop(), b.app.stop()])

    const all = await rows()
    expect(all).toEqual([
      { account: "acc-0", n: 0, node: "a" },
      { account: "acc-0", n: 1, node: "b" },
    ])
  }, 60_000)
})
