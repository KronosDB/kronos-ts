/**
 * A processor reset against a live postgres: the replay is stored with the
 * token and survives a restart, and what the reset function clears commits —
 * or rolls back — with the reset token.
 */
import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import {
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
  unitOfWork,
  type App,
  type EventDescriptor,
  type EventMessage,
  type RunningProcessor,
} from "@kronos-ts/core"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresEventStore } from "../postgres-event-store.js"
import { postgresTokenStore } from "../postgres-token-store.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

// The payload is not validated on this path, so no schema library is needed.
const Counted = { kind: "event", name: qn("reset", "Counted"), version: "1.0", payload: {} } as unknown as EventDescriptor<any>

let pg: RunningPostgres
let pool: PostgresResource

beforeAll(async () => {
  pg = await startPostgresContainer()
  pool = postgresPool(pg.connectionString)
  await pool.start()
  await pool.query(`CREATE TABLE reset_counted (n integer PRIMARY KEY, replay boolean NOT NULL)`)
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

function counted(n: number): EventMessage {
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: Counted.name,
    version: "1.0",
    payload: { n },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags: [],
  }
}

/** One process: the projection writes each event's `isReplay()` beside it. */
function boot(): { app: App; processor: RunningProcessor } {
  const eventStore = postgresEventStore(pool)
  const uow = postgresUnitOfWork(unitOfWork, pool)
  const onCounted = eventHandler(Counted, async ({ payload }: { payload: { n: number } }, ctx) => {
    const tx = await postgresTransaction(ctx.unitOfWork)
    await tx.query(`INSERT INTO reset_counted (n, replay) VALUES ($1, $2)`, [payload.n, ctx.isReplay()])
  })
  const app = kronos({
    eventHandlers: [
      {
        ...onCounted,
        eventStore,
        commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
        queryBus: interceptingQueryBus(localQueryBus(unitOfWork), correlation),
        processor: eventProcessor("reset-counted", eventStore, postgresTokenStore(pool), uow),
      },
    ],
  })
  return { app, processor: app.processors.get("reset-counted")! }
}

async function rows(): Promise<Array<{ n: number; replay: boolean }>> {
  return pool.query<{ n: number; replay: boolean }>(`SELECT n, replay FROM reset_counted ORDER BY n`)
}

async function until(done: () => Promise<boolean>, what: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (await done()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error(`timed out waiting for ${what}`)
}

const truncate = async (uow: Parameters<typeof postgresTransaction>[0]) => {
  const tx = await postgresTransaction(uow)
  await tx.query(`TRUNCATE reset_counted`)
}

describe("resetting a processor on postgres", () => {
  it("clears with the reset, replays after a restart, and tells the replay from what came after", async () => {
    await postgresEventStore(pool).append([counted(1), counted(2)])
    const first = boot()
    await until(async () => (await rows()).length === 2, "the first pass")
    first.processor.stop()

    await first.processor.resetTokens(0n, truncate)
    expect(await rows()).toEqual([])
    await first.app.stop()

    // A new process, and one more event it was never handed before the reset.
    await postgresEventStore(pool).append([counted(3)])
    const second = boot()
    await until(async () => (await rows()).length === 3, "the replay and the new event")
    await second.app.stop()

    expect(await rows()).toEqual([
      { n: 1, replay: true },
      { n: 2, replay: true },
      { n: 3, replay: false },
    ])
  }, 60_000)

  it("leaves the rows and the token as they were when the reset function throws", async () => {
    const { app, processor } = boot()
    await until(async () => (await rows()).length === 3, "the processor to catch up")
    processor.stop()

    await expect(
      processor.resetTokens(0n, async (uow) => {
        await truncate(uow)
        throw new Error("not today")
      }),
    ).rejects.toThrow("not today")

    expect((await rows()).length).toBe(3)
    expect(processor.replaying).toBe(false)
    await app.stop()
  }, 60_000)
})
