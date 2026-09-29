/**
 * A burst of commands larger than the pool, each loading state and writing
 * through `ctx.sql()` before it appends. A command that held its unit of
 * work's transaction across the handler pinned one connection while
 * `ctx.load` waited for a second, so a burst the size of the pool deadlocked
 * until Postgres ended the idle transactions. A command's statements run on
 * the pool now, and each command needs one connection at a time.
 */
import assert from "node:assert/strict"
import { afterAll, beforeAll, describe, it } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { z } from "zod"
import {
  command,
  commandHandler,
  correlation,
  event,
  interceptingCommandBus,
  interceptingQueryBus,
  kronos,
  localCommandBus,
  localQueryBus,
  qn,
  send,
  state,
  unitOfWork,
  type CommandHandlerContext,
} from "@kronos-ts/core"
import {
  postgresEventStore,
  postgresHandler,
  postgresPool,
  postgresUnitOfWork,
  type PostgresCapability,
  type PostgresResource,
} from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"

const CustomerIdentified = event({
  name: qn("burst", "CustomerIdentified"),
  payload: z.object({ customerId: z.string() }),
  tags: { customerId: (p) => p.customerId },
})
const IdentifyCustomer = command({
  name: qn("burst", "IdentifyCustomer"),
  payload: z.object({ customerId: z.string(), email: z.string() }),
})
const Customer = state({
  id: { customerId: z.string() },
  tags: (id) => ({ customerId: id.customerId }),
  evolve: [() => ({ identifications: 0 }), [CustomerIdentified, (s) => ({ identifications: s.identifications + 1 })]],
})

const POOL = 2
const BURST = 25

describe("postgres — a burst of commands larger than the pool", () => {
  let container: StartedTestContainer
  let pg: PostgresResource

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withEnvironment({ POSTGRES_USER: "kronos", POSTGRES_PASSWORD: "kronos", POSTGRES_DB: "kronos" })
      .withExposedPorts(5432)
      .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
      .start()
    pg = postgresPool(
      pgAdapter({
        connectionString: `postgresql://kronos:kronos@${container.getHost()}:${container.getMappedPort(5432)}/kronos`,
        poolConfig: { max: POOL },
        idleInTransactionTimeoutMs: 2_000,
      }),
    )
    await pg.start()
    await pg.query("CREATE TABLE customer_pii (customer_id TEXT PRIMARY KEY, email TEXT NOT NULL)")
  }, 120_000)

  afterAll(async () => {
    await pg?.close()
    await container?.stop()
  })

  it("commits every command, each loading state and writing through ctx.sql() first", async () => {
    const eventStore = postgresEventStore(pg)
    const identify = commandHandler(
      IdentifyCustomer,
      async ({ payload }, ctx: CommandHandlerContext & PostgresCapability) => {
        const customer = await ctx.load(Customer, { customerId: payload.customerId })
        await ctx.sql().query(
          "INSERT INTO customer_pii (customer_id, email) VALUES ($1, $2) ON CONFLICT (customer_id) DO UPDATE SET email = EXCLUDED.email",
          [payload.customerId, payload.email],
        )
        if (customer.identifications === 0) ctx.append(CustomerIdentified, { customerId: payload.customerId })
      },
    )
    const commandBus = interceptingCommandBus(localCommandBus(postgresUnitOfWork(unitOfWork, pg)), correlation)
    const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)
    const app = kronos({
      commandHandlers: [{ ...identify, handler: postgresHandler(identify.handler, pg), eventStore, commandBus, queryBus }],
    })

    try {
      const started = Date.now()
      const results = await Promise.allSettled(
        Array.from({ length: BURST }, (_, i) =>
          send(commandBus, IdentifyCustomer, { customerId: `c-${i}`, email: `c-${i}@example.com` }),
        ),
      )
      const failed = results.filter((r) => r.status === "rejected")
      assert.deepEqual(failed, [])
      // Well inside the idle-in-transaction timeout: nothing waited on a held connection.
      assert.ok(Date.now() - started < 2_000, `burst took ${Date.now() - started} ms`)
      const [row] = await pg.query<{ n: number }>("SELECT count(*)::int AS n FROM customer_pii")
      assert.equal(row!.n, BURST)
    } finally {
      await app.stop()
    }
  }, 30_000)
})
