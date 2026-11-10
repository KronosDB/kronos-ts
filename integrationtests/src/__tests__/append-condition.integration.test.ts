/**
 * A decision over two states, against every store family that runs in a
 * container here: in-memory, Postgres (plain and snapshotting), KronosDB. The
 * scenarios themselves are in `./append-condition-scenarios.ts`; Axon Server
 * runs them from `../axon-e2e`. The event store contract
 * (`./event-store-contract.ts`) runs against each of them too, and against
 * Postgres on each of its three drivers.
 */
import { afterAll, beforeAll, describe } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { inMemoryEventStore, jsonSerializer, unitOfWork, type EventStore } from "@kronos-ts/core"
import {
  postgresEventStore,
  postgresPool,
  postgresSnapshottingEventStore,
  postgresUnitOfWork,
  type PostgresResource,
} from "@kronos-ts/postgres"
import type { PostgresAdapter } from "@kronos-ts/postgres"
import { bunSqlAdapter } from "@kronos-ts/postgres/adapters/bun-sql"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { postgresAdapter } from "@kronos-ts/postgres/adapters/postgres"
import { kronosDbConnection, kronosDbEventStore, type KronosDbConnection } from "@kronos-ts/kronosdb"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"
import { appendConditionScenarios } from "./append-condition-scenarios.js"
import { eventStoreContract } from "./event-store-contract.js"

describe("append condition — in-memory", () => {
  appendConditionScenarios({ eventStore: () => inMemoryEventStore(), exact: true })
  eventStoreContract({ eventStore: () => inMemoryEventStore() })
})

async function startPostgres(): Promise<{ container: StartedTestContainer; url: string }> {
  const container = await new GenericContainer("postgres:16-alpine")
    .withEnvironment({ POSTGRES_USER: "kronos", POSTGRES_PASSWORD: "kronos", POSTGRES_DB: "kronos" })
    .withExposedPorts(5432)
    .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
    .start()
  return { container, url: `postgresql://kronos:kronos@${container.getHost()}:${container.getMappedPort(5432)}/kronos` }
}

const drivers: Record<string, (connectionString: string) => PostgresAdapter> = {
  pg: (connectionString) => pgAdapter({ connectionString }),
  "postgres.js": (connectionString) => postgresAdapter({ connectionString }),
  "Bun.SQL": (connectionString) => bunSqlAdapter({ connectionString }),
}

describe("append condition — postgres", () => {
  let container: StartedTestContainer
  let url: string
  let pg: PostgresResource
  const pools: PostgresResource[] = []
  const containers: StartedTestContainer[] = []

  beforeAll(async () => {
    ;({ container, url } = await startPostgres())
    pg = postgresPool(pgAdapter({ connectionString: url }))
    await pg.start()
  }, 120_000)

  afterAll(async () => {
    for (const pool of [pg, ...pools]) await pool?.close().catch(() => {})
    for (const stopped of containers) await stopped.stop().catch(() => {})
    await container?.stop()
  })

  for (const [driver, adapter] of Object.entries(drivers)) {
    describe(`event store contract on ${driver}`, () => {
      let pool: PostgresResource
      beforeAll(async () => {
        pool = postgresPool(adapter(url))
        pools.push(pool)
        await pool.start()
      }, 60_000)

      eventStoreContract({
        eventStore: () => postgresEventStore(pool),
        unreachable: async (port) => {
          const unreachable = postgresPool(adapter(`postgresql://kronos:kronos@127.0.0.1:${port}/kronos`), { bootstrap: false })
          pools.push(unreachable)
          return postgresEventStore(unreachable)
        },
        stopped: async () => {
          const own = await startPostgres()
          containers.push(own.container)
          const stopping = postgresPool(adapter(own.url))
          pools.push(stopping)
          await stopping.start()
          const store = postgresEventStore(stopping)
          await store.source({ query: { tags: { subject: "warm-up" } } })
          await own.container.stop()
          return store
        },
      })
    })
  }

  describe("plain", () => {
    eventStoreContract({ eventStore: () => postgresEventStore(pg) })
    appendConditionScenarios({
      eventStore: () => postgresEventStore(pg),
      unitOfWork: () => postgresUnitOfWork(unitOfWork, pg),
      exact: true,
    })
  })

  describe("snapshotting", () => {
    eventStoreContract({ eventStore: () => postgresSnapshottingEventStore(postgresEventStore(pg), pg, jsonSerializer()) })
    appendConditionScenarios({
      eventStore: () => postgresSnapshottingEventStore(postgresEventStore(pg), pg, jsonSerializer()),
      unitOfWork: () => postgresUnitOfWork(unitOfWork, pg),
      exact: true,
    })
  })
})

describe("append condition — kronosdb", () => {
  let server: KronosDbServer
  let connection: KronosDbConnection
  let eventStore: EventStore
  const connections: KronosDbConnection[] = []
  const servers: KronosDbServer[] = []

  beforeAll(async () => {
    server = await startKronosDb()
    connection = await kronosDbConnection({
      componentName: "append-condition-test",
      host: server.host,
      port: server.port,
      context: "default",
      serializer: jsonSerializer(),
    })
    eventStore = kronosDbEventStore(connection, "default")
  }, 120_000)

  afterAll(async () => {
    for (const other of connections) await other.close().catch(() => {})
    for (const other of servers) await other.stop().catch(() => {})
    await connection?.close()
    await server?.stop()
  })

  const connect = async (host: string, port: number) => {
    const opened = await kronosDbConnection({
      componentName: "event-store-contract",
      host,
      port,
      context: "default",
      serializer: jsonSerializer(),
    })
    connections.push(opened)
    return opened
  }

  appendConditionScenarios({ eventStore: () => eventStore, exact: false })
  eventStoreContract({
    eventStore: () => eventStore,
    unreachable: async (port) => kronosDbEventStore(await connect("127.0.0.1", port), "default"),
    stopped: async () => {
      const own = await startKronosDb()
      servers.push(own)
      const store = kronosDbEventStore(await connect(own.host, own.port), "default")
      await store.source({ query: { tags: { subject: "warm-up" } } })
      await own.stop()
      return store
    },
  })
})
