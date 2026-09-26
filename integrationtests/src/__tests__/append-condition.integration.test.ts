/**
 * A decision over two states, against every store family that runs in a
 * container here: in-memory, Postgres (plain and snapshotting), KronosDB. The
 * scenarios themselves are in `./append-condition-scenarios.ts`; Axon Server
 * runs them from `../axon-e2e`.
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
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { kronosDbConnection, kronosDbEventStore, type KronosDbConnection } from "@kronos-ts/kronosdb"
import { appendConditionScenarios } from "./append-condition-scenarios.js"

describe("append condition — in-memory", () => {
  appendConditionScenarios({ eventStore: () => inMemoryEventStore(), exact: true })
})

describe("append condition — postgres", () => {
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
      }),
    )
    await pg.start()
  }, 120_000)

  afterAll(async () => {
    await pg?.close()
    await container?.stop()
  })

  describe("plain", () => {
    appendConditionScenarios({
      eventStore: () => postgresEventStore(pg),
      unitOfWork: () => postgresUnitOfWork(unitOfWork, pg),
      exact: true,
    })
  })

  describe("snapshotting", () => {
    appendConditionScenarios({
      eventStore: () => postgresSnapshottingEventStore(postgresEventStore(pg), pg, { serializer: jsonSerializer() }),
      unitOfWork: () => postgresUnitOfWork(unitOfWork, pg),
      exact: true,
    })
  })
})

describe("append condition — kronosdb", () => {
  let container: StartedTestContainer
  let connection: KronosDbConnection
  let eventStore: EventStore

  beforeAll(async () => {
    container = await new GenericContainer("ghcr.io/kronosdb/kronosdb:0.9.0")
      .withExposedPorts(50051, 9240)
      .withWaitStrategy(Wait.forHttp("/ready", 9240).forStatusCode(200))
      .start()
    connection = await kronosDbConnection({
      componentName: "append-condition-test",
      host: container.getHost(),
      port: container.getMappedPort(50051),
      context: "default",
      serializer: jsonSerializer(),
    })
    eventStore = kronosDbEventStore(connection, "default")
  }, 120_000)

  afterAll(async () => {
    await connection?.close()
    await container?.stop()
  })

  appendConditionScenarios({ eventStore: () => eventStore, exact: false })
})
