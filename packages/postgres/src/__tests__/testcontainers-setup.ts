/**
 * Shared Postgres testcontainer fixture for @kronos-ts/postgres integration
 * tests. ALL integration tests in Plans 04 / 05 / 06 import this — no test
 * boots its own container directly.
 *
 * Image: postgres:16-alpine. PG16 is chosen over the D-12.13 floor of PG14
 * because PG16 + alpine is the smallest fast-pull image and exercises both
 * `xid8` and `pg_snapshot_xmin(pg_current_snapshot())` — the two PG14+
 * primitives Plan 05's tailing query depends on (D-12.14).
 *
 * Reuse: containers are NOT reused across processes (bun:test forks a worker
 * per file by default) — each integration test file pays the ~5-10s cold
 * start once. If the user wants faster local runs, set TESTCONTAINERS_REUSE=1
 * and the container will be promoted to a reusable singleton via the
 * testcontainers reuse protocol.
 */

import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"

export type RunningPostgres = {
  /** libpq-format connection string the adapter expects. */
  readonly connectionString: string
  /** Host:port + db + user split out for adapters that don't parse URIs. */
  readonly host: string
  readonly port: number
  readonly database: string
  readonly user: string
  readonly password: string
  /** Stop + remove the container. Idempotent. */
  stop(): Promise<void>
}

const PASSWORD = "kronos_test_password"
const DATABASE = "kronos_test"
const USER = "kronos_test_user"

export async function startPostgresContainer(): Promise<RunningPostgres> {
  const reuse = process.env.TESTCONTAINERS_REUSE === "1"
  let builder = new GenericContainer("postgres:16-alpine")
    .withEnvironment({
      POSTGRES_PASSWORD: PASSWORD,
      POSTGRES_DB: DATABASE,
      POSTGRES_USER: USER,
    })
    .withExposedPorts(5432)
    // Wait for the second "ready to accept connections" log line — the first
    // one is from the initial bootstrap DB; the second is from the final
    // listener after init scripts run. This avoids racy first-query failures.
    .withWaitStrategy(
      Wait.forLogMessage("database system is ready to accept connections", 2),
    )

  if (reuse) {
    builder = builder.withReuse()
  }

  const container: StartedTestContainer = await builder.start()
  const host = container.getHost()
  const port = container.getMappedPort(5432)
  const connectionString = `postgresql://${USER}:${PASSWORD}@${host}:${port}/${DATABASE}`

  let stopped = false
  return {
    connectionString,
    host,
    port,
    database: DATABASE,
    user: USER,
    password: PASSWORD,
    async stop() {
      if (stopped) return
      stopped = true
      await container.stop()
    },
  }
}
