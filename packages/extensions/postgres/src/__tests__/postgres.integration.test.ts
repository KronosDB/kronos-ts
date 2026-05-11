import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { kronos } from "@kronos-ts/core"
import { pgAdapter } from "../adapters/pg.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"
import { postgres } from "../postgres.js"

let pg: RunningPostgres

beforeAll(async () => {
  pg = await startPostgresContainer()
}, 60_000)

afterAll(async () => {
  await pg.stop()
}, 30_000)

describe("postgres() extension", () => {
  it("populates eventStore + snapshotStore slots and runs connect/disconnect lifecycle", async () => {
    const adapter = pgAdapter({ connectionString: pg.connectionString })

    // Capture the resolved slot factories to verify they are wired
    let capturedEventStore: unknown
    let capturedSnapshotStore: unknown

    const app = kronos({ quiet: true })
    app.use(postgres({ adapter }))
    // Register a onStart hook to inspect the built slots (via decorate)
    app.decorate("eventStore", (inner) => {
      capturedEventStore = inner
      return inner
    })
    app.decorate("snapshotStore", (inner) => {
      capturedSnapshotStore = inner
      return inner
    })

    const running = await app.start()

    // Slots should be populated with objects that have the expected methods
    expect(typeof (capturedEventStore as { append: unknown })?.append).toBe("function")
    expect(typeof (capturedEventStore as { source: unknown })?.source).toBe("function")
    expect(typeof (capturedEventStore as { open: unknown })?.open).toBe("function")
    expect(typeof (capturedSnapshotStore as { store: unknown })?.store).toBe("function")
    expect(typeof (capturedSnapshotStore as { load: unknown })?.load).toBe("function")

    await running.stop()
  })

  it("with bootstrap: false does not create the schema (consumer owns migrations)", async () => {
    const adapter = pgAdapter({ connectionString: pg.connectionString })

    // Connect directly to drop the tables, simulating a fresh environment
    await adapter.connect()
    await adapter.query(`DROP TABLE IF EXISTS kronos_events CASCADE`)
    await adapter.query(`DROP TABLE IF EXISTS kronos_snapshots CASCADE`)

    // Also drop the stored procedure
    await adapter.query(`DROP FUNCTION IF EXISTS kronos_append_with_check CASCADE`)
    await adapter.disconnect()

    const app = kronos({ quiet: true })
    app.use(postgres({ adapter, bootstrap: false }))
    const running = await app.start()

    // Tables should NOT exist (bootstrap was skipped)
    await adapter.connect()
    const row = await adapter.queryOne<{ exists: boolean }>(
      `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'kronos_events') AS exists`,
    )
    expect(row!.exists).toBe(false)
    await adapter.disconnect()

    await running.stop()
  })
})
