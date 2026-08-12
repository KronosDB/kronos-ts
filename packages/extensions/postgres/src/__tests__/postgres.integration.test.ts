import { describe, it, expect, beforeAll, afterAll } from "bun:test"
import { createApp, inMemoryComponents } from "@kronos-ts/app"
import { descriptorBasedTagResolver } from "@kronos-ts/eventsourcing"
import { jsonSerializer } from "@kronos-ts/messaging"
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

describe("postgres() backend", () => {
  it("provides eventStore + snapshotStore components and connects on construction", async () => {
    const adapter = pgAdapter({ connectionString: pg.connectionString })

    // The factory IS the lifecycle: it connects (and bootstraps) eagerly and
    // hands back the components it provides. No slot registry, no decorators —
    // the components are a value the composition root spreads over the defaults.
    const backend = await postgres({
      adapter,
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })
    const app = createApp({
      components: { ...inMemoryComponents(), ...backend.components },
      modules: [],
    })
    await backend.start()

    try {
      const { eventStore, snapshotStore } = backend.components
      expect(typeof (eventStore as { append: unknown }).append).toBe("function")
      expect(typeof (eventStore as { source: unknown }).source).toBe("function")
      expect(typeof (eventStore as { open: unknown }).open).toBe("function")
      expect(typeof (snapshotStore as { store: unknown }).store).toBe("function")
      expect(typeof (snapshotStore as { load: unknown }).load).toBe("function")

      // The connect + bootstrap lifecycle really ran: the schema is there.
      const row = await adapter.queryOne<{ exists: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'kronos_events') AS exists`,
      )
      expect(row!.exists).toBe(true)
    } finally {
      await app.stop()
      await backend.close()
    }
  })

  it("with bootstrap: false does not create the schema (consumer owns migrations)", async () => {
    const adapter = pgAdapter({ connectionString: pg.connectionString })

    // Connect directly to drop the tables, simulating a fresh environment
    await adapter.connect()
    await adapter.query(`DROP TABLE IF EXISTS kronos_events CASCADE`)
    await adapter.query(`DROP TABLE IF EXISTS kronos_snapshots CASCADE`)
    await adapter.disconnect()

    const backend = await postgres({
      adapter,
      bootstrap: false,
      serializer: jsonSerializer(),
      tagResolver: descriptorBasedTagResolver(),
    })
    const app = createApp({
      components: { ...inMemoryComponents(), ...backend.components },
      modules: [],
    })

    try {
      // Tables should NOT exist (bootstrap was skipped)
      const row = await adapter.queryOne<{ exists: boolean }>(
        `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'kronos_events') AS exists`,
      )
      expect(row!.exists).toBe(false)
    } finally {
      await app.stop()
      await backend.close()
    }
  })
})
