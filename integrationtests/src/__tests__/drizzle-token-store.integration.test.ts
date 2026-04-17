import { describe, expect, it, beforeAll, afterAll, beforeEach } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { drizzle } from "drizzle-orm/postgres-js"
import postgres from "postgres"
import { pgTable, varchar, integer, primaryKey } from "drizzle-orm/pg-core"
import { eq, and, or, lt, isNull } from "drizzle-orm"
import { drizzleTokenStore } from "@kronos-ts/extensions/drizzle"
import { globalSequenceToken, UnableToClaimTokenError } from "@kronos-ts/messaging"
import { TOKEN_TABLE_DDL, DROP_TOKEN_TABLE } from "./shared-token-table.js"

const kronosTokenEntries = pgTable("kronos_token_entries", {
  processorName: varchar("processor_name", { length: 255 }).notNull(),
  segment: integer("segment").notNull(),
  mask: integer("mask").notNull().default(0),
  tokenType: varchar("token_type", { length: 255 }),
  token: varchar("token", { length: 10000 }),
  timestamp: varchar("timestamp", { length: 255 }),
  owner: varchar("owner", { length: 255 }),
}, (table) => [
  primaryKey({ columns: [table.processorName, table.segment] }),
])

describe("Drizzle TokenStore (PostgreSQL)", () => {
  let container: StartedTestContainer
  let sql: ReturnType<typeof postgres>
  let db: ReturnType<typeof drizzle>

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withExposedPorts(5432)
      .withEnvironment({ POSTGRES_PASSWORD: "test", POSTGRES_USER: "test", POSTGRES_DB: "test" })
      .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
      .start()

    const port = container.getMappedPort(5432)
    const host = container.getHost()
    sql = postgres(`postgresql://test:test@${host}:${port}/test`)
    db = drizzle(sql)
    await sql.unsafe(TOKEN_TABLE_DDL)
  }, 120_000)

  afterAll(async () => {
    await sql?.end()
    await container?.stop()
  })

  beforeEach(async () => {
    await sql.unsafe("DELETE FROM kronos_token_entries")
  })

  // -- Happy paths --

  it("store and get a token", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    const token = globalSequenceToken(42n)

    await store.store("test-processor", 0, token)
    const retrieved = await store.get("test-processor", 0)

    expect(retrieved).toBeDefined()
    expect(retrieved!.position()).toBe(42n)
  })

  it("get returns undefined for nonexistent entry", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })

    const retrieved = await store.get("nonexistent", 0)

    expect(retrieved).toBeUndefined()
  })

  it("initializeSegments creates entries", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })

    await store.initializeSegments("test-processor", 3)
    const segments = await store.fetchSegments("test-processor")

    expect(segments).toEqual([0, 1, 2])
  })

  it("claimToken succeeds on unclaimed segment", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 1)

    const token = await store.claimToken("test-processor", 0, "owner-1")

    expect(token).toBeUndefined() // no token stored yet
  })

  it("claimToken re-claim by same owner refreshes", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")
    await store.store("test-processor", 0, globalSequenceToken(10n))

    const token = await store.claimToken("test-processor", 0, "owner-1")

    expect(token).toBeDefined()
    expect(token!.position()).toBe(10n)
  })

  it("extendClaim refreshes timestamp", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    // should not throw
    await store.extendClaim("test-processor", 0, "owner-1")
  })

  it("releaseClaim clears ownership", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    await store.releaseClaim("test-processor", 0, "owner-1")

    const available = await store.fetchAvailableSegments("test-processor")
    expect(available).toEqual([0])
  })

  it("fetchSegments lists all segments", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 4)

    const segments = await store.fetchSegments("test-processor")

    expect(segments).toEqual([0, 1, 2, 3])
  })

  it("fetchAvailableSegments returns unclaimed segments", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 3)
    await store.claimToken("test-processor", 1, "owner-1")

    const available = await store.fetchAvailableSegments("test-processor")

    expect(available).toContain(0)
    expect(available).toContain(2)
    expect(available).not.toContain(1)
  })

  it("deleteToken removes entry", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.store("test-processor", 0, globalSequenceToken(5n))

    await store.deleteToken("test-processor", 0)
    const retrieved = await store.get("test-processor", 0)

    expect(retrieved).toBeUndefined()
  })

  // -- Unhappy paths --

  it("claimToken throws when claimed by another", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    expect(
      store.claimToken("test-processor", 0, "owner-2"),
    ).rejects.toThrow(UnableToClaimTokenError)
  })

  it("claimToken succeeds after claim expires", async () => {
    const store = drizzleTokenStore({ db, table: kronosTokenEntries, eq, and, or, lt, isNull, claimTimeoutMs: 100 })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    // Wait for claim to expire
    await new Promise(r => setTimeout(r, 150))

    // Should succeed — claim expired
    const token = await store.claimToken("test-processor", 0, "owner-2")
    expect(token).toBeUndefined()
  })
})
