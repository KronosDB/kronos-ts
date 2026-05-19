import { describe, expect, it, beforeAll, afterAll, beforeEach } from "bun:test"
import { GenericContainer, type StartedTestContainer, Wait } from "testcontainers"
import { Kysely, PostgresDialect, sql } from "kysely"
import pg from "pg"
import { kyselyTokenStore } from "@kronos-ts/kysely"
import { globalSequenceToken, UnableToClaimTokenError } from "@kronos-ts/messaging"
import { TOKEN_TABLE_DDL, DROP_TOKEN_TABLE } from "./shared-token-table.js"

describe("Kysely TokenStore (PostgreSQL)", () => {
  let container: StartedTestContainer
  let db: Kysely<any>

  beforeAll(async () => {
    container = await new GenericContainer("postgres:16-alpine")
      .withExposedPorts(5432)
      .withEnvironment({ POSTGRES_PASSWORD: "test", POSTGRES_USER: "test", POSTGRES_DB: "test" })
      .withWaitStrategy(Wait.forLogMessage(/database system is ready to accept connections/, 2))
      .start()

    const port = container.getMappedPort(5432)
    const host = container.getHost()
    db = new Kysely({
      dialect: new PostgresDialect({
        pool: new pg.Pool({ connectionString: `postgresql://test:test@${host}:${port}/test` }),
      }),
    })
    await db.executeQuery(sql.raw(TOKEN_TABLE_DDL).compile(db))
  }, 120_000)

  afterAll(async () => {
    await db?.destroy()
    await container?.stop()
  })

  beforeEach(async () => {
    await db.executeQuery(sql.raw("DELETE FROM kronos_token_entries").compile(db))
  })

  // -- Happy paths --

  it("store and get a token", async () => {
    const store = kyselyTokenStore(db as any)
    const token = globalSequenceToken(42n)

    await store.store("test-processor", 0, token)
    const retrieved = await store.get("test-processor", 0)

    expect(retrieved).toBeDefined()
    expect(retrieved!.position()).toBe(42n)
  })

  it("get returns undefined for nonexistent entry", async () => {
    const store = kyselyTokenStore(db as any)

    const retrieved = await store.get("nonexistent", 0)

    expect(retrieved).toBeUndefined()
  })

  it("initializeSegments creates entries", async () => {
    const store = kyselyTokenStore(db as any)

    await store.initializeSegments("test-processor", 3)
    const segments = await store.fetchSegments("test-processor")

    expect(segments).toEqual([0, 1, 2])
  })

  it("claimToken succeeds on unclaimed segment", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 1)

    const token = await store.claimToken("test-processor", 0, "owner-1")

    expect(token).toBeUndefined() // no token stored yet
  })

  it("claimToken re-claim by same owner refreshes", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")
    await store.store("test-processor", 0, globalSequenceToken(10n))

    const token = await store.claimToken("test-processor", 0, "owner-1")

    expect(token).toBeDefined()
    expect(token!.position()).toBe(10n)
  })

  it("extendClaim refreshes timestamp", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    // should not throw
    await store.extendClaim("test-processor", 0, "owner-1")
  })

  it("releaseClaim clears ownership", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    await store.releaseClaim("test-processor", 0, "owner-1")

    const available = await store.fetchAvailableSegments("test-processor")
    expect(available).toEqual([0])
  })

  it("fetchSegments lists all segments", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 4)

    const segments = await store.fetchSegments("test-processor")

    expect(segments).toEqual([0, 1, 2, 3])
  })

  it("fetchAvailableSegments returns unclaimed segments", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 3)
    await store.claimToken("test-processor", 1, "owner-1")

    const available = await store.fetchAvailableSegments("test-processor")

    expect(available).toContain(0)
    expect(available).toContain(2)
    expect(available).not.toContain(1)
  })

  it("deleteToken removes entry", async () => {
    const store = kyselyTokenStore(db as any)
    await store.store("test-processor", 0, globalSequenceToken(5n))

    await store.deleteToken("test-processor", 0)
    const retrieved = await store.get("test-processor", 0)

    expect(retrieved).toBeUndefined()
  })

  // -- Unhappy paths --

  it("claimToken throws when claimed by another", async () => {
    const store = kyselyTokenStore(db as any)
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    expect(
      store.claimToken("test-processor", 0, "owner-2"),
    ).rejects.toThrow(UnableToClaimTokenError)
  })

  it("claimToken succeeds after claim expires", async () => {
    const store = kyselyTokenStore(db as any, { claimTimeoutMs: 100 })
    await store.initializeSegments("test-processor", 1)
    await store.claimToken("test-processor", 0, "owner-1")

    // Wait for claim to expire
    await new Promise(r => setTimeout(r, 150))

    // Should succeed — claim expired
    const token = await store.claimToken("test-processor", 0, "owner-2")
    expect(token).toBeUndefined()
  })
})
