/**
 * postgresTokenStore against a live postgres — the same proof set the ORM
 * families' token-store tests run, plus the one thing that is specific to this
 * family: a token update joins the unit of work's transaction.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from "bun:test"
import { globalSequenceToken, unitOfWork, UnableToClaimTokenError } from "@kronos-ts/core"
import type { TokenStore } from "@kronos-ts/core"
import { postgresPool, type PostgresResource } from "../postgres-pool.js"
import { postgresTokenStore } from "../postgres-token-store.js"
import { postgresTransaction, postgresUnitOfWork } from "../postgres-transaction.js"
import { startPostgresContainer, type RunningPostgres } from "./testcontainers-setup.js"

let pg: RunningPostgres
let pool: PostgresResource
let store: TokenStore

beforeAll(async () => {
  pg = await startPostgresContainer()
  pool = postgresPool(pg.connectionString)
  await pool.start()
  store = postgresTokenStore(pool)
}, 60_000)

afterAll(async () => {
  await pool.close()
  await pg.stop()
}, 30_000)

beforeEach(async () => {
  await pool.query(`TRUNCATE TABLE ${pool.tables.tokens}`)
})

describe("postgresTokenStore", () => {
  it("stores and reads back a token", async () => {
    await store.store("proc", 0, globalSequenceToken(42n))
    const token = await store.get("proc", 0)
    expect(token).toBeDefined()
    // Tokens carry behaviour (position/covers/bounds), so identity is the
    // position they round-trip, not structural equality.
    expect(token!.position()).toBe(42n)
  })

  it("get returns undefined for a segment that was never written", async () => {
    expect(await store.get("proc", 7)).toBeUndefined()
  })

  it("store upserts — a second store for the same segment replaces the token", async () => {
    await store.store("proc", 0, globalSequenceToken(1n))
    await store.store("proc", 0, globalSequenceToken(2n))
    expect((await store.get("proc", 0))!.position()).toBe(2n)
    expect(await store.fetchSegments("proc")).toEqual([0])
  })

  it("initializeSegments creates one row per segment and is idempotent", async () => {
    await store.initializeSegments("proc", 3)
    await store.initializeSegments("proc", 3)
    expect(await store.fetchSegments("proc")).toEqual([0, 1, 2])
    expect(await store.get("proc", 1)).toBeUndefined()
  })

  it("claimToken succeeds on an unclaimed segment and returns its token", async () => {
    await store.store("proc", 0, globalSequenceToken(9n))
    const token = await store.claimToken("proc", 0, "owner-a")
    expect(token!.position()).toBe(9n)
  })

  it("claimToken throws UnableToClaimTokenError when another owner holds a live claim", async () => {
    await store.store("proc", 0, globalSequenceToken(0n))
    await store.claimToken("proc", 0, "owner-a")
    await expect(store.claimToken("proc", 0, "owner-b")).rejects.toBeInstanceOf(
      UnableToClaimTokenError,
    )
  })

  it("claimToken succeeds once the previous claim has expired", async () => {
    const shortLived = postgresTokenStore(pool, { claimTimeoutMs: 1 })
    await shortLived.store("proc", 0, globalSequenceToken(0n))
    await shortLived.claimToken("proc", 0, "owner-a")
    await new Promise((r) => setTimeout(r, 20))
    await expect(shortLived.claimToken("proc", 0, "owner-b")).resolves.toBeDefined()
  })

  it("releaseClaim clears ownership so another instance can take the segment", async () => {
    await store.store("proc", 0, globalSequenceToken(0n))
    await store.claimToken("proc", 0, "owner-a")
    await store.releaseClaim("proc", 0, "owner-a")
    await expect(store.claimToken("proc", 0, "owner-b")).resolves.toBeDefined()
  })

  it("extendClaim refreshes the lease without changing ownership", async () => {
    const shortLived = postgresTokenStore(pool, { claimTimeoutMs: 200 })
    await shortLived.store("proc", 0, globalSequenceToken(0n))
    await shortLived.claimToken("proc", 0, "owner-a")
    await new Promise((r) => setTimeout(r, 120))
    await shortLived.extendClaim("proc", 0, "owner-a")
    await new Promise((r) => setTimeout(r, 120))
    // Without the extension the claim would have lapsed by now.
    await expect(shortLived.claimToken("proc", 0, "owner-b")).rejects.toBeInstanceOf(
      UnableToClaimTokenError,
    )
  })

  it("fetchAvailableSegments lists only the unclaimed (or stale) segments", async () => {
    await store.initializeSegments("proc", 3)
    await store.claimToken("proc", 1, "owner-a")
    expect(await store.fetchAvailableSegments("proc")).toEqual([0, 2])
  })

  it("deleteToken removes the row", async () => {
    await store.store("proc", 0, globalSequenceToken(1n))
    await store.deleteToken("proc", 0)
    expect(await store.fetchSegments("proc")).toEqual([])
  })

  it("joins the unit of work's transaction — a rolled-back unit of work loses the token", async () => {
    // THE point of this family. The token store writes through the same client
    // handle the handler writes through, so a crash cannot advance a
    // processor's token while losing the work it accounts for.
    const make = postgresUnitOfWork(unitOfWork, pool)

    await expect(
      make().execute(async (uow) => {
        // Force the lazy transaction open, then write the token into it.
        await postgresTransaction(uow)
        await store.store("proc", 0, globalSequenceToken(5n), uow)
        // Visible inside the transaction…
        expect((await store.get("proc", 0, uow))!.position()).toBe(5n)
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")

    // …and gone outside it.
    expect(await store.get("proc", 0)).toBeUndefined()
  })

  it("commits with the unit of work when it succeeds", async () => {
    const make = postgresUnitOfWork(unitOfWork, pool)
    await make().execute(async (uow) => {
      await postgresTransaction(uow)
      await store.store("proc", 0, globalSequenceToken(6n), uow)
    })
    expect((await store.get("proc", 0))!.position()).toBe(6n)
  })
})
