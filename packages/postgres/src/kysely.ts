import { clientHandler } from "./client-handler.js"

/** What a handler names to reach `ctx.db`: Kysely, typed as your build function types it. */
export type KyselyCapability<Db> = {
  readonly db: Db
}

/**
 * Kysely over the task's transaction.
 *
 * `build` is called with the driver client the transaction unwraps to. Kysely's
 * Postgres dialect wants a POOL, so the build presents that one client as a
 * pool that always hands it back and never releases it — the transaction owns
 * its lifetime, not Kysely:
 *
 * ```ts
 * import { Kysely, PostgresDialect } from "kysely"
 * import { kyselyHandler } from "@kronos-ts/postgres/kysely"
 *
 * const wrap = (h) => postgresHandler(
 *   kyselyHandler(h, (client: PoolClient) =>
 *     new Kysely<DB>({ dialect: new PostgresDialect({ pool: { connect: async () => ({ query: client.query.bind(client), release: () => {} }), end: async () => {} } }) })),
 *   pg,
 * )
 * ```
 *
 * That shim is for the `pg` driver, whose client already has Kysely's
 * `query(sql, params) => { rows, rowCount }` shape. Goes INSIDE
 * `postgresHandler`; `ctx.db` has Kysely's own types. Do not call
 * `db.transaction()` on the handle — it is already inside the task's.
 */
export const kyselyHandler = clientHandler("kyselyHandler")
