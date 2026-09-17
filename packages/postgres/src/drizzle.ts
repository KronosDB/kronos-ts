import { clientHandler } from "./client-handler.js"

export { type DbCapability } from "./client-handler.js"

/**
 * Drizzle over the task's transaction — the recommended client.
 *
 * `build` is called with the driver client the transaction unwraps to; you
 * name your driver flavour, and `ctx.db` has Drizzle's own types. The
 * SQL-style builder is typed by the `pgTable` you pass to each call, so
 * nothing else is needed; pass `{ schema }` only for Drizzle's relational
 * `db.query.*` API. Goes INSIDE `postgresHandler`, which supplies the
 * `ctx.sql()` it builds from:
 *
 * ```ts
 * import { drizzle } from "drizzle-orm/postgres-js"
 * import { drizzleHandler } from "@kronos-ts/postgres/drizzle"
 *
 * const wrap = (h) => postgresHandler(drizzleHandler(h, (client: Sql) => drizzle(client)), pg)
 *
 * // in a slice
 * type Ctx = EventHandlerContext & DbCapability<ReturnType<typeof makeDb>>
 * eventHandler(OrderCreated, async ({ payload }, ctx: Ctx) => {
 *   await ctx.db.insert(orderViews).values({ … }).onConflictDoUpdate({ … })
 * })
 * ```
 *
 * Do not call `db.transaction()` on the handle: it is already inside the
 * task's transaction, and a nested one is a savepoint at best.
 */
export const drizzleHandler = clientHandler("drizzleHandler")
