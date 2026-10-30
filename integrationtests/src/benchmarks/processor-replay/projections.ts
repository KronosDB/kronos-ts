/**
 * The two read models the benchmark fills: a Map, and a Postgres table through
 * a Drizzle client that is built in user land, as `transactional-commands`
 * shows (the framework ships none).
 */
import { drizzle } from "drizzle-orm/node-postgres"
import { bigint, integer, pgTable, text } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"
import type { PoolClient } from "pg"
import { eventHandler, unitOfWork, type EventHandler, type TokenStore, type UnitOfWork } from "@kronos-ts/core"
import {
  postgresPool,
  postgresTokenStore,
  postgresTransaction,
  postgresUnitOfWork,
  type PostgresResource,
} from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { Handled, descriptors } from "./domain.js"

export type Projection = {
  readonly handlers: ReadonlyArray<EventHandler<any, any>>
  readonly unitOfWork: () => UnitOfWork
  readonly tokenStore?: TokenStore<any>
  /** Events the handler has been given (a check, not a measurement). */
  handled(): Promise<number>
  close(): Promise<void>
}

/** `all` handles every type of the log instead of one in ten. */
export function memoryProjection(all = false): Projection {
  const balances = new Map<string, number>()
  let handled = 0
  const add = ({ payload }: { payload: { account: string; amount: number } }) => {
    balances.set(payload.account, (balances.get(payload.account) ?? 0) + payload.amount)
    handled++
  }
  return {
    handlers: (all ? descriptors : [Handled]).map((descriptor) => eventHandler(descriptor, add)),
    unitOfWork,
    handled: async () => handled,
    close: async () => {},
  }
}

export const balances = pgTable("replay_balances", {
  account: text("account").primaryKey(),
  total: bigint("total", { mode: "number" }).notNull().default(0),
  count: integer("count").notNull().default(0),
})

/** The table every drizzle run writes into. */
export async function prepareDatabase(pg: PostgresResource): Promise<void> {
  await pg.query(`CREATE TABLE IF NOT EXISTS replay_balances (account text PRIMARY KEY, total bigint NOT NULL DEFAULT 0, count integer NOT NULL DEFAULT 0)`)
  await pg.query(`TRUNCATE replay_balances`)
}

/** One upsert per event, in the batch's transaction: what a projection written today does. */
export async function drizzleProjection(url: string, uniqueKeys = false): Promise<Projection & { readonly pg: PostgresResource }> {
  const pg = postgresPool(pgAdapter({ connectionString: url, poolConfig: { max: 40 } }))
  await pg.start()
  await prepareDatabase(pg)

  return {
    pg,
    handlers: [eventHandler(Handled, async ({ payload, sequence }, ctx) => {
      const tx = await postgresTransaction(ctx.unitOfWork)
      const db = drizzle(tx.unwrap<PoolClient>())
      await db
        .insert(balances)
        .values({ account: uniqueKeys ? `${payload.account}#${sequence}` : payload.account, total: payload.amount, count: 1 })
        .onConflictDoUpdate({
          target: balances.account,
          set: { total: sql`${balances.total} + ${payload.amount}`, count: sql`${balances.count} + 1` },
        })
    })],
    unitOfWork: postgresUnitOfWork(unitOfWork, pg),
    tokenStore: postgresTokenStore(pg),
    handled: async () => {
      const rows = await pg.query<{ n: string | null }>(`SELECT sum(count)::text AS n FROM replay_balances`)
      return Number(rows[0]?.n ?? 0)
    },
    close: () => pg.close(),
  }
}
