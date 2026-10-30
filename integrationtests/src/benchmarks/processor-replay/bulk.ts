/**
 * THE BULK-WRITE PROTOTYPE. Not an API — a way to measure what one would buy.
 *
 * A handler does not write. It adds to a buffer that lives as long as the
 * batch's unit of work, and the buffer is written once, in the batch's
 * transaction, in PREPARE_COMMIT: after every handler has run, before the token
 * is stored (the processor registers that last, and phase actions run in
 * registration order), so rows and token still commit or roll back together.
 *
 * Everything here is built from what exists today: `uow.onPrepareCommit`,
 * `postgresTransaction`, a Drizzle client built in user land. That is the point
 * of prototyping it this way — the first API option (an accumulator keyed by the
 * unit of work) needs nothing from the framework, and the other two are what
 * you would add on top of it.
 */
import { drizzle } from "drizzle-orm/node-postgres"
import { sql } from "drizzle-orm"
import type { PoolClient } from "pg"
import { eventHandler, unitOfWork, type UnitOfWork } from "@kronos-ts/core"
import { postgresPool, postgresTokenStore, postgresTransaction, postgresUnitOfWork } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { Handled } from "./domain.js"
import { balances as table, prepareDatabase, type Projection } from "./projections.js"

// ── the accumulator: state that lives as long as one unit of work ───────────

const accumulators = new WeakMap<UnitOfWork, Map<symbol, unknown>>()

/**
 * The state `create` made for this unit of work, made on the first ask. `flush`
 * runs once, in PREPARE_COMMIT, with that state. A second unit of work gets its
 * own, so a batch can never see another batch's rows.
 */
export function batchScoped<S>(uow: UnitOfWork, key: symbol, create: () => S, flush: (state: S) => Promise<void>): S {
  let mine = accumulators.get(uow)
  if (mine === undefined) accumulators.set(uow, (mine = new Map()))
  if (mine.has(key)) return mine.get(key) as S
  const state = create()
  mine.set(key, state)
  uow.onPrepareCommit(() => flush(state))
  return state
}

// ── the writer: what a Drizzle adapter would offer on top ───────────────────

type Row = { account: string; total: number; count: number }

const balancesKey = Symbol("replay_balances")

/** Postgres allows 65535 parameters in a statement; three columns a row. */
const ROWS_PER_STATEMENT = 20_000

/**
 * Rows for `table`, merged by key while they wait: ON CONFLICT cannot touch one
 * row twice in a statement, and for a counter the merge IS the semantics.
 */
function pendingBalances(uow: UnitOfWork): Map<string, Row> {
  return batchScoped(
    uow,
    balancesKey,
    () => new Map<string, Row>(),
    async (pending) => {
      if (pending.size === 0) return
      const tx = await postgresTransaction(uow)
      const db = drizzle(tx.unwrap<PoolClient>())
      const rows = [...pending.values()]
      for (let from = 0; from < rows.length; from += ROWS_PER_STATEMENT) {
        await db
          .insert(table)
          .values(rows.slice(from, from + ROWS_PER_STATEMENT))
          .onConflictDoUpdate({
            target: table.account,
            set: { total: sql`${table.total} + excluded.total`, count: sql`${table.count} + excluded.count` },
          })
      }
    },
  )
}

export async function bulkDrizzleProjection(url: string, uniqueKeys = false): Promise<Projection> {
  const pg = postgresPool(pgAdapter({ connectionString: url, poolConfig: { max: 40 } }))
  await pg.start()
  await prepareDatabase(pg)

  return {
    handlers: [eventHandler(Handled, ({ payload, sequence }, ctx) => {
      const pending = pendingBalances(ctx.unitOfWork)
      const account = uniqueKeys ? `${payload.account}#${sequence}` : payload.account
      const row = pending.get(account)
      if (row) {
        row.total += payload.amount
        row.count++
      } else {
        pending.set(account, { account, total: payload.amount, count: 1 })
      }
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
