/**
 * THE REPOSITORY PROTOTYPE. User land, like `bulk.ts`: nothing here is a
 * framework API, and nothing in the framework changed to make it possible.
 *
 * A handler reads a row and saves it as if it were the table:
 *
 *   const b = await ctx.balances.get(id)
 *   await ctx.balances.save({ ...b, total: b.total + amount })
 *
 * and the batch decides when the table is written. `get` answers this batch's
 * copy, else the stored row, else a new row from the table's column defaults.
 * `save` only replaces the batch's copy. In PREPARE_COMMIT the copies are
 * written as one upsert per table, before the processor stores its token (it
 * registers that last, and phase actions run in registration order), so rows
 * and token commit or roll back together, and a failed batch's copies go with
 * its unit of work.
 *
 * How many events fold before a write is the processor's batch: up to
 * `replayBatchSize` while it is behind, one or two at the head.
 */
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres"
import { eq, getTableColumns, is, sql, SQL, type InferSelectModel } from "drizzle-orm"
import type { PgColumn, PgTable } from "drizzle-orm/pg-core"
import type { PoolClient } from "pg"
import { eventHandler, unitOfWork, type EventHandlerContext, type UnitOfWork } from "@kronos-ts/core"
import { postgresPool, postgresTokenStore, postgresTransaction, postgresUnitOfWork } from "@kronos-ts/postgres"
import { pgAdapter } from "@kronos-ts/postgres/adapters/pg"
import { Handled } from "./domain.js"
import { balances as table, prepareDatabase, type Projection } from "./projections.js"

// ── the repository: typed off the table, kept per batch ─────────────────────

type Row<T extends PgTable> = InferSelectModel<T>

/** The property that holds the table's primary key. One column; composite keys are not covered. */
type KeyName<T extends PgTable> = {
  [K in keyof T["_"]["columns"]]: T["_"]["columns"][K]["_"]["isPrimaryKey"] extends true ? K : never
}[keyof T["_"]["columns"]] & keyof Row<T>

export type Repository<T extends PgTable> = {
  /** This batch's row, else the stored one, else a new one from the table's defaults. */
  get(id: Row<T>[KeyName<T>]): Promise<Row<T>>
  /** Replace this batch's row. Written with the token when the batch commits. */
  save(row: Row<T>): Promise<void>
}

export type Repositories<T extends Record<string, PgTable>> = { readonly [K in keyof T]: Repository<T[K]> }

/** What a table needs to be kept in a batch: its key, and what a row starts as. */
type Shape = {
  readonly table: PgTable
  readonly key: PgColumn
  readonly keyName: string
  readonly fresh: Record<string, unknown>
  readonly others: ReadonlyArray<PgColumn>
}

function shapeOf(table: PgTable): Shape {
  const columns = Object.entries(getTableColumns(table)) as Array<[string, PgColumn]>
  const keys = columns.filter(([, column]) => column.primary)
  if (keys.length !== 1) throw new Error(`a repository needs a table with a one-column primary key`)
  const [keyName, key] = keys[0]!
  const fresh: Record<string, unknown> = {}
  for (const [name, column] of columns) {
    if (column === key) continue
    // A row nobody has stored yet starts at the column's default. A NOT NULL
    // column without one has nothing to start at, so the table is refused
    // when the handler is wrapped, not on the first event that misses.
    if (column.default !== undefined && !is(column.default, SQL)) fresh[name] = column.default
    else if (!column.notNull) fresh[name] = null
    else throw new Error(`a repository cannot start a row: column "${column.name}" is NOT NULL and has no default`)
  }
  return { table, key, keyName, fresh, others: columns.map(([, column]) => column).filter((column) => column !== key) }
}

/** Postgres allows 65535 parameters in a statement. */
const MAX_PARAMETERS = 65_535

async function write(db: NodePgDatabase, shape: Shape, rows: Map<unknown, Record<string, unknown>>): Promise<void> {
  if (rows.size === 0) return
  const all = [...rows.values()]
  const perStatement = Math.floor(MAX_PARAMETERS / (shape.others.length + 1))
  const set = Object.fromEntries(
    Object.entries(getTableColumns(shape.table))
      .filter(([, column]) => column !== shape.key)
      .map(([name, column]) => [name, sql`excluded.${sql.identifier((column as PgColumn).name)}`]),
  )
  for (let from = 0; from < all.length; from += perStatement) {
    await db
      .insert(shape.table)
      .values(all.slice(from, from + perStatement) as any)
      .onConflictDoUpdate({ target: shape.key, set })
  }
}

/** One batch: its client, and a copy of each row a handler has touched. */
type Batch = { readonly db: NodePgDatabase; readonly rows: Map<PgTable, Map<unknown, Record<string, unknown>>> }

const batches = new WeakMap<UnitOfWork, Promise<Batch>>()

function batchOf(uow: UnitOfWork, shapes: ReadonlyArray<Shape>): Promise<Batch> {
  let batch = batches.get(uow)
  if (batch === undefined) {
    batch = (async () => {
      const tx = await postgresTransaction(uow)
      const opened: Batch = { db: drizzle(tx.unwrap<PoolClient>()), rows: new Map() }
      uow.onPrepareCommit(async () => {
        for (const shape of shapes) await write(opened.db, shape, opened.rows.get(shape.table) ?? new Map())
      })
      return opened
    })()
    batches.set(uow, batch)
  }
  return batch
}

function repository(shape: Shape, batch: Batch): Repository<PgTable> {
  let rows = batch.rows.get(shape.table)
  if (rows === undefined) batch.rows.set(shape.table, (rows = new Map()))
  const kept = rows
  return {
    async get(id) {
      const row = kept.get(id)
      if (row !== undefined) return row as never
      const [stored] = await batch.db.select().from(shape.table).where(eq(shape.key, id)).limit(1)
      return (stored ?? { ...shape.fresh, [shape.keyName]: id }) as never
    },
    async save(row) {
      kept.set((row as Record<string, unknown>)[shape.keyName], row as Record<string, unknown>)
    },
  }
}

/**
 * The wrapper a host would write next to its `withDb`: puts a repository for
 * each named table on the handler's context, under the name it was given.
 */
export function repositoryHandler<T extends Record<string, PgTable>, M, C extends Repositories<T> & { readonly unitOfWork: UnitOfWork }>(
  next: (message: M, context: C) => Promise<void> | void,
  tables: T,
): (message: M, context: Omit<C, keyof T> & { readonly unitOfWork: UnitOfWork }) => Promise<void> {
  const shapes = Object.entries(tables).map(([name, t]) => [name, shapeOf(t)] as const)
  return async (message, context) => {
    const batch = await batchOf(context.unitOfWork, shapes.map(([, shape]) => shape))
    const repositories = Object.fromEntries(shapes.map(([name, shape]) => [name, repository(shape, batch)]))
    await next(message, { ...context, ...repositories } as unknown as C)
  }
}

// ── the projection, written against it ──────────────────────────────────────

type Ctx = EventHandlerContext & Repositories<{ balances: typeof table }>

export async function repositoryProjection(url: string, uniqueKeys = false): Promise<Projection> {
  const pg = postgresPool(pgAdapter({ connectionString: url }))
  await pg.start()
  await prepareDatabase(pg)

  const onHandled = eventHandler(Handled, async ({ payload, sequence }, ctx: Ctx) => {
    const b = await ctx.balances.get(uniqueKeys ? `${payload.account}#${sequence}` : payload.account)
    await ctx.balances.save({ ...b, total: b.total + payload.amount, count: b.count + 1 })
  })

  return {
    handlers: [{ ...onHandled, handler: repositoryHandler(onHandled.handler, { balances: table }) }],
    unitOfWork: postgresUnitOfWork(unitOfWork, pg),
    tokenStore: postgresTokenStore(pg),
    handled: async () => {
      const rows = await pg.query<{ n: string | null }>(`SELECT sum(count)::text AS n FROM replay_balances`)
      return Number(rows[0]?.n ?? 0)
    },
    close: () => pg.close(),
  }
}
