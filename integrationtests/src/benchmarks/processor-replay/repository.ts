/**
 * THE REPOSITORY PROTOTYPE. User land, like `bulk.ts`: nothing here is a
 * framework API.
 *
 * A handler reads a row and saves it as if it were the table:
 *
 *   const b = await ctx.balances.get(id)
 *   await ctx.balances.save({ ...b, total: b.total + amount })
 *
 * or hands over a fold and does not wait for the row at all:
 *
 *   await ctx.balances.upsert(id, (b) => ({ ...b, total: b.total + amount }))
 *
 * and the batch decides when the table is read and written. `get` answers this
 * batch's copy, else the stored row, else a new row from the table's column
 * defaults. The rows `get`s ask for in one turn of the event loop are read in
 * ONE select, so under `concurrent` — every sequence's first `get` arriving at
 * once — a batch reads in one statement. `save` only replaces the batch's
 * copy. `upsert` queues its fold; in PREPARE_COMMIT the rows it needs are read
 * in one select and the folds run in order. Then every row saved in the batch is
 * written as one upsert per table, before the processor stores its token (it
 * registers that last, and phase actions run in registration order), so rows
 * and token commit or roll back together, and a failed batch's copies go with
 * its unit of work.
 */
import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres"
import { getTableColumns, inArray, is, sql, SQL, type InferSelectModel } from "drizzle-orm"
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
  /** Fold this batch's row when the batch ends. The handler never waits for the row. */
  upsert(id: Row<T>[KeyName<T>], fold: (row: Row<T>) => Row<T>): Promise<void>
}

export type Repositories<T extends Record<string, PgTable>> = { readonly [K in keyof T]: Repository<T[K]> }

type Plain = Record<string, unknown>

/** What a table needs to be kept in a batch: its key, and what a row starts as. */
type Shape = {
  readonly table: PgTable
  readonly key: PgColumn
  readonly keyName: string
  readonly fresh: Plain
  readonly columns: number
  readonly set: Record<string, SQL>
}

function shapeOf(table: PgTable): Shape {
  const columns = Object.entries(getTableColumns(table)) as Array<[string, PgColumn]>
  const keys = columns.filter(([, column]) => column.primary)
  if (keys.length !== 1) throw new Error(`a repository needs a table with a one-column primary key`)
  const [keyName, key] = keys[0]!
  const fresh: Plain = {}
  for (const [name, column] of columns) {
    if (column === key) continue
    // A row nobody has stored yet starts at the column's default. A NOT NULL
    // column without one has nothing to start at, so the table is refused
    // when the handler is wrapped, not on the first event that misses.
    if (column.default !== undefined && !is(column.default, SQL)) fresh[name] = column.default
    else if (!column.notNull) fresh[name] = null
    else throw new Error(`a repository cannot start a row: column "${column.name}" is NOT NULL and has no default`)
  }
  const set = Object.fromEntries(
    columns.filter(([, column]) => column !== key).map(([name, column]) => [name, sql`excluded.${sql.identifier(column.name)}`]),
  )
  return { table, key, keyName, fresh, columns: columns.length, set }
}

/** Postgres allows 65535 parameters in a statement. */
const MAX_PARAMETERS = 65_535

/** One table within one batch. */
type Kept = {
  readonly shape: Shape
  /** The current row for every key the batch has read or saved. */
  readonly rows: Map<unknown, Plain>
  /** Keys whose row the batch has changed. */
  readonly dirty: Set<unknown>
  /** Keys asked for in this turn of the event loop, and who is waiting. */
  readonly asked: Map<unknown, Array<Waiting>>
  /** Folds waiting for the end of the batch, per key, in the order they came. */
  readonly folds: Map<unknown, Array<(row: Plain) => Plain>>
  reading: boolean
}

type Waiting = { readonly answer: (row: Plain) => void; readonly fail: (error: unknown) => void }

/** One batch: its client, and what it keeps of each table. */
type Batch = { readonly db: NodePgDatabase; readonly tables: Map<PgTable, Kept> }

async function read(db: NodePgDatabase, kept: Kept, keys: unknown[]): Promise<void> {
  const { shape } = kept
  const found = new Map<unknown, Plain>()
  for (let from = 0; from < keys.length; from += MAX_PARAMETERS) {
    const rows = (await db
      .select()
      .from(shape.table)
      .where(inArray(shape.key, keys.slice(from, from + MAX_PARAMETERS)))) as Plain[]
    for (const row of rows) found.set(row[shape.keyName], row)
  }
  // A row saved while this read was on the wire is newer than what was read.
  for (const key of keys) if (!kept.rows.has(key)) kept.rows.set(key, found.get(key) ?? { ...shape.fresh, [shape.keyName]: key })
}

/** Read everything asked for in this turn, in one select, and answer the askers. */
function readAsked(db: NodePgDatabase, kept: Kept): void {
  if (kept.reading) return
  kept.reading = true
  setImmediate(async () => {
    const asked = new Map(kept.asked)
    kept.asked.clear()
    kept.reading = false
    try {
      await read(db, kept, [...asked.keys()])
      for (const [key, waiting] of asked) for (const { answer } of waiting) answer(kept.rows.get(key)!)
    } catch (error) {
      // Every handler waiting on the read fails, and with it the batch.
      for (const waiting of asked.values()) for (const { fail } of waiting) fail(error)
    }
  })
}

async function write(db: NodePgDatabase, kept: Kept): Promise<void> {
  const { shape } = kept
  const pending = [...kept.folds.keys()].filter((key) => !kept.rows.has(key))
  if (pending.length > 0) await read(db, kept, pending)
  for (const [key, folds] of kept.folds) {
    kept.rows.set(key, folds.reduce((row, fold) => fold(row), kept.rows.get(key)!))
    kept.dirty.add(key)
  }
  kept.folds.clear()
  if (kept.dirty.size === 0) return
  const all = [...kept.dirty].map((key) => kept.rows.get(key)!)
  const perStatement = Math.floor(MAX_PARAMETERS / shape.columns)
  for (let from = 0; from < all.length; from += perStatement) {
    await db
      .insert(shape.table)
      .values(all.slice(from, from + perStatement) as never)
      .onConflictDoUpdate({ target: shape.key, set: shape.set })
  }
}

const batches = new WeakMap<UnitOfWork, Promise<Batch>>()

function batchOf(uow: UnitOfWork, shapes: ReadonlyArray<Shape>): Promise<Batch> {
  let batch = batches.get(uow)
  if (batch === undefined) {
    batch = (async () => {
      const tx = await postgresTransaction(uow)
      const opened: Batch = {
        db: drizzle(tx.unwrap<PoolClient>()),
        tables: new Map(
          shapes.map((shape) => [shape.table, { shape, rows: new Map(), dirty: new Set(), asked: new Map(), folds: new Map(), reading: false }]),
        ),
      }
      uow.onPrepareCommit(async () => {
        for (const kept of opened.tables.values()) await write(opened.db, kept)
      })
      return opened
    })()
    batches.set(uow, batch)
  }
  return batch
}

function repository(kept: Kept, db: NodePgDatabase): Repository<PgTable> {
  const { shape } = kept
  /** The row as the batch has it now, its queued folds applied. */
  async function current(id: unknown): Promise<Plain> {
    let row = kept.rows.get(id)
    if (row === undefined) {
      row = await new Promise<Plain>((answer, fail) => {
        const waiting = kept.asked.get(id)
        if (waiting) waiting.push({ answer, fail })
        else kept.asked.set(id, [{ answer, fail }])
        readAsked(db, kept)
      })
    }
    const folds = kept.folds.get(id)
    if (folds) {
      row = folds.reduce((r, fold) => fold(r), row)
      kept.folds.delete(id)
      kept.rows.set(id, row)
      kept.dirty.add(id)
    }
    return row
  }
  return {
    get: (id) => current(id) as never,
    async save(row) {
      const key = (row as Plain)[shape.keyName]
      kept.rows.set(key, row as Plain)
      kept.dirty.add(key)
    },
    async upsert(id, fold) {
      const folds = kept.folds.get(id)
      if (folds) folds.push(fold as never)
      else kept.folds.set(id, [fold as never])
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
    const repositories = Object.fromEntries(shapes.map(([name, shape]) => [name, repository(batch.tables.get(shape.table)!, batch.db)]))
    await next(message, { ...context, ...repositories } as unknown as C)
  }
}

// ── the projection, written against it ──────────────────────────────────────

type Ctx = EventHandlerContext & Repositories<{ balances: typeof table }>

/** `get-save` reads the row in the handler; `upsert` hands over the fold. */
export async function repositoryProjection(url: string, uniqueKeys = false, style: "get-save" | "upsert" = "get-save"): Promise<Projection> {
  const pg = postgresPool(pgAdapter({ connectionString: url }))
  await pg.start()
  await prepareDatabase(pg)

  const onHandled =
    style === "upsert"
      ? eventHandler(Handled, async ({ payload, sequence }, ctx: Ctx) => {
          await ctx.balances.upsert(uniqueKeys ? `${payload.account}#${sequence}` : payload.account, (b) => ({
            ...b,
            total: b.total + payload.amount,
            count: b.count + 1,
          }))
        })
      : eventHandler(Handled, async ({ payload, sequence }, ctx: Ctx) => {
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
