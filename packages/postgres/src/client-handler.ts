import { describe, type Described } from "@kronos-ts/core"
import type { PostgresCapability } from "./postgres-handler.js"

// ---------------------------------------------------------------------------
// YOUR QUERY BUILDER, OVER THE TASK'S TRANSACTION.
//
// Kronos owns the transaction; `postgresHandler` hands it to a handler as
// `ctx.sql()`. A query builder is a CLIENT you construct over that — never the
// owner of a transaction of its own — and this is the one small step that does
// it: call your builder's constructor on the driver client the transaction
// unwraps to, put the result on `ctx` as `db`, typed exactly as your builder
// types it. `@kronos-ts/postgres/drizzle` and `@kronos-ts/postgres/kysely` are
// this function under the name of the builder they are documented for.
//
// The handle is built once per `ctx.sql()` handle, so a relational schema is
// set up once per task, not once per statement. When the pool is `observed`,
// the client `unwrap()` returns is already bound to the invocation's trace, so
// every statement the builder issues is a span under the right handler — the
// builder never knows.
// ---------------------------------------------------------------------------

/** The capability this step supplies: `ctx.db`, typed as whatever `build` returned. */
export type DbCapability<Db> = {
  readonly db: Db
}

type ClientDescription<Name extends string, M, C, R> = {
  readonly name: Name
  readonly supplies: readonly ["db"]
  readonly uses: readonly ["sql"]
  readonly next: (message: M, context: C) => R
}

/**
 * INTERNAL to this package: the shared body of the builder-named wrappers.
 * `name` is what the boot walk prints.
 */
export function clientHandler<Name extends string>(name: Name) {
  // The context is INFERRED from the handler and CONSTRAINED to demand what
  // this step supplies (`db`). A handler that does not name it, or that was
  // already wrapped, refuses to fit `C`.
  return function handler<Db, Client, M, C extends DbCapability<Db>, R>(
    next: (message: M, context: C) => R,
    build: (client: Client) => Db,
  ): ((message: M, context: Omit<C, "db"> & PostgresCapability) => R) & Described<ClientDescription<Name, M, C, R>> {
    const built = new WeakMap<object, Db>()
    // The handler names only `db`; THIS step is what needs `sql`, so the
    // demand for it appears on the way out — and `postgresHandler`, outside,
    // is what erases it. Wrong order, no compile.
    const wrapped = (message: M, context: Omit<C, "db"> & PostgresCapability): R => {
      const sql = (context as PostgresCapability).sql()
      let db = built.get(sql)
      if (db === undefined) {
        db = build(sql.unwrap<Client>())
        built.set(sql, db)
      }
      return next(message, { ...context, db } as unknown as C)
    }
    return describe(wrapped, { name, supplies: ["db"], uses: ["sql"], next } as const)
  }
}
