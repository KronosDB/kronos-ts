/**
 * The postgres family's transaction seam: one unit-of-work factory and the
 * TYPED accessor pair that reads what it opened.
 *
 * `adapter.transaction(IL, fn)` opens a pg tx, runs `fn(tx)`, and COMMITs on
 * fn-resolve / ROLLBACKs on fn-reject. A unit of work needs a transaction whose
 * commit/rollback is callable LATER (at the COMMIT phase, or on error), not
 * when `fn` returns. The bridge parks `fn` on a deferred completion promise —
 * commit resolves it (→ adapter commits), rollback rejects it (→ adapter rolls
 * back). That bridge is private: this package ships the FINISHED factory, so
 * there is no `TransactionManager` for a host to implement or pass in.
 */

import type { UnitOfWorkBrand, UnitOfWork } from "@kronos-ts/core"
import {
  activeTransaction,
  adapterUnitOfWork,
  claimed,
  openTransaction,
  transactionRegistry,
} from "./transaction-glue.js"
import type { PostgresAdapter, PostgresAdapterTransaction } from "./adapter.js"
import { IsolationLevel } from "./adapter.js"

/**
 * THE POSTGRES FAMILY MARK — a phantom, type-only brand on every unit of work
 * postgresUnitOfWork(next, pg) mints, and the thing this package's token store and
 * dead-letter queue demand back.
 *
 * WHY IT EXISTS. This family is keyed by TRANSACTION IDENTITY: the token store,
 * the dead-letter queue and what a raw-SQL handler writes through `ctx.sql()` must all
 * write through the SAME handle, or they do not commit together. Handing this
 * package's token store a unit of work from another family does not throw — the
 * store looks for ITS transaction, does not find one, and falls back to its
 * plain handle, so the token update commits OUTSIDE the batch. Every test
 * passes; then a crash lands between the projection write and the token write
 * and a read model is permanently wrong. The mark turns that into a build
 * error.
 *
 * IT IS ERASED AND NEVER CONSTRUCTED. `UnitOfWorkBrand` hangs on an ambient
 * unique symbol declared in core; nothing writes the property and nothing can
 * read it. postgresUnitOfWork(…) returns exactly what it always
 * returned and asserts the branded type, so the emitted JavaScript is
 * unchanged.
 *
 * THE FIX STRING IS THIS PACKAGE'S TO WRITE, and that is the point of putting
 * it here. Core can only say "these two are different families"; this package
 * knows precisely which factory the host should have called, so a mismatch
 * prints that sentence at the wiring site.
 */
export type PostgresUnitOfWork = UnitOfWorkBrand<
  "postgres",
  "build this processor's unitOfWork with postgresUnitOfWork(next, pg) — this family's stores write through its transaction"
>

/**
 * Module-private symbol attaching commit/rollback control to a tx handle.
 * Consumers reading {@link activePostgresTransaction} see only `{ query,
 * unwrap }` — they cannot reach this without importing the symbol.
 */
const TX_CONTROL = Symbol("kronos.postgresTxControl")

type TxControl = {
  readonly resolveCommit: () => void
  readonly rejectRollback: (err: unknown) => void
  readonly txPromise: Promise<void>
}

type ManagedPostgresTransaction = PostgresAdapterTransaction & {
  [TX_CONTROL]: TxControl
}

/** Marker error: signals an intentional rollback so the .catch can suppress it. */
const ROLLBACK_MARKER = "__kronos_postgres_tx_rollback__"

/**
 * This package's PRIVATE table of transactions, keyed by unit of work.
 *
 * The base `UnitOfWork` has no transaction concept, so this is where a postgres
 * transaction actually lives — and the only things that can read it are the
 * accessors below. That is what makes them TYPED: the type comes from the
 * adapter that owns the driver, not from an assertion at the call site.
 */
const registry = transactionRegistry<PostgresAdapterTransaction>()

/** The three calls the shared glue makes. Not a public concept. */
function txHooks(pg: PostgresAdapter, isolationLevel: IsolationLevel) {
  return {
    async begin(): Promise<PostgresAdapterTransaction> {
      let captureTx!: (tx: PostgresAdapterTransaction) => void
      const txReady = new Promise<PostgresAdapterTransaction>((res) => {
        captureTx = res
      })

      let resolveCommit!: () => void
      let rejectRollback!: (err: unknown) => void
      const completion = new Promise<void>((res, rej) => {
        resolveCommit = res
        rejectRollback = rej
      })

      const txPromise = pg
        .transaction(isolationLevel, async (tx) => {
          // Per-transaction safety timeouts are armed by the adapter's
          // transaction() at BEGIN (see session-timeouts.ts), so they cover
          // this UoW-scoped tx and every ad-hoc adapter.transaction() alike.
          captureTx(tx)
          await completion
        })
        .then(
          () => undefined,
          (err) => {
            // Suppress the marker — rollback is an expected outcome.
            if (err instanceof Error && err.message === ROLLBACK_MARKER) return
            throw err
          },
        )

      // If the transaction callback fails before it hands back the tx — BEGIN
      // itself failing, or arming the safety timeouts throwing — `captureTx`
      // never runs and `txReady` would never resolve. Race it against
      // `txPromise` so an early failure rejects begin() instead of hanging it
      // forever. In the happy path `txPromise` stays pending (parked on
      // `completion` until commit/rollback), so `txReady` always wins.
      const tx = (await Promise.race([txReady, txPromise])) as
        | ManagedPostgresTransaction
        | undefined
      if (tx === undefined) {
        // txPromise settled first by resolving — the tx ended before begin()
        // returned, so the handle is unusable. Surface rather than return it.
        throw new Error("postgresUnitOfWork: transaction ended before it was opened")
      }
      tx[TX_CONTROL] = { resolveCommit, rejectRollback, txPromise }
      return tx
    },

    async commit(tx: PostgresAdapterTransaction): Promise<void> {
      const ctrl = (tx as ManagedPostgresTransaction)[TX_CONTROL]
      ctrl.resolveCommit()
      await ctrl.txPromise
    },

    async rollback(tx: PostgresAdapterTransaction): Promise<void> {
      const ctrl = (tx as ManagedPostgresTransaction)[TX_CONTROL]
      ctrl.rejectRollback(new Error(ROLLBACK_MARKER))
      try {
        await ctrl.txPromise
      } catch (err) {
        // A real follow-up error during ROLLBACK execution. Don't throw
        // from rollback() — the unit of work is already in an error path and a
        // cascading throw masks the original failure.
        console.warn("postgresUnitOfWork: rollback path threw:", err)
      }
    },
  }
}

/**
 * The unit-of-work factory this package exports: every unit of work it
 * mints runs inside one postgres transaction.
 *
 * Same shape as the core `unitOfWork`, so it drops into any seam that takes one:
 *
 * ```ts
 * const uow = postgresUnitOfWork(unitOfWork, pg)
 * const commandBus = interceptingCommandBus(localCommandBus(uow), correlation)
 * eventProcessor({ name, eventStore, tokenStore, unitOfWork: uow })
 * ```
 *
 * LAZY, unlike the ORM families' eager ones — and that is postgres's HONEST
 * default, not a tuning choice. The unit of work is claimed but no transaction
 * is opened, so a pure-read unit of work (a query, a projection that only
 * reads) never claims a pool connection. The first writer opens it via
 * {@link postgresTransaction}, and from there everything in that unit of work —
 * appended events, scheduled events, token updates, dead letters, and the
 * handler's own writes through `ctx.sql()` — commits or rolls back together.
 *
 * THING-FIRST: `next` — the factory the unit of work is minted from — comes
 * first, because that is the thing being decorated, and the client is the
 * configuration. It is required and never defaulted: a default hides the
 * composition chain, and the whole point of this shape is that a call site
 * shows what its units of work are made of.
 *
 * It is also CAPABILITY-PRESERVING. The decorator returns `() => U` for
 * whatever `U` it was handed, and it decorates the SAME handle rather than
 * rebuilding a record from it — so a composed capability survives both the type
 * and the runtime:
 *
 * ```ts
 * const uow = postgresUnitOfWork(() => correlating(unitOfWork(clock)), pg)
 * //    ^ () => CorrelatingUnitOfWork, and its transactions are keyed on that
 * //      very object, which is the one `ctx.unitOfWork` hands back
 * ```
 */
export function postgresUnitOfWork<U extends UnitOfWork = UnitOfWork>(
  next: () => U,
  pg: PostgresAdapter,
  isolationLevel: IsolationLevel = IsolationLevel.READ_COMMITTED,
): () => U & PostgresUnitOfWork {
  return adapterUnitOfWork(registry, txHooks(pg, isolationLevel), next) as () => U & PostgresUnitOfWork
}

/**
 * The postgres transaction `uow` is running in, OPENING one if it has not begun.
 *
 * This is the postgres family's opener — the lazy factory deliberately begins
 * nothing, so the first writer to call this is what claims a pool connection.
 * Rejects when `uow` did not come from {@link postgresUnitOfWork}: asking a
 * foreign unit of work for a transaction is a wiring mistake, and answering
 * `undefined` would push it downstream as a silent non-transactional write.
 */
export function postgresTransaction(uow: UnitOfWork): Promise<PostgresAdapterTransaction> {
  return openTransaction(registry, uow, "postgresUnitOfWork")
}

/**
 * The postgres transaction `uow` is already running in, or `undefined` — NEVER
 * opens one.
 *
 * What a token store, a dead-letter queue or a projection writer wants:
 *
 * ```ts
 * const write = activePostgresTransaction(ctx.unitOfWork) ?? pg
 * ```
 */
export function activePostgresTransaction(
  uow: UnitOfWork | undefined,
): PostgresAdapterTransaction | undefined {
  return activeTransaction(registry, uow)
}

/**
 * INTERNAL to this package. This unit of work's shared postgres transaction, or
 * `undefined` when there is none to share — i.e. no unit of work, or one this
 * family did not mint.
 *
 * Unlike {@link activePostgresTransaction} this OPENS the transaction when the
 * unit of work is ours and lazy-unopened, which is what the package's own write
 * paths (event store append, scheduler insert) want: join the caller's
 * transaction if there is one to join, otherwise say so and let the caller open
 * its own.
 */
export async function sharedPostgresTransaction(
  uow: UnitOfWork | undefined,
): Promise<PostgresAdapterTransaction | undefined> {
  if (uow === undefined || !claimed(registry, uow)) return undefined
  return postgresTransaction(uow)
}
