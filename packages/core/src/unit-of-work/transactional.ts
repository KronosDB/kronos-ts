import type { UnitOfWork } from "./unit-of-work.js"

// ---------------------------------------------------------------------------
// A FACTORY THAT SAYS ITS TASKS TRANSACT.
//
// A transaction family (`drizzleUnitOfWork`, `postgresUnitOfWork`, …) returns
// a unit-of-work factory whose every task opens a transaction. That is what a
// command bus and a processor want, and what a query bus must NOT be built
// from: a read needs no transaction, and a transactional query bus wraps every
// `SELECT` in `BEGIN`/`COMMIT` and holds a pooled connection per in-flight
// query — so a command awaiting a nested query holds two, which deadlocks the
// pool at concurrency ≥ pool size. The families mark their factories; the
// query bus refuses the mark, at compile time and at construction.
// ---------------------------------------------------------------------------

/** The key the mark lives under. */
export const TRANSACTIONAL: unique symbol = Symbol.for("kronos.unitOfWork.transactional")

/** A factory marked by a transaction family. */
export type Transactional = { readonly [TRANSACTIONAL]: true }

/**
 * `F` itself when it is not transactional; otherwise the sentence the
 * compiler prints. Used as `unitOfWork: F & RefusingTransactional<F>`.
 */
export type RefusingTransactional<F> = F extends Transactional
  ? "a query bus must be built from the plain unitOfWork factory — a read needs no transaction, and a query always runs in a task of its own (see QueryBus.query)"
  : unknown

/**
 * A factory that mints exactly what `factory` mints and carries the mark.
 * Every transaction family calls this on what it returns. It is a NEW
 * function, so the one handed in is untouched — marking a shared factory
 * (the `unitOfWork` export itself, say) must not turn it transactional
 * everywhere it is used.
 */
export function transactional<F extends () => UnitOfWork>(factory: F): F & Transactional {
  const marked = (() => factory()) as F
  Object.defineProperty(marked, TRANSACTIONAL, { value: true, enumerable: false, writable: false })
  return marked as F & Transactional
}

/** Was `factory` marked by a transaction family? */
export function isTransactional(factory: unknown): boolean {
  return typeof factory === "function" && (factory as { [TRANSACTIONAL]?: true })[TRANSACTIONAL] === true
}
