/**
 * The TYPE test for the function-level wrapper shape.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * (where the package build and the published `files` list already drop it) and
 * is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red — the only way a "this must not compile" claim
 * can be honest.
 *
 * The runtime side of the same story is in `drizzle-transaction.test.ts`.
 */
import type { CommandHandler, CommandMessage, HandlerContext } from "@kronos-ts/core"
import { type DrizzleContext, type DrizzleDb, drizzleHandler } from "../drizzle-transaction.js"

declare const db: DrizzleDb

/** A slice-side handler, annotated the way a slice annotates: `ctx: DrizzleContext`. */
declare const asksForDb: (message: CommandMessage, ctx: DrizzleContext) => Promise<void>

// ---------------------------------------------------------------------------
// (a) DIRECTIONAL ERASURE — db() goes in, the base context comes out.
// ---------------------------------------------------------------------------

const supplied = drizzleHandler(asksForDb, db)

/** The wrapped handler asks only for the BASE context… */
export const base: (message: CommandMessage, ctx: HandlerContext) => Promise<void> = supplied

/** …which is exactly what lets the host drop it into an entry unchanged. */
export const entry: CommandHandler = {
  kind: "command-handler",
  descriptor: {} as never,
  handler: supplied,
}

// ---------------------------------------------------------------------------
// (b) ORDER — a capability-demanding wrapper ordered wrong is a compile error.
// ---------------------------------------------------------------------------

// @ts-expect-error — `supplied` no longer asks for db(), so there is nothing left to supply
export const twice = drizzleHandler(supplied, db)

// @ts-expect-error — a handler that never asked for db() cannot have it supplied
export const never = drizzleHandler(async (_m: CommandMessage, _ctx: HandlerContext) => {}, db)

/**
 * A capability-agnostic wrapper — the shape every non-persistence wrapper has
 * (tracing, metering, logging). It erases nothing, so it composes on EITHER
 * side of `drizzleHandler`. That is the point of the constraint: what pins the
 * order is the ERASURE, not the wrapping.
 */
declare function counting<M, C, R>(next: (m: M, c: C) => R): (m: M, c: C) => R

export const outside = counting(drizzleHandler(asksForDb, db))
export const inside = drizzleHandler(counting(asksForDb), db)
