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
 * The runtime side of the same story is in `logging-handler.test.ts`.
 */
import type { CommandHandlerContext } from "../../command-handling/context.js"
import type { CommandMessage } from "../../messaging/messages.js"
import { type LogCapability, loggingHandler } from "../logging-handler.js"
import type { Logger } from "../logger.js"

declare const logger: Logger

/** A slice-side handler, annotated the way a slice annotates: `ctx: CommandHandlerContext & LogCapability`. */
declare const asksForLog: (
  message: CommandMessage,
  ctx: CommandHandlerContext & LogCapability,
) => Promise<void>

// ---------------------------------------------------------------------------
// (a) DIRECTIONAL ERASURE — log goes in, the base context comes out.
// ---------------------------------------------------------------------------

const supplied = loggingHandler(asksForLog, logger)

/** The wrapped handler asks only for the BASE context — no `log` in its demand. */
export const base: (message: CommandMessage, ctx: CommandHandlerContext) => Promise<void> = supplied

// ---------------------------------------------------------------------------
// (b) ORDER — a capability-demanding wrapper ordered wrong is a compile error.
// ---------------------------------------------------------------------------

// @ts-expect-error — `supplied` no longer asks for `log`, so there is nothing left to supply
export const twice = loggingHandler(supplied, logger)

// @ts-expect-error — a handler that never asked for `log` cannot have it supplied
export const never = loggingHandler(async (_m: CommandMessage, _ctx: CommandHandlerContext) => {}, logger)

/**
 * A capability-agnostic wrapper — the shape every other function-level
 * wrapper has that erases nothing, so it composes on EITHER side of
 * `loggingHandler`.
 */
declare function counting<M, C, R>(next: (m: M, c: C) => R): (m: M, c: C) => R

export const outside = counting(loggingHandler(asksForLog, logger))
export const inside = loggingHandler(counting(asksForLog), logger)
