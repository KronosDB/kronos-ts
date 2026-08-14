import type { Metadata } from "@kronos-ts/common"
import { append } from "@kronos-ts/eventsourcing/append"
import { load } from "@kronos-ts/eventsourcing/load"
import { schedule, scheduleAfter, cancelSchedule } from "@kronos-ts/eventsourcing/schedule"
import type { z } from "zod"
import type { CommandDescriptor, EventDescriptor, QueryDescriptor } from "./descriptor.js"
import { emitUpdate, type EmitUpdateFunction } from "./emit-update.js"
import { query } from "./query.js"
import { send } from "./send.js"
import { getOrBeginActiveTransaction } from "./transaction.js"

// ---------------------------------------------------------------------------
// Handler contexts — the explicit front door to the ambient UnitOfWork.
//
// The module-level helpers (append/load/send/emitUpdate) resolve their
// dependencies through AsyncLocalStorage, so calling them outside a handler is
// only detectable at runtime (NoActiveUnitOfWork / WrongUoWPhase). The context
// object carries the same capabilities as a typed argument to the handler
// itself: outside a handler there is no context value, so misuse becomes a
// compile error instead of a runtime throw. ALS remains underneath as the
// propagation mechanism — it is what lets non-kronos code (e.g. a drizzle
// transaction manager) enlist in the active UoW — but handlers should not
// need to reach for it.
// ---------------------------------------------------------------------------

/**
 * `append` as a context capability. Structural mirror of
 * `@kronos-ts/eventsourcing`'s AppendFunction (declared here to keep the type
 * surface local to messaging, matching how {@link ContextLoadFunction} mirrors
 * the state-module shape).
 */
export interface ContextAppendFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>): void
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, metadata: Metadata): void
  /** Batch form — `ctx.append([[A, a], [B, b]])`. Same atomic flush. */
  <T extends readonly EventDescriptor<any>[]>(events: {
    [K in keyof T]:
      | readonly [T[K], z.infer<T[K] extends EventDescriptor<infer P> ? P : never>]
      | readonly [T[K], z.infer<T[K] extends EventDescriptor<infer P> ? P : never>, Metadata]
  }): void
}

/**
 * `load` as a context capability. The first signature matches a
 * `StateModule`-shaped object structurally (without importing
 * `@kronos-ts/modelling`, which would invert the dependency direction) so both
 * the id and state types are inferred.
 */
export interface ContextLoadFunction {
  <Id, S>(module: { kind: "state-module"; name: string; create: (id: Id) => S }, id: Id): Promise<S>
  <S>(module: { name: string }, id: unknown): Promise<S>
}

/**
 * `send` as a context capability. Dispatches a command that is handled in its
 * own fresh UnitOfWork (see `send.ts` for the atomic-boundary semantics).
 */
export interface ContextSendFunction {
  <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: CommandDescriptor<P, R>,
    payload: z.infer<P>,
  ): Promise<unknown>
}

/**
 * `query` as a context capability. Consults a query handler — local or across
 * the distributed bus — inside the active UnitOfWork, with correlation
 * metadata carried like `ctx.send`. The AF5 analogue is injecting the
 * QueryGateway into a handler. Prefer a projection or a capability command;
 * reach for this when a decision genuinely needs another module's answer,
 * fresh.
 */
export interface ContextQueryFunction {
  <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: QueryDescriptor<P, R>,
    payload: z.infer<P>,
  ): Promise<R extends z.ZodType ? z.infer<R> : unknown>
}

/**
 * Capabilities available to event handlers running inside a processor's
 * UnitOfWork.
 *
 * Deliberately does NOT include `append`. An automation is a STATEFUL REACTOR:
 * it loads decision state (`ctx.load`) and expresses intent as a COMMAND
 * (`ctx.send`) — and `dispatch` always opens a fresh UnitOfWork, so the command
 * handler is its own atomic decide-and-append boundary with the full DCB
 * treatment. Appending from inside a processor's UnitOfWork would bypass that
 * boundary; the type makes it unrepresentable rather than discouraged.
 */
export interface EventHandlerContext {
  /** Load event-sourced state within the active UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction
  /** Schedule an event for future delivery via the configured event scheduler. */
  readonly schedule: typeof import("@kronos-ts/eventsourcing/schedule").schedule
  /** Schedule an event after a delay. */
  readonly scheduleAfter: typeof import("@kronos-ts/eventsourcing/schedule").scheduleAfter
  /** Cancel a previously scheduled event. */
  readonly cancelSchedule: typeof import("@kronos-ts/eventsourcing/schedule").cancelSchedule
  /** Dispatch a command; it is handled in its own fresh UnitOfWork. */
  readonly send: ContextSendFunction
  /** Consult a query handler (cross-module read) within the active UnitOfWork. */
  readonly query: ContextQueryFunction
  /** Emit a subscription-query update through the active query bus. */
  readonly emitUpdate: EmitUpdateFunction
  /**
   * The active adapter transaction, beginning it lazily if the UnitOfWork has
   * not started one yet. Typed by the configured transaction manager (e.g. a
   * drizzle/postgres transaction). `undefined` when no transactional
   * UnitOfWork factory is configured.
   */
  readonly transaction: <T = unknown>() => Promise<T | undefined>
}

/**
 * Capabilities available to QUERY handlers — read-only by construction.
 *
 * Queries already run inside a UnitOfWork (`simple-query-bus` dispatches
 * through `runInUoW`), so the ambient machinery a context needs is present;
 * what was missing was only the seeding and the argument. Deliberately narrow:
 * no `append` (a query must not write), and no `send` (dispatching a command
 * from a query breaks command/query separation).
 */
export interface QueryHandlerContext {
  /** Load event-sourced state within the active UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction
  /** The active adapter transaction, so a query can read inside it. */
  readonly transaction: <T = unknown>() => Promise<T | undefined>
}

/**
 * Capabilities available to command handlers: everything an event handler has,
 * plus `append` — the command handler is the atomic decide-and-append
 * boundary, and its UnitOfWork flushes buffered events at PREPARE_COMMIT.
 */
export interface HandlerContext extends EventHandlerContext {
  /** Append events to the active UnitOfWork, buffered until commit. */
  readonly append: ContextAppendFunction
}

/**
 * Shared event-handler context instance. Every capability resolves its
 * dependencies from the active UnitOfWork's ALS state at call time, so one
 * frozen instance serves every invocation — no per-dispatch allocation.
 */
export const EVENT_HANDLER_CONTEXT: EventHandlerContext = Object.freeze({
  load,
  schedule,
  scheduleAfter,
  cancelSchedule,
  send,
  query,
  emitUpdate,
  transaction: getOrBeginActiveTransaction,
})

/** Shared query-handler context instance. See {@link EVENT_HANDLER_CONTEXT}. */
export const QUERY_HANDLER_CONTEXT: QueryHandlerContext = Object.freeze({
  load,
  transaction: getOrBeginActiveTransaction,
})

/** Shared command-handler context instance. See {@link EVENT_HANDLER_CONTEXT}. */
export const HANDLER_CONTEXT: HandlerContext = Object.freeze({
  ...EVENT_HANDLER_CONTEXT,
  append,
})
