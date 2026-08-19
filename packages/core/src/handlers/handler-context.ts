import type { Metadata } from "../primitives/metadata.js"
import { appendFunction } from "../state/append.js"
import { loadFunction, type StateManagerLike } from "../state/load.js"
import { scheduleFunctions } from "../state/schedule.js"
import type { z } from "zod"
import type { CommandBus } from "../buses/command-bus.js"
import type { CommandDescriptor, EventDescriptor, QueryDescriptor } from "../messages/descriptor.js"
import { emitUpdateFunction, type EmitUpdateFunction } from "../buses/emit-update.js"
import type { EventScheduler, ScheduleToken, CancelResult } from "../processor/event-scheduler.js"
import type { Message } from "../messages/message.js"
import type { QueryBus } from "../buses/query-bus.js"
import { queryFunction } from "./ctx-query.js"
import { sendFunction } from "./ctx-send.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// Handler contexts — the unit of work, handed down.
//
// A context is built FRESH per invocation as a closure over that invocation's
// unit of work, the buses the caller already holds, and the item's stores. It
// is the only channel: there is no ambient state a capability can fall back on,
// so a capability used outside a handler is a compile error rather than a
// runtime throw, and a context outliving its unit of work throws
// NoActiveUnitOfWork off the `closed` flag.
// ---------------------------------------------------------------------------

/**
 * `append` as a context capability. Structural mirror of
 * the state layer's AppendFunction (declared here to keep the type
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
 * the state model, which would invert the dependency direction) so both
 * the id and state types are inferred.
 */
export interface ContextLoadFunction {
  <Id, S>(
    module: { kind: "state-module"; identity: string; create: (id: Id) => S },
    id: Id,
  ): Promise<S>
  <S>(module: { identity: string }, id: unknown): Promise<S>
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
 * the distributed bus — inside the handler's own UnitOfWork, with correlation
 * metadata carried like `ctx.send`. The AF5 analogue is injecting the
 * query gateway into a handler. Prefer a projection or a capability command;
 * reach for this when a decision genuinely needs another module's answer,
 * fresh.
 */
export interface ContextQueryFunction {
  <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
    descriptor: QueryDescriptor<P, R>,
    payload: z.infer<P>,
  ): Promise<R extends z.ZodType ? z.infer<R> : unknown>
}

/** `schedule` as a context capability. */
export interface ContextScheduleFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, at: Date): Promise<ScheduleToken>
  <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    at: Date,
    metadata: Metadata,
  ): Promise<ScheduleToken>
}

/** `scheduleAfter` as a context capability. */
export interface ContextScheduleAfterFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, delayMs: number): Promise<ScheduleToken>
  <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    delayMs: number,
    metadata: Metadata,
  ): Promise<ScheduleToken>
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
  /** Load event-sourced state within this UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction
  /** Schedule an event for future delivery via the configured event scheduler. */
  readonly schedule: ContextScheduleFunction
  /** Schedule an event after a delay. */
  readonly scheduleAfter: ContextScheduleAfterFunction
  /** Cancel a previously scheduled event. */
  readonly cancelSchedule: (token: ScheduleToken) => Promise<CancelResult>
  /** Dispatch a command; it is handled in its own fresh UnitOfWork. */
  readonly send: ContextSendFunction
  /** Consult a query handler (cross-module read) within this UnitOfWork. */
  readonly query: ContextQueryFunction
  /** Emit a subscription-query update through the query bus, after commit. */
  readonly emitUpdate: EmitUpdateFunction
  /**
   * True while the owning processor is replaying history. Use it to skip
   * side effects (emails, webhooks) that must not fire twice.
   */
  readonly isReplay: () => boolean
  /** Merge extra lineage keys onto this UnitOfWork (e.g. a `traceparent`). */
  readonly contributeCorrelationData: (partial: Record<string, string>) => void
  /**
   * The unit of work this context was built for.
   *
   * This is how a handler reaches a transaction: hand it to the owning
   * adapter's accessor — `activeDrizzleTransaction(ctx.unitOfWork) ?? db` to
   * read or write inside whatever this unit of work already opened, or
   * `await drizzleTransaction(ctx.unitOfWork)` to open one. There is no
   * `ctx.transaction`, because the context cannot type what it does not own.
   */
  readonly unitOfWork: UnitOfWork
}

/**
 * Capabilities available to QUERY handlers — read-only by construction.
 *
 * Deliberately narrow: no `append` (a query must not write), and no `send`
 * (dispatching a command from a query breaks command/query separation).
 * `query` IS here: a read composing another module's read stays a read.
 */
export interface QueryHandlerContext {
  /** Load event-sourced state within this UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction
  /** Consult a query handler (cross-module read) within this UnitOfWork. */
  readonly query: ContextQueryFunction
  /** Merge extra lineage keys onto this UnitOfWork (e.g. a `traceparent`). */
  readonly contributeCorrelationData: (partial: Record<string, string>) => void
  /**
   * The unit of work this context was built for.
   *
   * This is how a handler reaches a transaction: hand it to the owning
   * adapter's accessor — `activeDrizzleTransaction(ctx.unitOfWork) ?? db` to
   * read or write inside whatever this unit of work already opened, or
   * `await drizzleTransaction(ctx.unitOfWork)` to open one. There is no
   * `ctx.transaction`, because the context cannot type what it does not own.
   */
  readonly unitOfWork: UnitOfWork
}

/**
 * Capabilities available to command handlers: everything an event handler has,
 * plus `append` — the command handler is the atomic decide-and-append
 * boundary, and its UnitOfWork flushes buffered events at PREPARE_COMMIT.
 */
export interface HandlerContext extends EventHandlerContext {
  /** Append events to this UnitOfWork, buffered until commit. */
  readonly append: ContextAppendFunction
}

/**
 * Everything a context is built from. The handling modules and processors hold
 * these already — the stores of the item being invoked, the buses they were
 * constructed with — so building a context is a closure, not a lookup.
 */
export interface HandlerContextDeps {
  readonly uow: UnitOfWork
  /**
   * The message being handled by THIS invocation — the causing message for
   * everything the handler sends, appends, queries or schedules.
   *
   * It lives on the binding rather than on the unit of work because a unit of
   * work is scoped to a task and a task is not always one message: a processor
   * batch is one unit of work over many events, each of which gets its own
   * context carrying its own message. Omitted only where a capability is used
   * with no causing message at all (a bare test harness), in which case the
   * base metadata is empty.
   */
  readonly message?: Message
  readonly stateManager?: StateManagerLike
  readonly commandBus?: CommandBus
  readonly queryBus?: QueryBus
  readonly eventScheduler?: EventScheduler
}

/** Build the QUERY handler context for one invocation. */
export function queryHandlerContext(deps: HandlerContextDeps): QueryHandlerContext {
  const { uow } = deps
  return {
    load: loadFunction(deps) as ContextLoadFunction,
    query: queryFunction(deps) as ContextQueryFunction,
    contributeCorrelationData: (partial) => uow.contributeCorrelationData(partial),
    unitOfWork: uow,
  }
}

/** Build the EVENT handler context for one invocation. */
export function eventHandlerContext(deps: HandlerContextDeps): EventHandlerContext {
  const { uow } = deps
  const scheduling = scheduleFunctions(deps)
  return {
    load: loadFunction(deps) as ContextLoadFunction,
    schedule: scheduling.schedule as ContextScheduleFunction,
    scheduleAfter: scheduling.scheduleAfter as ContextScheduleAfterFunction,
    cancelSchedule: scheduling.cancelSchedule,
    send: sendFunction(deps) as ContextSendFunction,
    query: queryFunction(deps) as ContextQueryFunction,
    emitUpdate: emitUpdateFunction(deps),
    isReplay: () => uow.replaying,
    contributeCorrelationData: (partial) => uow.contributeCorrelationData(partial),
    unitOfWork: uow,
  }
}

/** Build the COMMAND handler context for one invocation. */
export function handlerContext(deps: HandlerContextDeps): HandlerContext {
  return {
    ...eventHandlerContext(deps),
    append: appendFunction(deps) as ContextAppendFunction,
  }
}
