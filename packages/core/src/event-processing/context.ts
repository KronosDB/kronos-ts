import { loadFunction, sourceFunction, type SnapshotReads } from "../event-sourcing/load.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { scheduleFunctions, type ScheduleVerbs } from "../event-scheduling/schedule.js"
import { queryFunction } from "../query-handling/query.js"
import { sendFunction } from "../command-handling/send.js"
import { emitUpdateFunction, type EmitUpdateFunction } from "../query-handling/emit-update.js"
import type {
  ContextLoadFunction,
  ContextQueryFunction,
  ContextSourceFunction,
  ContextSendFunction,
  HandlerContextDeps,
} from "../command-handling/context.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// The EVENT handler's context. See `command-handling/context.ts` for what a
// context is, and for the capability types all three kinds are built from.
// ---------------------------------------------------------------------------

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
export type EventHandlerContext<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = EventHandlerContextBase<U, E> & SnapshotReads<E> & ScheduleVerbs<E>

/**
 * The context every handling gets, before the entry's log has been asked what
 * it can do.
 *
 * A CONTEXT IS ASSEMBLED BY INTERSECTION. This is the part that is always
 * there; the other members are what a capable log adds — `SnapshotReads<E>`
 * contributes the fused read, `ScheduleVerbs<E>` the three scheduling verbs.
 * Splitting them is what makes each one structurally ABSENT against a log that
 * cannot serve it rather than present-and-complaining, and the pattern is the
 * seam any later capability tier extends: derive a face from the tier's own
 * `If…Capable` anchor and intersect it here.
 *
 * THERE ARE TWO STORE TIERS TODAY and both are visible on this line. Nothing
 * about the construction is per-tier; a third would be a third intersection
 * member and nothing else would move.
 */
type EventHandlerContextBase<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = {
  /** Load event-sourced state within this UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction<E>
  /**
   * THE RAW LAYER under `load`: run an event query against this entry's log and
   * fold the result yourself. The read is recorded on the unit of work the same
   * way `load`'s is.
   */
  readonly source: ContextSourceFunction
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
  /**
   * The unit of work this context was built for.
   *
   * This is how a handler reaches a transaction: hand it to the owning
   * adapter's accessor — `activeDrizzleTransaction(ctx.unitOfWork) ?? db` to
   * read or write inside whatever this unit of work already opened, or
   * `await drizzleTransaction(ctx.unitOfWork)` to open one. There is no
   * `ctx.transaction`, because the context cannot type what it does not own.
   *
   * It is TYPED as whatever the seam's unit-of-work factory mints — `U`, which
   * defaults to the bare {@link UnitOfWork} and is threaded here by the bus or
   * processor that opened the task. That is what lets a handler DEMAND a
   * composed capability: a handler annotated
   * `ctx: HandlerContext<CorrelatingUnitOfWork>` (which is what
   * `correlatingHandler` produces) does not typecheck against a bus built from
   * a bare `() => unitOfWork()`.
   */
  readonly unitOfWork: U
}

/** Build the EVENT handler context for one invocation. */
export function eventHandlerContext<U extends UnitOfWork, E extends EventStore = EventStore>(
  deps: HandlerContextDeps<U, E>,
): EventHandlerContext<U, E> {
  const { uow } = deps
  // ONE `source` FUNCTION, BOTH SHAPES — and THREE scheduling verbs, built
  // whether or not this entry's log can serve them. The demands are types, and
  // types are erased: what is built here is exactly what was built before any
  // of this existed, and what a handler is ALLOWED to reach was settled by the
  // compiler long before this line ran. A JavaScript caller who reaches one of
  // the verbs against a bare log gets the named error from `requireScheduling`.
  return {
    load: loadFunction(deps) as ContextLoadFunction<E>,
    source: sourceFunction(deps),
    ...scheduleFunctions(deps),
    send: sendFunction(deps) as ContextSendFunction,
    query: queryFunction(deps) as ContextQueryFunction,
    emitUpdate: emitUpdateFunction(deps),
    isReplay: () => uow.replaying,
    unitOfWork: uow,
  } as EventHandlerContext<U, E>
}
