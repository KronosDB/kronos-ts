import { loadFunction, sourceFunction, type SnapshotReads } from "../event-sourcing/load.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { queryFunction } from "./query.js"
import type {
  ContextLoadFunction,
  ContextQueryFunction,
  ContextSourceFunction,
  HandlerContextDeps,
} from "../command-handling/context.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// The QUERY handler's context. See `command-handling/context.ts` for what a
// context is, and for the capability types all three kinds are built from.
// ---------------------------------------------------------------------------

/**
 * Capabilities available to QUERY handlers — read-only by construction.
 *
 * Deliberately narrow: no `append` (a query must not write), and no `send`
 * (dispatching a command from a query breaks command/query separation).
 * `query` IS here: a read composing another module's read stays a read.
 */
export type QueryHandlerContext<
  E extends EventStore = EventStore,
  U extends UnitOfWork = UnitOfWork,
> = QueryHandlerContextBase<E, U> & SnapshotReads<E>

/** The always-there part; see `EventHandlerContext` for why it is split out. */
type QueryHandlerContextBase<
  E extends EventStore = EventStore,
  U extends UnitOfWork = UnitOfWork,
> = {
  /** Load event-sourced state within this UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction<E>
  /**
   * THE RAW LAYER under `load`: run an event query against this entry's log and
   * fold the result yourself. A pure read here — a query handling has no
   * `append` to condition — but the same read, off the same store.
   */
  readonly source: ContextSourceFunction
  /** Consult a query handler (cross-module read) within this UnitOfWork. */
  readonly query: ContextQueryFunction
  /**
   * The unit of work this context was built for — see the note on
   * `EventHandlerContext.unitOfWork` for why it is parametric.
   */
  readonly unitOfWork: U
}

/** Build the QUERY handler context for one invocation. */
export function queryHandlerContext<U extends UnitOfWork, E extends EventStore = EventStore>(
  deps: HandlerContextDeps<U, E>,
): QueryHandlerContext<E, U> {
  const { uow } = deps
  return {
    load: loadFunction(deps) as ContextLoadFunction<E>,
    source: sourceFunction(deps),
    query: queryFunction(deps) as ContextQueryFunction,
    unitOfWork: uow,
  } as QueryHandlerContext<E, U>
}
