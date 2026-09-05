import { loadFunction, sourceFunction } from "../event-sourcing/load.js"
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
 *
 * NO STORE PARAMETER. No tier adds a member to a QUERY context: snapshotting
 * is served by the store to `ctx.load` without a handler naming it, and a read
 * gives birth to nothing, so the scheduling tier never reaches here. The entry
 * still carries its log — `ctx.load` sources from it — but nothing about the
 * log's tiers changes this shape.
 */
export type QueryHandlerContext<U extends UnitOfWork = UnitOfWork> = {
  /** Load event-sourced state within this UnitOfWork (cached per UoW). */
  readonly load: ContextLoadFunction
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
export function queryHandlerContext<U extends UnitOfWork>(
  deps: HandlerContextDeps<U>,
): QueryHandlerContext<U> {
  const { uow } = deps
  return {
    load: loadFunction(deps),
    source: sourceFunction(deps),
    query: queryFunction(deps) as ContextQueryFunction,
    unitOfWork: uow,
  } as QueryHandlerContext<U>
}
