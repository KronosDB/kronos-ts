import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  type Metadata,
  type CommandDescriptor,
  type EventDescriptor,
  type EventMessage,
  type QueryDescriptor,
} from "../messaging/messages.js"
import { appendFunction } from "../event-sourcing/append.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { SnapshotDemand } from "../event-sourcing/load.js"
import type { State } from "../event-sourcing/state.js"
import type { CommandBus } from "./bus.js"
import type { QueryBus } from "../query-handling/bus.js"
import { eventHandlerContext, type EventHandlerContext } from "../event-processing/context.js"
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
//
// There are three of them, one per message kind, and each lives with its kind:
// the EVENT context in `event-processing/context.ts`, the QUERY context in
// `query-handling/context.ts`, and the COMMAND context — the widest, the one that can
// `append` — here. What all three are BUILT FROM (the capability types and the
// deps record) lives here too, with the widest of them, rather than in a
// `shared/` folder that would name no concept at all.
// ---------------------------------------------------------------------------

/**
 * `append` as a context capability. Structural mirror of
 * the state layer's AppendFunction (declared here to keep the type
 * surface local to messaging, matching how {@link ContextLoadFunction} mirrors
 * the state-module shape).
 */
export type ContextAppendFunction = {
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>): void
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>, metadata: Metadata): void
  /** Batch form — `ctx.append([[A, a], [B, b]])`. Same atomic flush. */
  <T extends readonly EventDescriptor<any>[]>(events: {
    [K in keyof T]:
      | readonly [T[K], InferOutput<T[K] extends EventDescriptor<infer P> ? P : never>]
      | readonly [T[K], InferOutput<T[K] extends EventDescriptor<infer P> ? P : never>, Metadata]
  }): void
}

/**
 * `load` as a context capability — the state VALUE and its id, and nothing
 * else. Both `Id` and `S` are inferred from the state that is handed in, so a
 * handler writes `ctx.load(Course, { courseId })` and gets `CourseState` back.
 *
 * There is nowhere to register the state first. The fold is a function of the
 * value at the call site and the log on the entry's site, so passing it IS the
 * whole arrangement.
 *
 * AND THE TWO ARE CHECKED AGAINST EACH OTHER. `E` is the entry's log, threaded
 * down from the composition root; a state that declares a snapshot policy is
 * refused here unless `E` can serve one. See `SnapshotDemand` — and
 * `IfSnapshotCapable` behind it — in `event-sourcing/load.ts`.
 */
export type ContextLoadFunction<E extends EventStore = EventStore> = <Id, S>(
  state: State<Id, S, any> & SnapshotDemand<E>,
  id: Id,
) => Promise<S>

/**
 * `source` as a context capability — THE RAW LAYER under {@link
 * ContextLoadFunction}.
 *
 * Two layers of one story. `state()` DERIVES a query — one per folded event
 * type, from the tag record and the fold — and hands you the folded state.
 * `ctx.source` lets you WRITE the query and run the fold yourself, and gives
 * you the matching events in stream order.
 *
 * What does not change between the layers is the consistency guarantee: the
 * read is recorded on the task exactly as `ctx.load`'s is, so a `ctx.append` in
 * the same handling is conditioned on "nothing matching this query has landed
 * since". A hand-rolled fold is a first-class DCB decision, not an escape
 * hatch that gives one up.
 *
 * ONE SIGNATURE HERE. The FUSED form — `ctx.source(query, { snapshot })` — is
 * not part of this type: a context is ASSEMBLED by intersection, and that
 * overload is contributed by `SnapshotReads<E>` only when the entry's log can
 * serve it. Against a bare log it is structurally ABSENT, so asking for it is
 * "this call takes one argument", not "your argument is wrong".
 */
export type ContextSourceFunction = (query: EventQuery) => Promise<ReadonlyArray<EventMessage>>

/**
 * `send` as a context capability. Dispatches a command that is handled in its
 * own fresh UnitOfWork (see `send.ts` for the atomic-boundary semantics).
 */
export type ContextSendFunction = <P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: CommandDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
) => Promise<unknown>

/**
 * `query` as a context capability. Consults a query handler — local or across
 * the distributed bus — inside the handler's own UnitOfWork. The AF5 analogue
 * is injecting the query gateway into a handler. Prefer a projection or a
 * capability command; reach for this when a decision genuinely needs another
 * module's answer, fresh.
 */
export type ContextQueryFunction = <P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: QueryDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
) => Promise<R extends StandardSchemaV1 ? InferOutput<R> : unknown>

// THE SCHEDULING VERBS ARE NOT DECLARED HERE. They used to be — three context
// capability types beside these, duplicating the three in
// `event-scheduling/schedule.ts` — because a context ALWAYS had them and the
// only question was whether the field they read was populated. It is not always
// so any more: `ScheduleVerbs<E>` contributes them, and only to a context whose
// entry wired a log that can hold events that have not happened yet. They live
// with the tier that adds them, exactly as `SnapshotReads<E>` lives with the
// read it widens.

/**
 * Capabilities available to command handlers: everything an event handler has,
 * plus `append` — the command handler is the atomic decide-and-append
 * boundary, and its UnitOfWork flushes buffered events at PREPARE_COMMIT.
 */
export type HandlerContext<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = EventHandlerContext<U, E> & {
  /** Append events to this UnitOfWork, buffered until commit. */
  readonly append: ContextAppendFunction
}

/**
 * Everything a context is built from. The handling modules and processors hold
 * these already — the stores of the item being invoked, the buses they were
 * constructed with — so building a context is a closure, not a lookup.
 */
export type HandlerContextDeps<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = {
  /**
   * The task this invocation runs in. Its TYPE is what the context republishes
   * as `ctx.unitOfWork`, so a composed capability (`correlating`, an adapter's)
   * survives all the way to the handler's signature instead of being laundered
   * back to the bare handle here.
   */
  readonly uow: U
  /**
   * The log `ctx.load` sources from and `ctx.append` flushes to — the entry's
   * site, passed straight through. There is no state manager and no registry:
   * a store plus the state value handed to `ctx.load` is the whole fold.
   *
   * ITS TYPE IS PART OF THE CONTEXT'S. `E` rides from here into `ctx.load`,
   * `ctx.source` AND `ctx.schedule`, which is what lets a wrapped log offer the
   * fused read, refuse a snapshotting state on an unwrapped one, and hand out
   * the scheduling verbs only where there is somewhere to schedule INTO. There
   * is no second store field beside this one and there never will be: a
   * capability the log has is a capability this entry has.
   *
   * THE `eventScheduler` FIELD THAT USED TO SIT HERE IS GONE. A schedule fires
   * into a log, so the log is what holds it — see `event-scheduling/scheduler.ts`.
   */
  readonly eventStore?: E
  readonly commandBus?: CommandBus
  readonly queryBus?: QueryBus
}

/** Build the COMMAND handler context for one invocation. */
export function handlerContext<U extends UnitOfWork, E extends EventStore = EventStore>(
  deps: HandlerContextDeps<U, E>,
): HandlerContext<U, E> {
  return {
    ...eventHandlerContext(deps),
    append: appendFunction(deps) as ContextAppendFunction,
  } as HandlerContext<U, E>
}
