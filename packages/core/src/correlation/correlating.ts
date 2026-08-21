import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * A unit of work that also carries a correlation map.
 *
 * The map lives in THIS function's closure — not on the base unit of work,
 * which is pure task lifecycle and has never heard of correlation. That is the
 * whole shape of the feature: correlation is a capability you COMPOSE onto a
 * task, and a deployment that does not compose it has no way to observe that
 * the concept exists.
 *
 * ```ts
 * const uow = () => correlating(unitOfWork(clock))
 * const commandBus = localCommandBus(uow)      // CommandBus<CorrelatingUnitOfWork>
 * ```
 *
 * The record DELEGATES member by member instead of spreading `uow`. Spreading
 * would read `phase` and `closed` ONCE — they are getters over the lifecycle's
 * own state — and freeze the copies at whatever they were when the wrapper was
 * built, so the guards would see a unit of work that is forever un-started and
 * never closed. `replaying` needs both halves for the same reason: the
 * processor writes it, the handler reads it, and a one-way copy loses one of
 * them.
 *
 * `execute` hands the CORRELATED handle to the action rather than the one it
 * wrapped. Downstream registries are WeakMaps keyed by unit of work — an
 * adapter's transaction table is the one that matters — so a task must present
 * ONE identity for its whole lifetime. Handing the inner handle inward would
 * split it in two: the factory claims the correlated object, and the action
 * would then ask about an object nobody claimed.
 */
export function correlating(uow: UnitOfWork) {
  let correlation: Record<string, string> = {}

  const correlated = {
    get phase() {
      return uow.phase
    },
    get closed() {
      return uow.closed
    },
    get replaying() {
      return uow.replaying
    },
    set replaying(value: boolean) {
      uow.replaying = value
    },

    events: uow.events,
    stateCache: uow.stateCache,

    execute: <R>(action: (handle: UnitOfWork) => Promise<R>): Promise<R> =>
      uow.execute(() => action(correlated)),

    on: uow.on.bind(uow),
    onPrepareCommit: uow.onPrepareCommit.bind(uow),
    onCommit: uow.onCommit.bind(uow),
    onAfterCommit: uow.onAfterCommit.bind(uow),
    onError: uow.onError.bind(uow),
    whenComplete: uow.whenComplete.bind(uow),

    now: () => uow.now(),

    /**
     * What this task stamps onto everything it gives birth to — the map
     * `correlatingHandler` overlays through the birth verbs' trailing
     * `metadata` parameter.
     */
    correlationData: (): Record<string, string> => correlation,

    /**
     * Merge extra correlation keys onto this task.
     *
     * `correlatingHandler` calls it once per invocation with the handled
     * message's pair; a handler that wants more — an OpenTelemetry
     * `traceparent`, a tenant — calls it mid-handling through
     * `ctx.unitOfWork`, and the next verb picks the addition up because the
     * overlay is read per call rather than captured at wrap time.
     */
    attachCorrelationData: (partial: Record<string, string>): void => {
      correlation = { ...correlation, ...partial }
    },
  }

  return correlated
}

/**
 * The type of what {@link correlating} returns — DERIVED, never hand-written.
 *
 * The function is the source of truth: a member that stopped delegating would
 * change this type, and the probe that asserts it is still assignable to
 * `UnitOfWork` is what turns that into a failed build.
 *
 * It is a real name so a handler can demand the capability without an anonymous
 * intersection in its signature — `ctx.unitOfWork: CorrelatingUnitOfWork` reads
 * as a requirement, `ctx.unitOfWork: UnitOfWork & { … }` reads as an accident.
 */
export type CorrelatingUnitOfWork = ReturnType<typeof correlating>
