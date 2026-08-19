import type { EventQuery } from "../query/event-query.js"
import type { EventMessage } from "../messages/message.js"
import type { Clock } from "../primitives/clock.js"

/**
 * Lifecycle phases for message processing, ordered by execution priority.
 *
 * Phases execute in ascending order. Actions within the same phase
 * execute in registration order. Actions registered during execution
 * (e.g. `onPrepareCommit` from a handler) are picked up when their
 * phase comes.
 *
 * The numeric values are stable — external code (handler transformers,
 * processors) compares phase numbers.
 */
export const Phase = {
  /** Setup before handler invocation (e.g. transaction start). */
  PRE_INVOCATION: -10000,
  /** Actual handler execution. */
  INVOCATION: 0,
  /** Cleanup after handler, before commit. */
  POST_INVOCATION: 10000,
  /** Prepare for commit (e.g. event store flush, token store). */
  PREPARE_COMMIT: 20000,
  /** Actual commit (e.g. database transaction commit). */
  COMMIT: 30000,
  /** Post-commit notifications (e.g. subscription query updates). */
  AFTER_COMMIT: 40000,
} as const

export type PhaseValue = (typeof Phase)[keyof typeof Phase]

/** A unit of work lifecycle action. */
export type PhaseAction = () => Promise<void> | void
/** Notified when the unit of work fails, with the phase it failed in. */
export type UoWErrorHandler = (error: unknown, phase?: PhaseValue) => Promise<void> | void
/** Notified when the unit of work completes successfully. */
export type CompleteHandler = () => void

/**
 * Thrown when a capability is used without a live unit of work — a handler
 * context that outlived its `UnitOfWork` (`uow.closed === true`), or a
 * component reaching for one that was never handed down.
 *
 * Before the unit of work was handed down as a parameter this meant "no
 * ambient state"; the `closed` flag is what replaces that check.
 */
export class NoActiveUnitOfWork extends Error {
  constructor(message = "No active UnitOfWork: the unit of work is closed or was never entered") {
    super(message)
    this.name = "NoActiveUnitOfWork"
  }
}

/**
 * Thrown by mutating capabilities (`append`, `send`, `emitUpdate`, `schedule`)
 * when called outside the INVOCATION phase. A UoW IS live — it is simply in the
 * wrong phase. Catches the real bug pattern: mutations issued from lifecycle
 * hooks (`onCommit`, `onPrepareCommit`, …), which run after the decide step.
 */
export class WrongUoWPhase extends Error {
  readonly currentPhase: PhaseValue | null
  constructor(currentPhase: PhaseValue | null) {
    super(
      `Mutating helper called during phase ${currentPhase} — must be called during INVOCATION (${Phase.INVOCATION}) only. ` +
      `Do not call append/send/emitUpdate from lifecycle hooks (onPrepareCommit, onCommit, onAfterCommit, onError, whenComplete).`
    )
    this.name = "WrongUoWPhase"
    this.currentPhase = currentPhase
  }
}

/**
 * What one `load()` contributed to the append condition: the query it sourced
 * and the position it read up to.
 */
export interface SourcingInfo {
  readonly query: EventQuery
  readonly markerPosition: bigint
}

/**
 * The events a handler appended, and what it read to decide them.
 *
 * `ctx.append` only BUFFERS here; the command path's PREPARE_COMMIT flush
 * (see `event-flush.ts`) turns the buffer plus the sourcing infos into one
 * conditional event-store write — the DCB consistency check.
 */
export interface UoWEventBuffer {
  /** Events appended during INVOCATION, flushed as one atomic write. */
  readonly buffered: EventMessage[]
  /** One entry per `load()`, combined into the append condition. */
  readonly sourcingInfos: SourcingInfo[]
  /**
   * Once-per-UoW guard for the flush registration. `registerEventFlush` sets
   * it; a second registration on the same UoW is a no-op.
   */
  flushRegistered: boolean
}

/**
 * Per-UnitOfWork state cache backing `ctx.load`. A repeated `load()` of the
 * same module+id returns the cached promise instead of re-sourcing, and
 * `ctx.append` evolves the cached state so a handler that appends then loads
 * sees its own writes.
 */
export interface UoWStateCache {
  /** Cache key → in-flight or settled load result. */
  readonly entries: Map<string, Promise<unknown>>
  /** Cache key → the module and id it was loaded for, so evolvers can be applied. */
  readonly modules: Map<string, { module: unknown; id: unknown }>
}

/**
 * The unit of work, handed down as a parameter.
 *
 * It is scoped to a TASK, not to a message: a command bus opens one per
 * command, a processor opens one per BATCH. It therefore holds no message and
 * no message metadata — the message belongs to the BINDING, and the
 * per-invocation `ctx` already closes over both the message and this handle.
 * Lineage that must cross from a message onto what a handler emits arrives
 * through {@link contributeCorrelationData}, which works identically for a
 * one-message task and for each event in a batch.
 *
 * The phase lifecycle, the event buffer, the state cache and correlation
 * lineage hang off this one object. A TRANSACTION does NOT: the base has no
 * transaction concept, no `transaction()` and no `activeTransaction()`. Those
 * were typed by assertion — `uow.transaction<DrizzleTransaction>()` asserted a
 * type the handle could not know — and they put database vocabulary on a
 * primitive that has no database. Each adapter now owns both ends instead: its
 * decorator keeps the transaction in adapter-private state keyed by the unit of
 * work, hooks commit and rollback through the PUBLIC phase API here, and
 * exports a TYPED accessor pair (`drizzleTransaction(uow)` /
 * `activeDrizzleTransaction(uow)`). The type comes from the adapter that knows
 * it, and a unit of work with no adapter composed simply has no transaction to
 * ask about.
 *
 * A handler never sees the handle indirectly: the handling modules and
 * processors close over it to build the `ctx` a handler receives — which
 * exposes it as `ctx.unitOfWork` — and infrastructure (token stores,
 * dead-letter queues, event stores, schedulers) takes it as a trailing
 * parameter.
 */
export interface UnitOfWork {
  /** The phase currently executing. `null` before the lifecycle starts. */
  readonly phase: PhaseValue | null
  /**
   * True once the lifecycle has finished (successfully or not). A `ctx` held
   * past its unit of work throws {@link NoActiveUnitOfWork} rather than
   * silently writing into a committed UoW.
   */
  readonly closed: boolean

  // ── lifecycle registration ────────────────────────────────────────────
  /** Register an action for an arbitrary phase. */
  /**
   * Run `action` inside this unit of work, driving the phase protocol:
   * PRE_INVOCATION → INVOCATION → the action → POST_INVOCATION →
   * PREPARE_COMMIT → COMMIT → AFTER_COMMIT, or the error handlers on failure.
   *
   * The protocol lives HERE, on the handle, rather than in a free `run*`
   * function, because it is the unit of work's own lifecycle — nothing else
   * can drive it correctly, and a caller that forgets the rollback ordering
   * has no way to be caught. One unit of work executes exactly once.
   */
  execute<R>(action: (uow: UnitOfWork) => Promise<R>): Promise<R>

  on(phase: PhaseValue, action: PhaseAction): void
  /** Run before commit — event flush, token store writes. */
  onPrepareCommit(action: PhaseAction): void
  /** Run at commit — the adapter transaction commit. */
  onCommit(action: PhaseAction): void
  /** Run after a successful commit — subscription updates, notifications. */
  onAfterCommit(action: PhaseAction): void
  /** Run when the unit of work fails — rollback, compensations. */
  onError(handler: UoWErrorHandler): void
  /** Run when the unit of work completes successfully. */
  whenComplete(handler: CompleteHandler): void

  // ── the task's instant ────────────────────────────────────────────────
  /**
   * The current instant in epoch milliseconds, from the {@link Clock} this unit
   * of work was minted with.
   *
   * Every message this task gives birth to stamps its `timestamp` from HERE —
   * `ctx.append`, `ctx.send`, `ctx.query`, `ctx.schedule`, and the bus that
   * minted this unit of work for a message the edge left unstamped. That is the
   * whole point: a task has one idea of "now", so the events it appends and the
   * commands it sends cannot disagree about when they happened, and a test can
   * make that instant whatever it likes by handing `unitOfWork` a clock.
   */
  now(): number

  // ── correlation lineage ───────────────────────────────────────────────
  /** The lineage this unit of work stamps on everything it sends or appends. */
  correlationData(): Record<string, string>
  /** Merge extra lineage keys — an OpenTelemetry `traceparent`, say. */
  contributeCorrelationData(partial: Record<string, string>): void

  // ── per-message buffers ───────────────────────────────────────────────
  /** Appended events and the sourcing infos that condition their write. */
  readonly events: UoWEventBuffer
  /** State loaded during this unit of work. */
  readonly stateCache: UoWStateCache

  // ── replay ────────────────────────────────────────────────────────────
  /** True while a tracking/streaming processor is replaying history. */
  replaying: boolean
}

// A unit-of-work FACTORY is written `() => UnitOfWork` wherever a seam takes
// one. There is no name for it: naming a one-arrow type buys an import and
// hides the arrow, and the arrow is the whole contract. `unitOfWork` itself is
// one; an adapter's `drizzleUnitOfWork(db, unitOfWork)` is another, and they are
// interchangeable because they are the same shape.
//
//   simpleCommandBus(unitOfWork)                        // bare
//   simpleCommandBus(drizzleUnitOfWork(db, unitOfWork)) // transactional


// ── implementation ──────────────────────────────────────────────────────

type Status = "not_started" | "started" | "completed" | "error"

class ManagedUnitOfWork implements UnitOfWork {
  phase: PhaseValue | null = null
  closed = false
  replaying = false

  readonly events: UoWEventBuffer = {
    buffered: [],
    sourcingInfos: [],
    flushRegistered: false,
  }

  readonly stateCache: UoWStateCache = {
    entries: new Map<string, Promise<unknown>>(),
    modules: new Map<string, { module: unknown; id: unknown }>(),
  }

  status: Status = "not_started"

  readonly phaseActions = new Map<PhaseValue, PhaseAction[]>()
  readonly errorHandlers: UoWErrorHandler[] = []
  readonly completeHandlers: CompleteHandler[] = []

  private correlation: Record<string, string> = {}

  constructor(private readonly clock: Clock) {}

  now(): number {
    return this.clock()
  }

  /**
   * Run `action` inside this unit of work, driving the phase protocol:
   * PRE_INVOCATION → INVOCATION → the action → POST_INVOCATION →
   * PREPARE_COMMIT → COMMIT → AFTER_COMMIT, or the error handlers on failure.
   *
   * The protocol lives HERE, on the handle, rather than in a free `run*`
   * function, because it is the unit of work's own lifecycle — nothing else
   * can drive it correctly, and a caller that forgets the rollback ordering
   * has no way to be caught. One unit of work executes exactly once.
   */
  execute<R>(action: (uow: UnitOfWork) => Promise<R>): Promise<R> {
    if (this.status !== "not_started") {
      throw new Error("UnitOfWork.execute: this unit of work has already been executed")
    }
    return drivePhases(this, () => action(this))
  }

  on(phase: PhaseValue, action: PhaseAction): void {
    let bucket = this.phaseActions.get(phase)
    if (!bucket) {
      bucket = []
      this.phaseActions.set(phase, bucket)
    }
    bucket.push(action)
  }

  onPrepareCommit(action: PhaseAction): void {
    this.on(Phase.PREPARE_COMMIT, action)
  }

  onCommit(action: PhaseAction): void {
    this.on(Phase.COMMIT, action)
  }

  onAfterCommit(action: PhaseAction): void {
    this.on(Phase.AFTER_COMMIT, action)
  }

  onError(handler: UoWErrorHandler): void {
    this.errorHandlers.push(handler)
  }

  whenComplete(handler: CompleteHandler): void {
    this.completeHandlers.push(handler)
  }

  correlationData(): Record<string, string> {
    return this.correlation
  }

  contributeCorrelationData(partial: Record<string, string>): void {
    this.correlation = { ...this.correlation, ...partial }
  }
}

/**
 * THE unit-of-work factory: a fresh, bare unit of work per call.
 *
 * This is the public primitive. Hand it to whatever opens units of work —
 * `simpleCommandBus(unitOfWork)` — or hand that seam an adapter's factory
 * instead when the units of work should carry a transaction. Nothing is
 * ambient and nothing is defaulted behind you: a seam that mints units of work
 * says which factory it mints them from.
 *
 * `clock` is where the task's idea of "now" comes from — `uow.now()`, and
 * therefore the `timestamp` on every message the task gives birth to. ABSENT
 * means system time, which is the null behaviour rather than a defaulted
 * dependency: `Date.now` IS the clock, and passing it explicitly says the same
 * thing as passing nothing. A test hands in its own and every timestamp under
 * the task becomes whatever it says:
 *
 * ```ts
 * simpleCommandBus(unitOfWork)                      // system time
 * simpleCommandBus(() => unitOfWork(fixtureClock))  // the fixture's instant
 * ```
 *
 * Driving the lifecycle is `uow.execute(action)`, on the handle.
 */
export function unitOfWork(clock?: Clock): UnitOfWork {
  return new ManagedUnitOfWork(clock ?? Date.now)
}

// ── private: the lifecycle-driving loop ───────────────────────────────
//
// Re-sort semantics: actions registered during a phase's own execution at
// EARLIER phase values are silently dropped (the phase is already past).
// `runPhase` drains its own bucket repeatedly so actions registered for the
// SAME phase during execution are picked up before moving on.

async function drivePhases<R>(
  uow: ManagedUnitOfWork,
  action: () => Promise<R>,
): Promise<R> {
  uow.status = "started"
  try {
    await runPhase(uow, Phase.PRE_INVOCATION)

    uow.phase = Phase.INVOCATION
    let actions = uow.phaseActions.get(Phase.INVOCATION)
    while (actions && actions.length > 0) {
      const bucket = actions
      uow.phaseActions.set(Phase.INVOCATION, [])
      for (const a of bucket) await a()
      actions = uow.phaseActions.get(Phase.INVOCATION)
    }
    const result = await action()

    await runPhase(uow, Phase.POST_INVOCATION)
    await runPhase(uow, Phase.PREPARE_COMMIT)
    await runPhase(uow, Phase.COMMIT)
    await runPhase(uow, Phase.AFTER_COMMIT)

    uow.status = "completed"
    uow.closed = true
    for (const h of uow.completeHandlers) {
      try {
        h()
      } catch (e) {
        console.warn("UnitOfWork: completion handler threw an exception:", e)
      }
    }
    return result
  } catch (error) {
    uow.status = "error"
    const failedPhase = uow.phase ?? undefined
    uow.closed = true
    for (const h of uow.errorHandlers) {
      try {
        await h(error, failedPhase)
      } catch (e) {
        console.warn("UnitOfWork: error handler threw an exception:", e)
      }
    }
    throw error
  }
}

async function runPhase(uow: ManagedUnitOfWork, phase: PhaseValue): Promise<void> {
  uow.phase = phase
  // Late registration: actions registered during a phase's own execution fire
  // next loop iteration (still within the same phase).
  let actions = uow.phaseActions.get(phase)
  while (actions && actions.length > 0) {
    const bucket = actions
    uow.phaseActions.set(phase, [])
    for (const a of bucket) await a()
    actions = uow.phaseActions.get(phase)
  }
}

// ── guards ──────────────────────────────────────────────────────────────

/**
 * Guard for mutating capabilities. Throws {@link NoActiveUnitOfWork} once the
 * unit of work has closed, {@link WrongUoWPhase} outside INVOCATION.
 */
export function requireInvocation(uow: UnitOfWork): UnitOfWork {
  if (uow.closed) throw new NoActiveUnitOfWork()
  if (uow.phase !== Phase.INVOCATION) throw new WrongUoWPhase(uow.phase)
  return uow
}

/** Guard for read-only capabilities: live unit of work, any phase. */
export function requireLive(uow: UnitOfWork): UnitOfWork {
  if (uow.closed) throw new NoActiveUnitOfWork()
  return uow
}
