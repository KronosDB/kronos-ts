import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { EventMessage } from "../messaging/messages.js"

/**
 * Lifecycle phases of one unit of work, in the order `execute` runs them:
 * PRE_INVOCATION → INVOCATION → POST_INVOCATION → PREPARE_COMMIT → COMMIT →
 * AFTER_COMMIT. Actions within a phase run in registration order; an action
 * registered for the current phase during its own execution still runs
 * (`runPhase` drains its bucket until it is empty); one registered for a phase
 * already past is dropped.
 *
 * THE ORDER IS THE CODE IN `drivePhases`, NOT THE VALUES. There is no custom
 * phase and nothing sorts by these — the values are names, chosen so a phase
 * prints as itself in an error rather than as an ordering key inherited from
 * another framework.
 */
export const Phase = {
  /** Setup before handler invocation (e.g. transaction start). */
  PRE_INVOCATION: "pre-invocation",
  /** Actual handler execution. */
  INVOCATION: "invocation",
  /** Cleanup after handler, before commit. */
  POST_INVOCATION: "post-invocation",
  /** Prepare for commit (e.g. event store flush, token store). */
  PREPARE_COMMIT: "prepare-commit",
  /** Actual commit (e.g. database transaction commit). */
  COMMIT: "commit",
  /** Post-commit notifications (e.g. subscription query updates). */
  AFTER_COMMIT: "after-commit",
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
      `Mutating helper called during phase "${currentPhase}" — only allowed during "${Phase.INVOCATION}". ` +
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
export type SourcingInfo = {
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
export type UoWEventBuffer = {
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
export type UoWStateCache = {
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
 *
 * It holds no CORRELATION either. Correlation is not knowledge a task is born
 * with — it is a capability composed onto one: `correlating(unitOfWork())`
 * returns a unit of work that also carries a correlation map, and
 * `correlatingHandler` is what fills it and overlays it onto what a handler
 * emits. A deployment that does not want the concept never mentions it, and
 * nothing here changes shape to accommodate one that does.
 *
 * The phase lifecycle, the event buffer and the state cache hang off this one
 * object. A TRANSACTION does NOT: the base has no
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
export type UnitOfWork = {
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
   * The current instant in epoch milliseconds, from the clock this unit of work
   * was minted with.
   *
   * Every message this task gives birth to stamps its `timestamp` from HERE —
   * `ctx.append`, `ctx.send`, `ctx.query`, `ctx.schedule`, and the bus that
   * minted this unit of work for a message the edge left unstamped. That is the
   * whole point: a task has one idea of "now", so the events it appends and the
   * commands it sends cannot disagree about when they happened, and a test can
   * make that instant whatever it likes by handing `unitOfWork` a clock.
   */
  now(): number

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
// one; an adapter's `drizzleUnitOfWork(unitOfWork, db)` is another, and they are
// interchangeable because they are the same shape.
//
//   localCommandBus(unitOfWork)                        // bare
//   localCommandBus(drizzleUnitOfWork(unitOfWork, db)) // transactional


// ── implementation ──────────────────────────────────────────────────────

type Status = "not_started" | "started" | "completed" | "error"

/**
 * THE unit-of-work factory: a fresh, bare unit of work per call.
 *
 * This is the public primitive. Hand it to whatever opens units of work —
 * `localCommandBus(unitOfWork)` — or hand that seam an adapter's factory
 * instead when the units of work should carry a transaction. Nothing is
 * ambient and nothing is defaulted behind you: a seam that mints units of work
 * says which factory it mints them from.
 *
 * `clock` — `() => number` — is where the task's idea of "now" comes from:
 * `uow.now()`, and therefore the `timestamp` on every message the task gives
 * birth to. It reads an INSTANT, in epoch milliseconds — not a duration and not
 * a monotonic tick. It is the same number a message's `timestamp` carries,
 * which is what makes a clock substitutable at every message-birth site without
 * converting anything.
 *
 * THERE IS NO `Clock` TYPE, here or anywhere. The arrow IS the contract, the
 * same way a unit-of-work factory is spelled `() => UnitOfWork` and never
 * named: a one-arrow alias buys an import and hides the one thing the reader
 * needed to see. Every seam that takes a clock writes `clock?: () => number`
 * inline — this one, the schedulers, the fixture, the recorders.
 *
 * ABSENT means system time, which is the null behaviour rather than a defaulted
 * dependency: `Date.now` IS a clock, and passing it explicitly says the same
 * thing as passing nothing. A test hands in its own and every timestamp under
 * the task becomes whatever it says:
 *
 * ```ts
 * localCommandBus(unitOfWork)                      // system time
 * localCommandBus(() => unitOfWork(fixtureClock))  // the fixture's instant
 * ```
 *
 * Driving the lifecycle is `uow.execute(action)`, on the handle.
 *
 * The phase state and the buffers are CLOSED OVER, not fields: nothing outside
 * this function can reach `status` or the phase buckets, and `phase`/`closed`
 * are accessors precisely because the lifecycle — and only the lifecycle —
 * advances them.
 *
 * What comes back is PURE TASK LIFECYCLE. Correlation is composed on top —
 * `correlating(unitOfWork(clock))` — and nothing here knows the word.
 */
export function unitOfWork(clock?: () => number): UnitOfWork {
  // `clock` stays OPTIONAL rather than defaulted in the parameter list: a
  // default would drop `unitOfWork.length` to 0, and the arity is the claim
  // that a task takes a clock and nothing else.
  const tick = clock ?? Date.now

  let phase: PhaseValue | null = null
  let closed = false
  let replaying = false
  let status: Status = "not_started"

  const events: UoWEventBuffer = {
    buffered: [],
    sourcingInfos: [],
    flushRegistered: false,
  }

  const stateCache: UoWStateCache = {
    entries: new Map<string, Promise<unknown>>(),
    modules: new Map<string, { module: unknown; id: unknown }>(),
  }

  const phaseActions = new Map<PhaseValue, PhaseAction[]>()
  const errorHandlers: UoWErrorHandler[] = []
  const completeHandlers: CompleteHandler[] = []

  const on = (at: PhaseValue, action: PhaseAction): void => {
    let bucket = phaseActions.get(at)
    if (!bucket) {
      bucket = []
      phaseActions.set(at, bucket)
    }
    bucket.push(action)
  }

  // ── private: the lifecycle-driving loop ───────────────────────────────
  //
  // Actions registered during a phase's own execution for a phase that is
  // already past are silently dropped.
  // `runPhase` drains its own bucket repeatedly so actions registered for the
  // SAME phase during execution are picked up before moving on.

  const runPhase = async (at: PhaseValue): Promise<void> => {
    phase = at
    // Late registration: actions registered during a phase's own execution fire
    // next loop iteration (still within the same phase).
    let actions = phaseActions.get(at)
    while (actions && actions.length > 0) {
      const bucket = actions
      phaseActions.set(at, [])
      for (const a of bucket) await a()
      actions = phaseActions.get(at)
    }
  }

  const drivePhases = async <R>(action: () => Promise<R>): Promise<R> => {
    status = "started"
    try {
      await runPhase(Phase.PRE_INVOCATION)

      phase = Phase.INVOCATION
      let actions = phaseActions.get(Phase.INVOCATION)
      while (actions && actions.length > 0) {
        const bucket = actions
        phaseActions.set(Phase.INVOCATION, [])
        for (const a of bucket) await a()
        actions = phaseActions.get(Phase.INVOCATION)
      }
      const result = await action()

      await runPhase(Phase.POST_INVOCATION)
      await runPhase(Phase.PREPARE_COMMIT)
      await runPhase(Phase.COMMIT)
      await runPhase(Phase.AFTER_COMMIT)

      status = "completed"
      closed = true
      for (const h of completeHandlers) {
        try {
          h()
        } catch (e) {
          console.warn("UnitOfWork: completion handler threw an exception:", e)
        }
      }
      return result
    } catch (error) {
      status = "error"
      const failedPhase = phase ?? undefined
      closed = true
      for (const h of errorHandlers) {
        try {
          await h(error, failedPhase)
        } catch (e) {
          console.warn("UnitOfWork: error handler threw an exception:", e)
        }
      }
      throw error
    }
  }

  const uow: UnitOfWork = {
    get phase() {
      return phase
    },
    get closed() {
      return closed
    },
    get replaying() {
      return replaying
    },
    set replaying(value: boolean) {
      replaying = value
    },

    events,
    stateCache,

    // The guard is SYNCHRONOUS — a second execute is a wiring bug, and a
    // rejected promise would let it be swallowed by the caller's own error path.
    execute: <R>(action: (handle: UnitOfWork) => Promise<R>): Promise<R> => {
      if (status !== "not_started") {
        throw new Error("UnitOfWork.execute: this unit of work has already been executed")
      }
      return drivePhases(() => action(uow))
    },

    on,
    onPrepareCommit: (action) => { on(Phase.PREPARE_COMMIT, action) },
    onCommit: (action) => { on(Phase.COMMIT, action) },
    onAfterCommit: (action) => { on(Phase.AFTER_COMMIT, action) },
    onError: (handler) => { errorHandlers.push(handler) },
    whenComplete: (handler) => { completeHandlers.push(handler) },

    now: () => tick(),
  }

  return uow
}

// ── guards ──────────────────────────────────────────────────────────────

/**
 * Guard for mutating capabilities. Throws {@link NoActiveUnitOfWork} once the
 * unit of work has closed, {@link WrongUoWPhase} outside INVOCATION.
 *
 * Generic in the handle it is given, and returns THAT handle — a guard checks a
 * unit of work, it does not launder one. Widening the return to `UnitOfWork`
 * would silently strip whatever capability the caller had composed on
 * (`correlating`, an adapter's), which is exactly the class of bug the
 * parametric threading exists to make impossible.
 */
export function requireInvocation<U extends UnitOfWork>(uow: U): U {
  if (uow.closed) throw new NoActiveUnitOfWork()
  if (uow.phase !== Phase.INVOCATION) throw new WrongUoWPhase(uow.phase)
  return uow
}

/** Guard for read-only capabilities: live unit of work, any phase. Same non-laundering rule. */
export function requireLive<U extends UnitOfWork>(uow: U): U {
  if (uow.closed) throw new NoActiveUnitOfWork()
  return uow
}
