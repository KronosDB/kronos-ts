import { emptyMetadata, type Metadata } from "@kronos-ts/common"
import type { ProcessingContext, PhaseValue } from "./processing-context.js"
import { Phase } from "./processing-context.js"
import { createProcessingContext } from "./default-processing-context.js"
import { processingStateStorage, createInitialProcessingState } from "./processing-state.js"

type PhaseAction = (ctx: ProcessingContext) => Promise<void> | void
type ErrorHandler = (ctx: ProcessingContext, error: unknown) => Promise<void> | void
type CompleteHandler = (ctx: ProcessingContext) => void

/**
 * A UnitOfWork orchestrates a single ProcessingContext through its
 * ordered lifecycle phases.
 *
 * Usage:
 * 1. Create via `createUnitOfWork()`
 * 2. Optionally register pre-execution hooks (on, onError, whenComplete)
 * 3. Call `executeWithResult(action)` — this drives the full lifecycle
 *
 * The lifecycle:
 * - PRE_INVOCATION: setup (e.g., transaction start)
 * - INVOCATION: the action runs here, handler receives ProcessingContext
 * - POST_INVOCATION: post-handler hooks
 * - PREPARE_COMMIT: flush events, store tokens
 * - COMMIT: database transaction commit
 * - AFTER_COMMIT: subscription query updates, notifications
 * - On error: error handlers run, remaining phases are skipped
 * - whenComplete: runs on successful completion only
 * - doFinally: always runs (success or failure)
 */
export interface UnitOfWork {
  /** Register an action to run in the given lifecycle phase (before execution). */
  on(phase: PhaseValue, action: PhaseAction): void

  /** Register an error handler (before execution). */
  onError(handler: ErrorHandler): void

  /** Register a handler that runs on successful completion only. */
  whenComplete(handler: CompleteHandler): void

  /**
   * Execute the action within this UnitOfWork's lifecycle.
   * The action is registered in the INVOCATION phase and receives
   * the ProcessingContext. All phases are then driven to completion.
   */
  executeWithResult<R>(action: (ctx: ProcessingContext) => Promise<R>): Promise<R>
}

/** Factory function for creating UnitOfWork instances. */
export type UnitOfWorkFactory = (metadata?: Metadata) => UnitOfWork

/**
 * Creates a new UnitOfWork.
 *
 * Pre-registration hooks (on, onError, whenComplete) are buffered
 * until executeWithResult is called, then applied to the internal
 * ProcessingContext before phase execution begins.
 */
/**
 * Module-private registry mapping an ALS-state object to the ProcessingContext
 * created by `createUnitOfWork` for that state. Populated by `createUnitOfWork`
 * just before entering `processingStateStorage.run`; consulted by `runInUoW`
 * when it detects an active ALS state and needs to recover the live ctx
 * without constructing a new one.
 *
 * The WeakMap indirection is transitional. Plan 04 (D-34) collapses
 * `UnitOfWork` into the runner itself — at that point the registry disappears
 * because the state and the ctx become the same thing.
 *
 * Why a WeakMap and not a field on `InternalProcessingState`?
 * `InternalProcessingState` is intentionally non-exported (D-13) and has no
 * slot for a public ProcessingContext (D-19/D-20 made ctx a thin shim over
 * the state's resources Map — circular reference would result if the ctx
 * were stored on the state). The WeakMap keys on the state object identity
 * and lets GC reclaim entries naturally when the run exits.
 */
const activeContextRegistry = new WeakMap<object, ProcessingContext>()

export function createUnitOfWork(metadata?: Metadata): UnitOfWork {
  const prePhaseActions: Array<{ phase: PhaseValue; action: PhaseAction }> = []
  const preErrorHandlers: ErrorHandler[] = []
  const preCompleteHandlers: CompleteHandler[] = []
  let executed = false

  return {
    on(phase: PhaseValue, action: PhaseAction): void {
      if (executed) throw new Error("Cannot register hooks after execution has started")
      prePhaseActions.push({ phase, action })
    },

    onError(handler: ErrorHandler): void {
      if (executed) throw new Error("Cannot register hooks after execution has started")
      preErrorHandlers.push(handler)
    },

    whenComplete(handler: CompleteHandler): void {
      if (executed) throw new Error("Cannot register hooks after execution has started")
      preCompleteHandlers.push(handler)
    },

    async executeWithResult<R>(action: (ctx: ProcessingContext) => Promise<R>): Promise<R> {
      if (executed) throw new Error("UnitOfWork can only be executed once")
      executed = true

      const resolvedMetadata = metadata ?? emptyMetadata()
      const ctx = createProcessingContext(resolvedMetadata)

      // Apply pre-registered hooks
      for (const { phase, action: a } of prePhaseActions) {
        ctx.on(phase, a)
      }
      for (const handler of preErrorHandlers) {
        ctx.onError(handler)
      }
      for (const handler of preCompleteHandlers) {
        ctx.whenComplete(handler)
      }

      // Register the user's action in INVOCATION phase
      let result!: R
      ctx.on(Phase.INVOCATION, async (c) => {
        result = await action(c)
      })

      // D-01: enter processingStateStorage.run on UoW entry. The ALS state
      // is populated with the SAME metadata as the ProcessingContext. Dual-write
      // (D-02): ctx remains source of truth; ALS state exists in parallel but
      // no framework code reads from it yet — Phase 2 flips readers file-by-file.
      const alsState = createInitialProcessingState(resolvedMetadata)

      // Phase 3 / Plan 01 (D-32, D-33): record the ctx⇄state mapping so that
      // a nested runInUoW can recover the active ProcessingContext without
      // constructing a new one. Plan 04 (D-34) inlines this when UoW collapses.
      activeContextRegistry.set(alsState, ctx)

      return processingStateStorage.run(alsState, async () => {
        ctx.markStarted()
        try {
          await ctx.executePhases()
          ctx.markCompleted()
          ctx.runCompleteHandlers()
          return result
        } catch (error) {
          ctx.markError()
          await ctx.runErrorHandlers(error, ctx.failedPhase ?? undefined)
          throw error
        }
      })
    },
  }
}

/**
 * Unconditionally start a new UnitOfWork and run `action` inside it.
 *
 * Used by gateways (D-32): user-initiated dispatch always creates a fresh UoW,
 * even when called from inside another UoW. Mirrors Axon Framework 5's
 * `CommandGateway` semantics — always-new — vs an injected `CommandDispatcher`
 * which reuses the active UoW.
 *
 * The action receives the freshly-created ProcessingContext. The returned
 * Promise resolves with the action's result, or rejects with its error,
 * after all UoW phases have run.
 *
 * Implementation note: defers to `createUnitOfWork(metadata).executeWithResult`
 * for now to keep the diff minimal. Plan 04 (D-34) inlines the phase-driving
 * loop and deletes the `UnitOfWork` interface — at that point this function
 * becomes the entry point itself.
 */
export function runInNewUoW<R>(
  metadata: Metadata | undefined,
  action: (ctx: ProcessingContext) => Promise<R>,
): Promise<R> {
  const uow = createUnitOfWork(metadata)
  return uow.executeWithResult(action)
}

/**
 * Enter a UnitOfWork and run `action` inside it. ALS-aware:
 *
 * - If `processingStateStorage` already has an active state (we are nested
 *   inside a parent UoW), this REUSES the parent's ProcessingContext and
 *   calls `action` directly — no new `processingStateStorage.run`, no new
 *   phase lifecycle. This is CTX-02: nested dispatch threads through the
 *   same UoW so transactionality spans handler-internal bus calls.
 *
 * - If no state is active, behaves identically to `runInNewUoW` — creates
 *   a fresh UoW and drives the full phase lifecycle.
 *
 * Used by buses (D-32): `commandBus.dispatch` and `queryBus.query` route
 * through this so that handler-internal dispatches auto-nest, while
 * primary dispatch (no active UoW) starts a new one.
 *
 * If an active ALS state exists but `createUnitOfWork` did not register the
 * ctx in `activeContextRegistry`, this throws — that combination indicates a
 * UoW entered ALS through a non-sanctioned path (e.g. raw
 * `processingStateStorage.run` outside the UoW factory). In production code
 * such a path does not exist; tests that need an ALS-only run should call
 * `processingStateStorage.run` directly without going through `runInUoW`.
 */
export function runInUoW<R>(
  metadata: Metadata | undefined,
  action: (ctx: ProcessingContext) => Promise<R>,
): Promise<R> {
  const state = processingStateStorage.getStore()
  if (state !== undefined) {
    const ctx = activeContextRegistry.get(state)
    if (ctx === undefined) {
      throw new Error(
        "runInUoW: processingStateStorage has an active state but no registered ProcessingContext. " +
          "This indicates a UoW entered ALS via a non-createUnitOfWork path.",
      )
    }
    return action(ctx)
  }
  return runInNewUoW(metadata, action)
}

/**
 * Creates a default UnitOfWorkFactory.
 */
export function defaultUnitOfWorkFactory(): UnitOfWorkFactory {
  return (metadata?: Metadata) => createUnitOfWork(metadata)
}
