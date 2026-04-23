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
 * Creates a default UnitOfWorkFactory.
 */
export function defaultUnitOfWorkFactory(): UnitOfWorkFactory {
  return (metadata?: Metadata) => createUnitOfWork(metadata)
}
