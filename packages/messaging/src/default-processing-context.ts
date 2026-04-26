import type { Metadata, ResourceKey, Configuration } from "@kronos-ts/common"
import { type ProcessingContext, type PhaseValue } from "./processing-context.js"
import {
  processingStateStorage,
  getResource,
  setResource,
  computeIfAbsent as alsComputeIfAbsent,
  removeResource,
  hasResource,
  updateResource,
} from "./processing-state.js"

type PhaseAction = (ctx: ProcessingContext) => Promise<void> | void
type ErrorHandler = (ctx: ProcessingContext, error: unknown, phase?: PhaseValue) => Promise<void> | void
type CompleteHandler = (ctx: ProcessingContext) => void

type Status = "not_started" | "started" | "completed" | "error"

/**
 * Internal ProcessingContext implementation with phase-ordered lifecycle.
 *
 * Exposed through the UnitOfWork — not created directly by user code.
 * The additional execute/setStatus methods are used by the UnitOfWork
 * to drive the lifecycle.
 */
export interface ManagedProcessingContext extends ProcessingContext {
  /** Execute all registered phase actions in order. */
  executePhases(): Promise<void>
  /** Run all registered error handlers with the phase where the error occurred. */
  runErrorHandlers(error: unknown, phase?: PhaseValue): Promise<void>
  /** Run all registered completion handlers. */
  runCompleteHandlers(): void
  /** Transition to started state. */
  markStarted(): void
  /** Transition to completed state. */
  markCompleted(): void
  /** Transition to error state. */
  markError(): void
  /** The phase that was executing when an error occurred (if any). */
  readonly failedPhase: PhaseValue | null
}

/**
 * Creates a new ProcessingContext with phase-ordered lifecycle support.
 *
 * @param metadata The message metadata
 * @param configuration Optional application Configuration for component resolution
 */
export function createProcessingContext(metadata: Metadata, configuration?: Configuration): ManagedProcessingContext {
  // Phase 2 Plan 01 (D-19/D-20): the local `resources` Map is gone. All six
  // resource methods below delegate to the ALS-backed accessors in
  // processing-state.ts so ctx and module-level reads/writes share a single
  // source of truth (the resources Map inside processingStateStorage.getStore()).
  const phaseActions = new Map<PhaseValue, PhaseAction[]>()
  const errorHandlers: ErrorHandler[] = []
  const completeHandlers: CompleteHandler[] = []

  let status: Status = "not_started"
  /** The phase currently being executed, or null if not executing. */
  let currentPhase: PhaseValue | null = null
  /** The last error passed to runErrorHandlers, for late onError registration. */
  let lastError: unknown = undefined

  function addPhaseAction(phase: PhaseValue, action: PhaseAction): void {
    // Block registration in phases that have already been entered.
    // Registering in the current or earlier phase would mean the action
    // silently never runs — fail fast instead.
    if (currentPhase !== null && phase <= currentPhase) {
      throw new Error(
        `Cannot register action in phase ${phase}: ProcessingContext is already in phase ${currentPhase}. ` +
        `Register in a later phase instead.`,
      )
    }

    let actions = phaseActions.get(phase)
    if (!actions) {
      actions = []
      phaseActions.set(phase, actions)
    }
    actions.push(action)
  }

  const ctx: ManagedProcessingContext = {
    metadata,

    get<T>(key: ResourceKey<T>): T | undefined {
      return getResource(key)
    },

    set<T>(key: ResourceKey<T>, value: T): T | undefined {
      return setResource(key, value)
    },

    computeIfAbsent<T>(key: ResourceKey<T>, supplier: () => T): T {
      return alsComputeIfAbsent(key, supplier)
    },

    remove<T>(key: ResourceKey<T>): T | undefined {
      return removeResource(key)
    },

    contains<T>(key: ResourceKey<T>): boolean {
      return hasResource(key)
    },

    update<T>(key: ResourceKey<T>, updater: (current: T | undefined) => T): T {
      return updateResource(key, updater)
    },

    withResource<T>(key: ResourceKey<T>, value: T): ProcessingContext {
      // Create a lightweight wrapper that overrides one resource
      // and delegates everything else to the parent context
      return createResourceOverridingContext(ctx, key, value)
    },

    component<T>(key: string): T | undefined {
      if (!configuration) return undefined
      if (!configuration.hasComponent(key)) return undefined
      return configuration.getComponent<T>(key)
    },

    on(phase: PhaseValue, action: PhaseAction): void {
      addPhaseAction(phase, action)
    },

    onError(handler: ErrorHandler): void {
      if (status === "error") {
        // Already errored — execute immediately
        try { handler(ctx, lastError, currentPhase ?? undefined) } catch (e) {
          console.warn("ProcessingContext: error handler threw an exception:", e)
        }
        return
      }
      errorHandlers.push(handler)
    },

    whenComplete(handler: CompleteHandler): void {
      if (status === "completed") {
        // Already completed successfully — execute immediately
        try { handler(ctx) } catch (e) {
          console.warn("ProcessingContext: completion handler threw an exception:", e)
        }
        return
      }
      completeHandlers.push(handler)
    },

    onPrepareCommit(action: PhaseAction): void {
      addPhaseAction(20000 as PhaseValue, action)
    },

    onCommit(action: PhaseAction): void {
      addPhaseAction(30000 as PhaseValue, action)
    },

    onAfterCommit(action: PhaseAction): void {
      addPhaseAction(40000 as PhaseValue, action)
    },

    get isStarted() { return status === "started" },
    get isError() { return status === "error" },
    get isCompleted() { return status === "completed" || status === "error" },

    markStarted() { status = "started" },
    markCompleted() { status = "completed" },
    markError() { status = "error" },
    get failedPhase() { return status === "error" ? currentPhase : null },

    async executePhases(): Promise<void> {
      // Execute phases in ascending order.
      // After each phase, re-check for newly registered phases
      // (handlers can register hooks during execution).
      //
      // Phase 3 / Plan 02 (CTX-03, D-30): hooks registered via the module-level
      // lifecycle accessors (`on`, `onPrepareCommit`, …) write into the ALS
      // state's `phaseActions` Map, NOT this ctx's local map. Drain both at
      // each iteration so the two registration paths are observably
      // equivalent — same firing order rules, same fail-fast semantics.
      while (true) {
        absorbAlsLifecycle()
        const sortedPhases = [...phaseActions.keys()].sort((a, b) => a - b)
        let executedAny = false

        for (const phase of sortedPhases) {
          const actions = phaseActions.get(phase)
          if (!actions || actions.length === 0) continue

          // Track the current phase so addPhaseAction can block
          // registration in already-executed or current phases.
          currentPhase = phase

          // Drain all actions for this phase (new ones added during
          // execution are picked up in the next iteration of the
          // outer while loop).
          phaseActions.delete(phase)
          for (const action of actions) {
            await action(ctx)
          }
          executedAny = true
          // After executing a phase, break to re-sort in case new
          // phases were registered with later order values.
          break
        }

        if (!executedAny) break
      }
    },

    async runErrorHandlers(error: unknown, phase?: PhaseValue): Promise<void> {
      lastError = error
      absorbAlsLifecycle()
      for (const handler of errorHandlers) {
        try {
          await handler(ctx, error, phase)
        } catch (e) {
          console.warn("ProcessingContext: error handler threw an exception:", e)
        }
      }
    },

    runCompleteHandlers(): void {
      absorbAlsLifecycle()
      for (const handler of completeHandlers) {
        try {
          handler(ctx)
        } catch (e) {
          console.warn("ProcessingContext: completion handler threw an exception:", e)
        }
      }
    },
  }

  /**
   * Phase 3 / Plan 02 (CTX-03, D-30): drain any lifecycle registrations made
   * via the module-level accessors (`on`, `onPrepareCommit`, `onError`,
   * `whenComplete`, …) into this ctx's local lifecycle structures.
   *
   * The module-level accessors are thin wrappers over `registerPhaseAction` /
   * `registerErrorHandler` / `registerCompleteHandler` — those write into the
   * ALS state's same-named fields, separate from this ctx's local maps. By
   * absorbing them at every executePhases iteration (and before runError /
   * runComplete), registrations made through either path fire identically.
   *
   * Only meaningful while inside `processingStateStorage.run` (i.e. during
   * phase execution); a no-op outside.
   */
  function absorbAlsLifecycle(): void {
    const state = processingStateStorage.getStore()
    if (state === undefined) return

    if (state.phaseActions.size > 0) {
      for (const [phase, actions] of state.phaseActions) {
        if (actions.length === 0) continue
        let bucket = phaseActions.get(phase)
        if (!bucket) {
          bucket = []
          phaseActions.set(phase, bucket)
        }
        for (const a of actions) {
          // Adapt phase-state-shape `() => Promise<void> | void` to
          // ctx-shape `(ctx) => Promise<void> | void` (ignored arg).
          bucket.push(() => a())
        }
        actions.length = 0
      }
    }

    if (state.errorHandlers.length > 0) {
      for (const h of state.errorHandlers) {
        errorHandlers.push((_ctx, err, phase) => h(err, phase))
      }
      state.errorHandlers.length = 0
    }

    if (state.completeHandlers.length > 0) {
      for (const h of state.completeHandlers) {
        completeHandlers.push(() => h())
      }
      state.completeHandlers.length = 0
    }
  }

  return ctx
}

/**
 * A lightweight ProcessingContext that overrides a single resource
 * and delegates everything else to the parent. Used by `withResource()`.
 *
 * This is the TypeScript equivalent of AF5's ResourceOverridingProcessingContext.
 */
function createResourceOverridingContext<T>(
  delegate: ProcessingContext,
  overrideKey: ResourceKey<T>,
  overrideValue: T,
): ProcessingContext {
  return {
    get<U>(key: ResourceKey<U>): U | undefined {
      if (key.symbol === overrideKey.symbol) return overrideValue as unknown as U
      return delegate.get(key)
    },
    set<U>(key: ResourceKey<U>, value: U): U | undefined {
      if (key.symbol === overrideKey.symbol) {
        const prev = overrideValue
        ;(overrideValue as unknown) = value
        return prev as unknown as U
      }
      return delegate.set(key, value)
    },
    computeIfAbsent<U>(key: ResourceKey<U>, supplier: () => U): U {
      if (key.symbol === overrideKey.symbol) return overrideValue as unknown as U
      return delegate.computeIfAbsent(key, supplier)
    },
    remove<U>(key: ResourceKey<U>): U | undefined {
      return delegate.remove(key)
    },
    contains<U>(key: ResourceKey<U>): boolean {
      if (key.symbol === overrideKey.symbol) return true
      return delegate.contains(key)
    },
    update<U>(key: ResourceKey<U>, updater: (current: U | undefined) => U): U {
      return delegate.update(key, updater)
    },
    withResource<U>(key: ResourceKey<U>, value: U): ProcessingContext {
      return createResourceOverridingContext(this, key, value)
    },
    component<U>(key: string): U | undefined {
      return delegate.component(key)
    },
    on(phase, action) { delegate.on(phase, action) },
    onError(handler) { delegate.onError(handler) },
    whenComplete(handler) { delegate.whenComplete(handler) },
    onPrepareCommit(action) { delegate.onPrepareCommit(action) },
    onCommit(action) { delegate.onCommit(action) },
    onAfterCommit(action) { delegate.onAfterCommit(action) },
    get isStarted() { return delegate.isStarted },
    get isError() { return delegate.isError },
    get isCompleted() { return delegate.isCompleted },
    get metadata() { return delegate.metadata },
  }
}
