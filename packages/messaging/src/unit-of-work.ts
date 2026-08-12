import { emptyMetadata, type Metadata } from "@kronos-ts/common"
import {
  processingStateStorage,
  initialProcessingState,
  Phase,
  type PhaseValue,
} from "./processing-state.js"

/**
 * Runner type: the canonical "enter a UnitOfWork" shape.
 *
 * Plan 03-04 (CTX-04 / D-34): replaces the old `UnitOfWorkFactory` shape.
 * Extensions (kronosdb, axon-server) and transactional wrappers compose
 * runners — `transactionalUnitOfWorkFactory` accepts a delegate runner
 * and returns a new runner that begins/commits/rolls-back a transaction
 * around the inner action.
 */
export type UoWRunner = <R>(
  metadata: Metadata | undefined,
  action: () => Promise<R>,
) => Promise<R>

/**
 * Run `action` inside a NEW UnitOfWork. Always creates a fresh UoW even
 * if one is already active on the ALS stack.
 *
 * Used by gateways (D-32): `commandGateway.send`, `queryGateway.query`,
 * and any external entry point. This keeps gateway calls isolated from
 * an injected dispatcher that reuses the active UoW.
 */
export function runInNewUoW<R>(
  metadata: Metadata | undefined,
  action: () => Promise<R>,
): Promise<R> {
  const resolvedMetadata = metadata ?? emptyMetadata()
  const state = initialProcessingState(resolvedMetadata)
  return processingStateStorage.run(state, () => drivePhases(state, action))
}

/**
 * Run `action` inside a UnitOfWork. ALS-aware:
 *
 * - If `processingStateStorage` already has an active state (we are
 *   nested inside a parent UoW), this REUSES the parent's state and
 *   calls `action` directly — no new `processingStateStorage.run`, no
 *   new phase lifecycle. This is CTX-02: nested dispatch threads through
 *   the same UoW so transactionality spans handler-internal bus calls.
 *
 * - If no state is active, behaves identically to `runInNewUoW` —
 *   creates a fresh UoW and drives the full phase lifecycle.
 *
 * Used by `queryBus.query` so handler-internal queries auto-nest, while
 * a primary query (no active UoW) starts a new one.
 *
 * NOTE: `commandBus.dispatch` deliberately does NOT use this — for AF5
 * parity every command gets its own fresh UnitOfWork via `runInNewUoW`,
 * nested or not. See `simpleCommandBus`.
 */
export function runInUoW<R>(
  metadata: Metadata | undefined,
  action: () => Promise<R>,
): Promise<R> {
  if (processingStateStorage.getStore() !== undefined) return action()
  return runInNewUoW(metadata, action)
}

// ── private: the lifecycle-driving loop ───────────────────────────────
//
// Plan 03-04 (D-34): the phase-driving loop previously lived inside
// `createUnitOfWork` + `ManagedProcessingContext.executePhases`. Inlined
// here because the ProcessingContext abstraction is gone — the runner is
// now the only entry point.
//
// Re-sort semantics from the old `executePhases` (default-processing-context.ts
// lines 168-199): handlers registered during a phase's own execution at
// EARLIER phase values are silently dropped (the phase is already past).
// `runPhase` drains its own bucket repeatedly so handlers registered for
// the SAME phase during execution are picked up before moving on. After
// a phase finishes, the outer loop re-reads `phaseActions.keys()` so any
// new LATER phase entries are picked up in order.

type State = ReturnType<typeof initialProcessingState>

async function drivePhases<R>(state: State, action: () => Promise<R>): Promise<R> {
  state.status = "started"
  try {
    await runPhase(state, Phase.PRE_INVOCATION)

    state.currentPhase = Phase.INVOCATION
    let actions = state.phaseActions.get(Phase.INVOCATION)
    while (actions && actions.length > 0) {
      const bucket = actions
      state.phaseActions.set(Phase.INVOCATION, [])
      for (const a of bucket) await a()
      actions = state.phaseActions.get(Phase.INVOCATION)
    }
    const result = await action()

    await runPhase(state, Phase.POST_INVOCATION)
    await runPhase(state, Phase.PREPARE_COMMIT)
    await runPhase(state, Phase.COMMIT)
    await runPhase(state, Phase.AFTER_COMMIT)

    state.status = "completed"
    for (const h of state.completeHandlers) {
      try {
        h()
      } catch (e) {
        console.warn("UnitOfWork: completion handler threw an exception:", e)
      }
    }
    return result
  } catch (error) {
    state.status = "error"
    const failedPhase = state.currentPhase ?? undefined
    for (const h of state.errorHandlers) {
      try {
        await h(error, failedPhase)
      } catch (e) {
        console.warn("UnitOfWork: error handler threw an exception:", e)
      }
    }
    throw error
  }
}

async function runPhase(state: State, phase: PhaseValue): Promise<void> {
  state.currentPhase = phase
  // Late registration: actions registered during a phase's own execution
  // fire next loop iteration (still within the same phase).
  let actions = state.phaseActions.get(phase)
  while (actions && actions.length > 0) {
    const bucket = actions
    state.phaseActions.set(phase, [])
    for (const a of bucket) await a()
    actions = state.phaseActions.get(phase)
  }
}
