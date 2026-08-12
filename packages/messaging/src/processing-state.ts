import { AsyncLocalStorage } from "node:async_hooks"
import type { Metadata, ResourceKey } from "@kronos-ts/common"

/**
 * Lifecycle phases for message processing, ordered by execution priority.
 *
 * Phases execute in ascending order. Actions within the same phase
 * execute in registration order. Actions registered during execution
 * (e.g., onPrepareCommit from a handler) are picked up when their
 * phase comes.
 *
 * Plan 03-04 (CTX-04 / D-34): Phase enum relocated here from the
 * deleted processing-context.ts. The numeric values are stable —
 * external code (handler enhancers, processors) compares phase numbers.
 */
export const Phase = {
  /** Setup before handler invocation (e.g., transaction start). */
  PRE_INVOCATION: -10000,
  /** Actual handler execution. */
  INVOCATION: 0,
  /** Cleanup after handler, before commit. */
  POST_INVOCATION: 10000,
  /** Prepare for commit (e.g., event store flush, token store). */
  PREPARE_COMMIT: 20000,
  /** Actual commit (e.g., database transaction commit). */
  COMMIT: 30000,
  /** Post-commit notifications (e.g., subscription query updates). */
  AFTER_COMMIT: 40000,
} as const

export type PhaseValue = (typeof Phase)[keyof typeof Phase]

// D-13: NOT exported.
type PhaseAction = () => Promise<void> | void
type ErrorHandler = (error: unknown, phase?: PhaseValue) => Promise<void> | void
type CompleteHandler = () => void

type Status = "not_started" | "started" | "completed" | "error"

type InternalProcessingState = {
  resources: Map<symbol, unknown>
  phaseActions: Map<PhaseValue, PhaseAction[]>
  errorHandlers: ErrorHandler[]
  completeHandlers: CompleteHandler[]
  currentPhase: PhaseValue | null
  status: Status
  metadata: Metadata
}

// D-04: Framework-wide ALS instance. Exported (deep-path only per D-05).
export const processingStateStorage = new AsyncLocalStorage<InternalProcessingState>()

// D-10: stable error name.
export class NoActiveUnitOfWork extends Error {
  constructor(message = "No active UnitOfWork: accessor called outside processingStateStorage.run()") {
    super(message)
    this.name = "NoActiveUnitOfWork"
  }
}

/**
 * Plan 04-01 (HDL-02 / D-43): thrown by mutating helpers (append, send, emitUpdate)
 * when called outside the INVOCATION phase. Distinct class (not a NoActiveUnitOfWork
 * subclass) — a UoW IS active, but it's in the wrong phase. Catches the real bug
 * pattern of mutations from lifecycle hooks (onCommit, onPrepareCommit, etc.).
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

function requireState(): InternalProcessingState {
  const state = processingStateStorage.getStore()
  if (state === undefined) throw new NoActiveUnitOfWork()
  return state
}

/**
 * Plan 04-01 (HDL-02 / D-43): mutator guard. Throws NoActiveUnitOfWork outside a UoW;
 * throws WrongUoWPhase when active phase != INVOCATION; otherwise returns the state.
 * Used by append (eventsourcing), send + emitUpdate (messaging).
 * Internal — NOT exported from index.ts barrel. Consumed via deep-path import.
 */
export function requireInvocationPhase(): InternalProcessingState {
  const state = processingStateStorage.getStore()
  if (state === undefined) throw new NoActiveUnitOfWork()
  if (state.currentPhase !== Phase.INVOCATION) throw new WrongUoWPhase(state.currentPhase)
  return state
}

// ── Resource accessors ──────────────────────────────────────────────────

export function getResource<T>(key: ResourceKey<T>): T | undefined {
  const state = requireState()
  return state.resources.get(key.symbol) as T | undefined
}

export function setResource<T>(key: ResourceKey<T>, value: T): T | undefined {
  const state = requireState()
  const previous = state.resources.get(key.symbol) as T | undefined
  state.resources.set(key.symbol, value)
  return previous
}

export function computeIfAbsent<T>(key: ResourceKey<T>, supplier: () => T): T {
  const state = requireState()
  const existing = state.resources.get(key.symbol)
  if (existing !== undefined) return existing as T
  const value = supplier()
  state.resources.set(key.symbol, value)
  return value
}

export function removeResource<T>(key: ResourceKey<T>): T | undefined {
  const state = requireState()
  const previous = state.resources.get(key.symbol) as T | undefined
  state.resources.delete(key.symbol)
  return previous
}

export function hasResource<T>(key: ResourceKey<T>): boolean {
  const state = requireState()
  return state.resources.has(key.symbol)
}

export function updateResource<T>(
  key: ResourceKey<T>,
  updater: (current: T | undefined) => T,
): T {
  const state = requireState()
  const current = state.resources.get(key.symbol) as T | undefined
  const updated = updater(current)
  state.resources.set(key.symbol, updated)
  return updated
}

// ── Lifecycle registration ──────────────────────────────────────────────

export function registerPhaseAction(phase: PhaseValue, action: PhaseAction): void {
  const state = requireState()
  let bucket = state.phaseActions.get(phase)
  if (!bucket) {
    bucket = []
    state.phaseActions.set(phase, bucket)
  }
  bucket.push(action)
}

export function registerErrorHandler(handler: ErrorHandler): void {
  const state = requireState()
  state.errorHandlers.push(handler)
}

export function registerCompleteHandler(handler: CompleteHandler): void {
  const state = requireState()
  state.completeHandlers.push(handler)
}

// ── Lifecycle accessors (CTX-03, D-30) ──────────────────────────────────
//
// Module-level equivalents of ctx.on / ctx.onError / ctx.whenComplete /
// ctx.onPrepareCommit / ctx.onCommit / ctx.onAfterCommit. Thin wrappers
// over the Phase 1 accessors (registerPhaseAction / registerErrorHandler /
// registerCompleteHandler). Same fail-fast contract (D-31): throw
// NoActiveUnitOfWork outside an active processingStateStorage.run.

export function on(phase: PhaseValue, action: PhaseAction): void {
  registerPhaseAction(phase, action)
}

export function onPrepareCommit(action: PhaseAction): void {
  registerPhaseAction(Phase.PREPARE_COMMIT, action)
}

export function onCommit(action: PhaseAction): void {
  registerPhaseAction(Phase.COMMIT, action)
}

export function onAfterCommit(action: PhaseAction): void {
  registerPhaseAction(Phase.AFTER_COMMIT, action)
}

export function onError(handler: ErrorHandler): void {
  registerErrorHandler(handler)
}

export function whenComplete(handler: CompleteHandler): void {
  registerCompleteHandler(handler)
}

// ── withOverride (D-07..D-09) ──────────────────────────────────────────

/**
 * Run `fn` in a nested processingStateStorage context where `key` resolves to `value`.
 *
 * Only the `resources` Map is forked (D-08). All other fields — phaseActions,
 * errorHandlers, completeHandlers, status, currentPhase, metadata — are SHARED
 * references to the parent state. Registering a phase action or error handler
 * inside `fn` targets the parent's lifecycle.
 *
 * After `fn` resolves, the parent's resources are unchanged (D-09) — nested
 * `storage.run()` naturally pops the forked state.
 *
 * Throws NoActiveUnitOfWork if called outside an existing processingStateStorage.run().
 */
export function withOverride<T, R>(
  key: ResourceKey<T>,
  value: T,
  fn: () => Promise<R>,
): Promise<R> {
  const parent = requireState()
  const forkedResources = new Map(parent.resources)
  forkedResources.set(key.symbol, value)

  const forkedState: InternalProcessingState = {
    resources: forkedResources,
    // SHARED references to parent (D-08):
    phaseActions: parent.phaseActions,
    errorHandlers: parent.errorHandlers,
    completeHandlers: parent.completeHandlers,
    status: parent.status,
    currentPhase: parent.currentPhase,
    metadata: parent.metadata,
  }

  return processingStateStorage.run(forkedState, fn)
}

/**
 * INTERNAL — used by UnitOfWork to construct the initial state passed to
 * processingStateStorage.run(). The InternalProcessingState type is intentionally
 * non-exported (D-13); this factory is the sanctioned construction point.
 * Deep-path import only; NOT re-exported from the package barrel (D-05).
 *
 * Return type is intentionally INFERRED (no `: InternalProcessingState` annotation).
 * Annotating the return type with a non-exported name produces TS4023 under
 * `declaration: true` / `isolatedDeclarations`, breaking the package build.
 * Inference preserves D-13 (the type remains non-exported) while allowing `.d.ts`
 * emission.
 */
export function initialProcessingState(metadata: Metadata) {
  return {
    resources: new Map<symbol, unknown>(),
    phaseActions: new Map<PhaseValue, PhaseAction[]>(),
    errorHandlers: [] as ErrorHandler[],
    completeHandlers: [] as CompleteHandler[],
    currentPhase: null as PhaseValue | null,
    status: "not_started" as Status,
    metadata,
  }
}
