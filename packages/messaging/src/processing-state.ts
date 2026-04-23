import { AsyncLocalStorage } from "node:async_hooks"
import type { Metadata, ResourceKey } from "@kronos-ts/common"
import type { PhaseValue } from "./processing-context.js"

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

function requireState(): InternalProcessingState {
  const state = processingStateStorage.getStore()
  if (state === undefined) throw new NoActiveUnitOfWork()
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
