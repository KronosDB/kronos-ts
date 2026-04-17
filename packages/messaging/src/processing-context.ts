import type { Metadata, ResourceKey } from "@kronos-ts/common"

/**
 * Lifecycle phases for message processing, ordered by execution priority.
 *
 * Phases execute in ascending order. Actions within the same phase
 * execute in registration order. Actions registered during execution
 * (e.g., onPrepareCommit from a handler) are picked up when their
 * phase comes.
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

/**
 * Context for a single unit of message processing.
 *
 * Provides type-safe resource storage, ordered lifecycle hooks,
 * and state queries. Created by a UnitOfWork for each dispatch.
 *
 * Handlers receive this and can register additional lifecycle hooks
 * (e.g., onPrepareCommit for event flushing) and store scoped
 * resources (e.g., entity cache, buffered events).
 */
export interface ProcessingContext {
  /** Retrieve a resource by its typed key. */
  get<T>(key: ResourceKey<T>): T | undefined

  /** Store a resource under its typed key. Returns the previous value if any. */
  set<T>(key: ResourceKey<T>, value: T): T | undefined

  /** Get or lazily create a resource. */
  computeIfAbsent<T>(key: ResourceKey<T>, supplier: () => T): T

  /** Remove a resource and return its previous value. */
  remove<T>(key: ResourceKey<T>): T | undefined

  /** Check if a resource exists. */
  contains<T>(key: ResourceKey<T>): boolean

  /** Update a resource atomically. Returns the new value. */
  update<T>(key: ResourceKey<T>, updater: (current: T | undefined) => T): T

  /**
   * Create a branched ProcessingContext that overrides a single resource.
   * All other resources and lifecycle hooks are delegated to this context.
   * Used for nested dispatch or service-specific resource isolation.
   */
  withResource<T>(key: ResourceKey<T>, value: T): ProcessingContext

  /**
   * Resolve a component from the application Configuration.
   * Returns undefined if the context is not associated with a Configuration
   * or the component doesn't exist.
   */
  component<T>(key: string): T | undefined

  /** Register an action to run in the given lifecycle phase. */
  on(phase: PhaseValue, action: (ctx: ProcessingContext) => Promise<void> | void): void

  /**
   * Register an error handler. Called with the error and the phase
   * it occurred in when any phase action fails.
   */
  onError(handler: (ctx: ProcessingContext, error: unknown, phase?: PhaseValue) => Promise<void> | void): void

  /** Register a handler that runs on successful completion only. */
  whenComplete(handler: (ctx: ProcessingContext) => void): void

  /** Shorthand: register action in PREPARE_COMMIT phase. */
  onPrepareCommit(action: (ctx: ProcessingContext) => Promise<void> | void): void

  /** Shorthand: register action in COMMIT phase. */
  onCommit(action: (ctx: ProcessingContext) => Promise<void> | void): void

  /** Shorthand: register action in AFTER_COMMIT phase. */
  onAfterCommit(action: (ctx: ProcessingContext) => Promise<void> | void): void

  /** Whether this context has started executing phases. */
  readonly isStarted: boolean

  /** Whether this context ended with an error. */
  readonly isError: boolean

  /** Whether this context has completed (success or error). */
  readonly isCompleted: boolean

  /** The metadata of the message being processed. */
  readonly metadata: Metadata
}
