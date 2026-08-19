import type { EventQuery } from "../query/event-query.js"
import type { StateModule } from "./state.js"

/**
 * Metadata about what was sourced when loading state.
 * Used by the framework to build append conditions.
 */
export interface SourcingInfo {
  readonly query: EventQuery
  readonly markerPosition: bigint
}

/**
 * Result of loading state — the state plus sourcing metadata.
 */
export interface LoadResult<S = unknown> {
  readonly state: S
  readonly sourcingInfo: SourcingInfo
}

/**
 * A repository that knows how to load state of a specific state module
 * by sourcing events from the event store and folding them through evolvers.
 */
export interface StateRepository<Id = unknown, S = unknown> {
  /** The state's durable name, when it has one. Diagnostics only. */
  readonly stateName?: string
  load(id: Id): Promise<LoadResult<S>>
  /**
   * Load state, creating the initial state if no events exist.
   * Unlike `load()`, this never fails for a new state — it returns
   * the `create()` state with empty sourcing info.
   */
  loadOrCreate(id: Id): Promise<LoadResult<S>>
}

/**
 * Manages state repositories and provides the `load()` capability
 * to command and event handlers.
 */
export interface StateManager {
  register<Id, S>(
    state: StateModule<Id, S>,
    repository: StateRepository<Id, S>,
  ): void

  load<Id, S>(
    state: StateModule<Id, S>,
    id: Id,
  ): Promise<LoadResult<S>>
}

export function stateManager(): StateManager {
  // Keyed on the definition's `identity`, not its `name`: `name` is optional
  // (it is durable snapshot identity, nothing more) and a host attaching stores
  // spreads the definition, so the object reference is not stable either.
  const repositories = new Map<string, StateRepository>()

  return {
    register<Id, S>(
      state: StateModule<Id, S>,
      repository: StateRepository<Id, S>,
    ): void {
      repositories.set(state.identity, repository as StateRepository)
    },

    async load<Id, S>(
      state: StateModule<Id, S>,
      id: Id,
    ): Promise<LoadResult<S>> {
      const repo = repositories.get(state.identity)
      if (!repo) {
        throw new Error(
          `No repository registered for state "${state.name ?? state.identity}". ` +
          `Make sure it is included in the states array.`,
        )
      }
      return repo.load(id) as Promise<LoadResult<S>>
    },
  }
}
