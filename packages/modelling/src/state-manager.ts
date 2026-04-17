import type { EventCriteria } from "@kronos-ts/messaging"
import type { EntityModule } from "./entity.js"

/**
 * Metadata about what was sourced when loading entity state.
 * Used by the framework to build append conditions.
 */
export interface SourcingInfo {
  readonly criteria: EventCriteria
  readonly markerPosition: bigint
}

/**
 * Result of loading entity state — the state plus sourcing metadata.
 */
export interface LoadResult<S = unknown> {
  readonly state: S
  readonly sourcingInfo: SourcingInfo
}

/**
 * A repository that knows how to load the state of a specific entity type
 * by sourcing events from the event store and folding them through evolvers.
 */
export interface EntityRepository<Id = unknown, S = unknown> {
  readonly entityName: string
  load(id: Id): Promise<LoadResult<S>>
  /**
   * Load entity state, creating the initial state if no events exist.
   * Unlike `load()`, this never fails for a new entity — it returns
   * the `create()` state with empty sourcing info.
   */
  loadOrCreate(id: Id): Promise<LoadResult<S>>
}

/**
 * Manages entity repositories and provides the `load()` capability
 * to command and event handlers.
 */
export interface StateManager {
  register<Id, S>(
    entity: EntityModule<Id, S>,
    repository: EntityRepository<Id, S>,
  ): void

  load<Id, S>(
    entity: EntityModule<Id, S>,
    id: Id,
  ): Promise<LoadResult<S>>
}

export function createStateManager(): StateManager {
  const repositories = new Map<string, EntityRepository>()

  return {
    register<Id, S>(
      entity: EntityModule<Id, S>,
      repository: EntityRepository<Id, S>,
    ): void {
      repositories.set(entity.name, repository as EntityRepository)
    },

    async load<Id, S>(
      entity: EntityModule<Id, S>,
      id: Id,
    ): Promise<LoadResult<S>> {
      const repo = repositories.get(entity.name)
      if (!repo) {
        throw new Error(
          `No repository registered for entity "${entity.name}". ` +
          `Make sure it is included in the entities array.`,
        )
      }
      return repo.load(id) as Promise<LoadResult<S>>
    },
  }
}
