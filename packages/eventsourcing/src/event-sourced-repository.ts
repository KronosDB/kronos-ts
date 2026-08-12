import { qualifiedNameToString } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"
import type { StateModule, StateRepository, LoadResult } from "@kronos-ts/modelling"
import type { EventStore } from "./event-store.js"
import { sourcingCondition } from "./sourcing-condition.js"
import type { SnapshotStore, Snapshot } from "./snapshot-store.js"
import type { SnapshotPolicy, EvolutionResult } from "./snapshot-policy.js"

export interface EventSourcedRepositoryOptions<Id, S> {
  state: StateModule<Id, S>
  eventStore: EventStore
  snapshotStore?: SnapshotStore
  snapshotPolicy?: SnapshotPolicy
}

/**
 * Creates a repository for a state module sourced from events.
 *
 * When `load(id)` is called, the repository:
 * 1. Checks the snapshot store for a cached state (if configured)
 * 2. Resolves the sourcing criteria from the state module + id
 * 3. Sources matching events from the event store (from snapshot position if available)
 * 4. Starts from snapshot state (or `create()`) and folds events through matching evolvers
 * 5. Optionally creates a new snapshot if the policy triggers
 * 6. Returns the state AND sourcing info (criteria + marker)
 */
export function eventSourcedRepository<Id, S>(
  module: StateModule<Id, S>,
  eventStore: EventStore,
  snapshotStore?: SnapshotStore,
  snapshotPolicy?: SnapshotPolicy,
): StateRepository<Id, S> {
  async function doLoad(id: Id): Promise<LoadResult<S>> {
    const startTime = performance.now()
    const criteria = module.criteria(id)

    // Try to load snapshot
    let state = module.create(id)
    let startPosition: bigint | undefined
    let snapshot: Snapshot | undefined

    if (snapshotStore) {
      try {
        snapshot = await snapshotStore.load(module.name, id)
        if (snapshot) {
          state = snapshot.payload as S
          // Source events AFTER the snapshot position
          startPosition = snapshot.position + 1n
        }
      } catch (err) {
        console.warn(`Failed to load snapshot for ${module.name}:${String(id)}, falling back to full replay:`, err)
        // Fall back to full replay from the beginning
      }
    }

    const condition = sourcingCondition(criteria, startPosition)
    const { events, marker } = await eventStore.source(condition)

    const lifecycle = module.lifecycle
    let isFirstEvent = !snapshot // first event only if no snapshot
    let wasDeleted = lifecycle?.isDeleted?.(state) ?? false

    let eventsApplied = 0
    for (const event of events) {
      const previousState = state
      state = await applyEvent(module, state, event, id)
      eventsApplied++

      // Lifecycle hooks
      if (lifecycle && state !== previousState) {
        // onCreate: first event transitions from initial state
        if (isFirstEvent && eventsApplied === 1) {
          await lifecycle.onCreate?.(state, id)
        }

        // onStateChange: after each evolving event
        await lifecycle.onStateChange?.(previousState, state, event, id)

        // onDelete: when isDeleted transitions from false to true
        if (lifecycle.isDeleted) {
          const nowDeleted = lifecycle.isDeleted(state)
          if (nowDeleted && !wasDeleted) {
            await lifecycle.onDelete?.(state, id)
          }
          wasDeleted = nowDeleted
        }
      }
    }

    const sourcingTimeMs = performance.now() - startTime

    // Check if we should create a new snapshot (fire-and-forget)
    if (snapshotStore && snapshotPolicy && eventsApplied > 0) {
      const result: EvolutionResult = { eventsApplied, sourcingTimeMs }
      if (snapshotPolicy.shouldSnapshot(result)) {
        snapshotStore
          .store(module.name, id, {
            position: marker.position,
            payload: state,
            timestamp: Date.now(),
            metadata: {},
          })
          .catch((err) => {
            console.warn(`Failed to store snapshot for ${module.name}:${String(id)}:`, err)
          })
      }
    }

    return {
      state,
      sourcingInfo: {
        criteria,
        markerPosition: marker.position,
      },
    }
  }

  return {
    stateName: module.name,
    load: doLoad,
    loadOrCreate: doLoad, // Same implementation — create() always provides initial state
  }
}

async function applyEvent<Id, S>(
  module: StateModule<Id, S>,
  state: S,
  event: EventMessage,
  id: Id,
): Promise<S> {
  const eventType = qualifiedNameToString(event.name)

  for (const evolver of module.evolvers) {
    const evolverType = qualifiedNameToString(evolver.descriptor.name)
    if (evolverType === eventType) {
      return await evolver.evolve(state, event)
    }
  }

  return state
}
