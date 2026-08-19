import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { EventMessage } from "../messages/message.js"
import type { StateModule } from "./state.js"
import type { StateRepository, LoadResult } from "./state-manager.js"
import type { EventStore } from "../stores/event-store.js"
import { sourcingCondition } from "../stores/sourcing-condition.js"
import type { SnapshotStore, Snapshot } from "../stores/snapshot-store.js"
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
 * 2. Resolves the sourcing query from the state module + id
 * 3. Sources matching events from the event store (from snapshot position if available)
 * 4. Starts from snapshot state (or `create()`) and folds events through matching evolvers
 * 5. Optionally creates a new snapshot if the policy triggers
 * 6. Returns the state AND sourcing info (query + marker)
 */
export function eventSourcedRepository<Id, S>(
  module: StateModule<Id, S>,
  eventStore: EventStore,
  snapshotStore?: SnapshotStore,
  snapshotPolicy?: SnapshotPolicy,
): StateRepository<Id, S> {
  // Snapshots are keyed on the state's DURABLE name; a state without one has no
  // durable identity to store under, so it simply never snapshots. `kronos`
  // refuses to boot an unnamed state that was GIVEN a snapshot policy or store,
  // and this guard covers the same mistake made by constructing a repository
  // directly.
  const stateName = module.name
  if (snapshotPolicy && stateName === undefined) {
    throw new Error(
      "A snapshot policy was configured for a state with no `name`. " +
      "Snapshots are keyed on the state's durable name — add `name` to its `state({ ... })` definition, " +
      "or drop the snapshot policy.",
    )
  }
  const snapshots = stateName !== undefined ? snapshotStore : undefined

  async function doLoad(id: Id): Promise<LoadResult<S>> {
    const startTime = performance.now()
    const query = module.query(id)

    // Try to load snapshot
    let state = module.create(id)
    let startPosition: bigint | undefined
    let snapshot: Snapshot | undefined

    if (snapshots) {
      try {
        snapshot = await snapshots.load(stateName!, id)
        if (snapshot) {
          state = snapshot.payload as S
          // Source events AFTER the snapshot position
          startPosition = snapshot.position + 1n
        }
      } catch (err) {
        console.warn(`Failed to load snapshot for ${stateName}:${String(id)}, falling back to full replay:`, err)
        // Fall back to full replay from the beginning
      }
    }

    const condition = sourcingCondition(query, startPosition)
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
    if (snapshots && snapshotPolicy && eventsApplied > 0) {
      const result: EvolutionResult = { eventsApplied, sourcingTimeMs }
      if (snapshotPolicy.shouldSnapshot(result)) {
        snapshots
          .store(stateName!, id, {
            position: marker.position,
            payload: state,
            timestamp: Date.now(),
            metadata: {},
          })
          .catch((err) => {
            console.warn(`Failed to store snapshot for ${stateName}:${String(id)}:`, err)
          })
      }
    }

    return {
      state,
      sourcingInfo: {
        query,
        markerPosition: marker.position,
      },
    }
  }

  return {
    stateName,
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

  for (const [descriptor, evolve] of module.evolvers) {
    const evolverType = qualifiedNameToString(descriptor.name)
    if (evolverType === eventType) {
      return await evolve(state, event)
    }
  }

  return state
}
