import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import { BUFFERED_EVENTS_KEY, SOURCING_INFOS_KEY } from "@kronos-ts/eventsourcing"
import type { EventCriteria } from "./event-criteria.js"
import type { EventMessage } from "./message.js"
import { getResource, hasResource, onPrepareCommit, setResource } from "./processing-state.js"

// ---------------------------------------------------------------------------
// Event flush — the single place buffered appends become an event-store write.
//
// `ctx.append()` only BUFFERS into the active UnitOfWork; someone must register
// a PREPARE_COMMIT hook that flushes the buffer to a store, with the append
// condition derived from what the handler load()ed (the DCB consistency check).
// Historically only the command path registered it. Processors now do too, so
// STATEFUL event handlers (automations that load state and append facts
// directly, without a command hop) work — and a UnitOfWork that buffers events
// with nowhere to flush them FAILS at commit instead of dropping them.
// ---------------------------------------------------------------------------

/** Once-per-UoW guard. Nested dispatch re-enters invocation inside the same ALS
 *  state; without this every nesting level appends the same buffer again. */
export const EVENT_FLUSH_REGISTERED_KEY: ResourceKey<boolean> = resourceKey("eventFlushRegistered")

export interface EventFlushStore {
  append(events: ReadonlyArray<EventMessage>, condition?: unknown): Promise<unknown>
}

export interface EventFlushOptions {
  eventStore: EventFlushStore
  tagResolver?: { resolve(event: EventMessage): Array<{ key: string; value: string }> }
  /** Command-path hook: lets a handler definition override the sourced criteria. */
  appendCondition?: (sourcedCriteria: EventCriteria) => EventCriteria
}

/**
 * Register the flush for the ACTIVE UnitOfWork (no-op if already registered).
 * At PREPARE_COMMIT: resolve tags, build the append condition from the UoW's
 * sourcing infos (every `load()` contributed criteria + a consistency marker),
 * and append the buffer in one store call.
 */
export function registerEventFlush(options: EventFlushOptions): void {
  if (hasResource(EVENT_FLUSH_REGISTERED_KEY)) return
  setResource(EVENT_FLUSH_REGISTERED_KEY, true)
  onPrepareCommit(async () => {
    const buffered = getResource(BUFFERED_EVENTS_KEY)
    if (!buffered || buffered.length === 0) return

    // Correlation data is applied per-event at append() time, so the buffer
    // already carries the active lineage here.
    const resolvedEvents = options.tagResolver
      ? buffered.map((event) => ({
          ...event,
          tags: [...event.tags, ...options.tagResolver!.resolve(event)],
        }))
      : buffered

    const sourcingInfos = getResource(SOURCING_INFOS_KEY) ?? []
    let appendCondition: unknown
    if (sourcingInfos.length > 0) {
      const combinedCriteria: EventCriteria =
        sourcingInfos.length === 1
          ? sourcingInfos[0]!.criteria
          : { kind: "either" as const, criteria: sourcingInfos.map((s) => s.criteria) }
      const maxMarker = sourcingInfos.reduce(
        (max, s) => (s.markerPosition > max ? s.markerPosition : max),
        -1n,
      )
      const finalCriteria = options.appendCondition
        ? options.appendCondition(combinedCriteria)
        : combinedCriteria
      appendCondition = { criteria: finalCriteria, marker: { position: maxMarker } }
    }

    await options.eventStore.append(resolvedEvents, appendCondition)
  })
}

/**
 * For a UnitOfWork with NO event store (a hand-built processor without one):
 * turn "appended events with nowhere to go" into a commit-time error instead of
 * a silent drop. Registered in place of the flush.
 */
export function registerEventFlushGuard(processorName: string): void {
  if (hasResource(EVENT_FLUSH_REGISTERED_KEY)) return
  setResource(EVENT_FLUSH_REGISTERED_KEY, true)
  onPrepareCommit(async () => {
    const buffered = getResource(BUFFERED_EVENTS_KEY)
    if (!buffered || buffered.length === 0) return
    throw new Error(
      `Processor "${processorName}": ${buffered.length} event(s) were appended in this ` +
        `UnitOfWork, but the processor was built without an eventStore to flush them to. ` +
        `Pass eventStore (kronos does this automatically) or dispatch a command instead.`,
    )
  })
}
