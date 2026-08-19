import { queryItems, type EventQuery, type QueryItem } from "../query/event-query.js"
import type { EventMessage } from "../messages/message.js"
import type { UnitOfWork } from "./unit-of-work.js"

// ---------------------------------------------------------------------------
// Event flush — the single place buffered appends become an event-store write.
//
// `ctx.append()` only BUFFERS onto the unit of work; someone must register a
// PREPARE_COMMIT hook that flushes the buffer to a store, with the append
// condition derived from what the handler load()ed (the DCB consistency check).
// Only the COMMAND path registers it — dispatch always opens a fresh
// UnitOfWork, so the command handler is the sole decide-and-append boundary.
// Event handlers deliberately have no `append` (see handler-context.ts).
// ---------------------------------------------------------------------------

export interface EventFlushStore {
  append(
    events: ReadonlyArray<EventMessage>,
    condition?: unknown,
    uow?: UnitOfWork,
  ): Promise<unknown>
}

export interface EventFlushOptions {
  eventStore: EventFlushStore
  tagResolver?: (event: EventMessage) => Array<{ key: string; value: string }>
  /** Command-path hook: lets a handler definition override the sourced query. */
  appendCondition?: (sourcedQuery: EventQuery) => EventQuery
}

/**
 * Register the flush on `uow` (no-op if already registered — the
 * `flushRegistered` flag on the unit of work is the once-per-UoW guard).
 *
 * At PREPARE_COMMIT: resolve tags, build the append condition from the unit of
 * work's sourcing infos (every `load()` contributed a query + a consistency
 * marker), and append the buffer in one store call — inside the unit of work,
 * so the write joins its transaction.
 */
export function registerEventFlush(uow: UnitOfWork, options: EventFlushOptions): void {
  if (uow.events.flushRegistered) return
  uow.events.flushRegistered = true
  uow.onPrepareCommit(async () => {
    const buffered = uow.events.buffered
    if (buffered.length === 0) return

    // Correlation data is applied per-event at append() time, so the buffer
    // already carries the active lineage here.
    const resolvedEvents = options.tagResolver
      ? buffered.map((event) => ({
          ...event,
          tags: [...event.tags, ...options.tagResolver!(event)],
        }))
      : buffered

    const sourcingInfos = uow.events.sourcingInfos
    let appendCondition: unknown
    if (sourcingInfos.length > 0) {
      // Combining queries is a CONCAT of their items, because the items of a
      // query are ORed — no nesting, no `either` wrapper to build by hand.
      const combined: ReadonlyArray<QueryItem> = sourcingInfos.flatMap((s) =>
        queryItems(s.query),
      )
      const maxMarker = sourcingInfos.reduce(
        (max, s) => (s.markerPosition > max ? s.markerPosition : max),
        -1n,
      )
      const query = options.appendCondition ? options.appendCondition(combined) : combined
      appendCondition = { query, marker: { position: maxMarker } }
    }

    await options.eventStore.append(resolvedEvents, appendCondition, uow)
  })
}
