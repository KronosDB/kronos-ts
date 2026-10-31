import type { EventStore } from "../event-sourcing/event-store.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { MessageStream, SequencedEvent, StreamingCondition } from "./source.js"
import { globalSequenceToken, type TrackingToken } from "./tracking-token.js"

/**
 * ONE READ OF THE LOG, HANDED TO EVERY SEGMENT A NODE HOLDS. Each segment opens
 * its own stream here, exactly as it would on the store, and gets the events it
 * accepts; the log underneath is read once, from wherever the furthest-behind
 * segment is.
 *
 * - A segment opened behind where the log has been read reopens it from there.
 *   The others skip what they have already seen, so a segment that failed and
 *   comes back costs a re-read, not a second stream.
 * - The read stops while ANY segment has a full buffer: the slowest segment sets
 *   the pace, and memory stays bounded at a buffer per segment.
 * - Every stream says how far it has looked (`progressToken`) — through the
 *   events it was not given, which are most of them when there are many
 *   segments. That is what moves a quiet segment's token.
 */
export type SharedStream = {
  open(condition: StreamingCondition, accepts: (event: SequencedEvent) => boolean): MessageStream<SequencedEvent>
}

type View = {
  /** The position of the next event this view has not looked at. */
  next: bigint
  /**
   * Events below this were seen before the log was reopened behind this view,
   * and are skipped. Zero for the view the log was reopened for: a store that
   * resumes from a cursor (Postgres's commit order) can hand out an event with
   * a lower position than the one before it, and that event is new.
   */
  skipBelow: bigint
  /** The token that resumes the log at `next` — what a reopen starts from. */
  resume: TrackingToken | undefined
  /** The token just past the last event this view looked at. */
  looked: TrackingToken | undefined
  readonly accepts: (event: SequencedEvent) => boolean
  readonly buffer: SequencedEvent[]
  head: number
  callback: (() => void) | null
}

export function sharedStream(store: EventStore, options: { query?: EventQuery; readAhead: number }): SharedStream {
  const views = new Set<View>()
  let inner: MessageStream<SequencedEvent> | null = null
  /** The position of the next event the log underneath hands out. */
  let at = 0n

  const ready = (view: View) => view.buffer.length - view.head

  function reopen() {
    inner?.close()
    inner = null
    let from: View | undefined
    for (const view of views) if (from === undefined || view.next < from.next) from = view
    if (from === undefined) return
    at = from.next
    for (const view of views) view.skipBelow = view.next > from.next ? view.next : 0n
    inner = store.open({
      position: from.next,
      ...(from.resume !== undefined ? { token: from.resume } : {}),
      ...(options.query !== undefined ? { query: options.query } : {}),
      readAhead: options.readAhead,
    })
    inner.setCallback(() => {
      for (const view of views) view.callback?.()
    })
  }

  function full(): boolean {
    for (const view of views) if (ready(view) >= options.readAhead) return true
    return false
  }

  function read() {
    if (inner === null) return
    // A view handed events by ANOTHER view's read was not woken by the log —
    // the log called back once, before any of it was read. Wake it here, or a
    // segment that went to sleep with an empty buffer sleeps on a full one, and
    // the full one holds the read up for everybody.
    const given = new Set<View>()
    while (!full() && inner.hasNextAvailable()) {
      const event = inner.next()!
      const past = event.token ?? globalSequenceToken(event.sequence + 1n)
      at = event.sequence + 1n
      for (const view of views) {
        if (event.sequence < view.skipBelow) continue
        view.next = at
        view.resume = past
        view.looked = past
        if (view.accepts(event)) {
          view.buffer.push(event)
          given.add(view)
        }
      }
    }
    for (const view of given) view.callback?.()
  }

  return {
    open(condition, accepts) {
      const view: View = {
        next: condition.position,
        skipBelow: 0n,
        resume: condition.token,
        looked: undefined,
        accepts,
        buffer: [],
        head: 0,
        callback: null,
      }
      views.add(view)
      if (inner === null || inner.error() !== undefined || view.next < at) reopen()
      else view.skipBelow = view.next

      const take = (): SequencedEvent | undefined => {
        if (ready(view) === 0) read()
        if (ready(view) === 0) return undefined
        const event = view.buffer[view.head++]!
        if (view.head === view.buffer.length) {
          view.buffer.length = 0
          view.head = 0
        }
        return event
      }

      return {
        next: take,
        peek() {
          if (ready(view) === 0) read()
          return ready(view) > 0 ? view.buffer[view.head] : undefined
        },
        hasNextAvailable() {
          if (ready(view) === 0) read()
          return ready(view) > 0
        },
        isCompleted: () => inner?.isCompleted() ?? false,
        error: () => inner?.error(),
        setCallback(callback) {
          view.callback = callback
        },
        close() {
          view.callback = null
          views.delete(view)
          if (views.size === 0) {
            inner?.close()
            inner = null
          }
        },
        progressToken() {
          if (ready(view) > 0) return undefined
          // Caught up with the log underneath, a store that filters may have
          // looked further than the last event it handed out.
          const beyond = view.next >= at && inner !== null && !inner.hasNextAvailable() ? inner.progressToken?.() : undefined
          if (beyond !== undefined && (view.looked === undefined || beyond.position() > view.looked.position())) return beyond
          return view.looked
        },
      }
    },
  }
}
