import type {
  AppendCondition,
  ConsistencyMarker,
  EventMessage,
  TaggedEvent,
  EventQuery,
  EventStore,
  MessageStream,
  SequencedEvent,
  SourcingCondition,
  SourcingResult,
  StreamingCondition,
  TrackingToken,
} from "@kronos-ts/core"
import { compileQuery, FIRST_TOKEN, globalSequenceToken, isUnavailable, markerAt, noMarker } from "@kronos-ts/core"
import { ConcurrencyError } from "./errors.js"
import type { KronosDbConnectionHandle } from "./kronosdb.js"
import { subscription, type SubscriptionOptions } from "./kronosdb-subscription.js"
import {
  criteriaToWire,
  outbound,
  readEvents,
  writeCondition,
  writeCriteria,
  writeRecords,
  type OutboundEvent,
  type WireCriterion,
} from "./native-events.js"
import { contextRequest, contextStream, unexpected } from "./native-requests.js"
import type { NativeTransport } from "./native-transport.js"
import {
  FLAG_END,
  FrameKind,
  decodeHead,
  reader,
  writeU16,
  writeU32,
  writeU64,
} from "./protocol/kronosdb-protocol.js"

/**
 * The criteria a READ sends, with KronosDB's one structural rule applied.
 *
 * @internal Shared with `kronosDbSnapshottingEventStore`, whose fused read must
 * select EXACTLY what the plain `Source` below selects — the fused read is the
 * same query with a cached prefix, so a second copy of this rule that drifted
 * would make the two paths disagree about which events a state is folded from.
 */
export function sourceCriteria(query: EventQuery): WireCriterion[] {
  const criteria = criteriaToWire(compileQuery(query))

  // An empty criteria list would match nothing on a condition, so a read that
  // means "everything" says so with one criterion that has no names and no tags.
  return criteria.length === 0 ? [{ names: [], tags: [] }] : criteria
}

/** The condition an append sends: the marker the decision read up to, and what it must not have missed. */
function wireCondition(condition: AppendCondition | undefined): { marker: bigint; criteria: WireCriterion[] } | undefined {
  if (condition === undefined) return undefined
  // The origin marker (-1) means "nothing was read": everything counts.
  const marker = condition.marker.position
  return { marker: marker < 0n ? 0n : marker, criteria: criteriaToWire(compileQuery(condition.query)) }
}

/**
 * The marker an Appended answers with. The hot path of every write, so it
 * reads the one field it needs from the frame instead of decoding the three.
 * A conflict is not an error on the wire but is one here: it is what the
 * caller's retry loop catches.
 */
function parseAppended(kind: number, buf: Uint8Array, start: number, end: number): bigint {
  if (kind === FrameKind.Appended) {
    // first_position u64, count u32, marker u64
    if (end - start < 20) throw unexpected(kind, "a complete Appended")
    return new DataView(buf.buffer, buf.byteOffset + start, 20).getBigUint64(12, true)
  }
  if (kind === FrameKind.AppendConflict) {
    const position = new DataView(buf.buffer, buf.byteOffset + start, 8).getBigUint64(0, true)
    throw new ConcurrencyError(`consistency condition violated: conflicting event at position ${position}`)
  }
  throw unexpected(kind, "Appended")
}

// ---------------------------------------------------------------------------
// KronosDB Event Store — implements the EventStore interface over the native protocol
// ---------------------------------------------------------------------------

/** How often a filtered stream hears how far the server has looked (ms). */
const PROGRESS_INTERVAL_MS = 1000

/** The wait before an append is sent again: this at first, doubling up to the longest (ms). */
const RETRY_FIRST_WAIT_MS = 50
const RETRY_LONGEST_WAIT_MS = 1000

/** A wait before try `attempt + 1`, between half and all of the doubled step so tries from many clients spread out. */
function retryWait(attempt: number): number {
  const step = Math.min(RETRY_FIRST_WAIT_MS * 2 ** attempt, RETRY_LONGEST_WAIT_MS)
  return step * (0.5 + Math.random() * 0.5)
}

/**
 * Creates an EventStore backed by KronosDB's native protocol.
 *
 * `context` is the KronosDB context this store addresses — a plain string, not
 * an object you build. Two stores on different contexts share the ONE socket on
 * `kdb`; the context id each request carries is the entire difference.
 *
 * `streaming` overrides how `open()` subscribes. A stream reads ahead as far as
 * the connection's `window` allows, in bytes, whatever its reader's
 * `readAhead` (the window, not an event count, is what bounds what the server
 * sends ahead of a slow reader). One with a query asks the server for a
 * Progress frame every second while nothing matches, which is how a processor
 * of rare events moves its token.
 *
 * A few things the wire gives that the framework's `EventStore` does not ask
 * for, kept out of this shape and available beside it: `Progress` frames
 * ({@link kronosDbSubscription}), the log's tail and a timestamp's position
 * ({@link kronosDbTail}, {@link kronosDbPositionAt}).
 *
 * An append the server answers `unavailable`, or whose connection is lost
 * before the answer, may or may not have been written. It is sent again, with
 * the same events, for up to the connection's `appendRetryMs`: a server that
 * already holds them answers with the positions they have and writes nothing.
 * So an append rides through a failover or a restart, and fails as
 * unavailable only when the server stayed unreachable for that long. A
 * conflict is never sent again.
 */
export function kronosDbEventStore(
  kdb: Pick<KronosDbConnectionHandle, "connection" | "serializer">,
  context: string = kdb.connection.config.context,
  streaming: SubscriptionOptions = {},
): EventStore {
  const link = kdb.connection.native
  const serializer = kdb.serializer

  const retryMs = kdb.connection.config.appendRetryMs

  async function send(events: ReadonlyArray<OutboundEvent>, condition: AppendCondition | undefined): Promise<bigint> {
    const wired = wireCondition(condition)
    const once = () =>
      contextRequest(
        link,
        context,
        FrameKind.Append,
        (w, id) => {
          writeU16(w, id)
          writeCondition(w, wired)
          writeRecords(w, events)
        },
        parseAppended,
      )

    let failure: unknown
    try {
      return await once()
    } catch (error) {
      failure = error
    }

    // The server recognises a retry by its events' identifiers, so an event
    // without one would be written a second time.
    if (retryMs <= 0 || events.some(({ event }) => event.identifier === "")) throw failure
    const until = Date.now() + retryMs
    for (let attempt = 0; ; attempt++) {
      // A conflict, a refusal, bad credentials: another try changes nothing.
      if (!isUnavailable(failure)) throw failure
      const wait = retryWait(attempt)
      if (Date.now() + wait >= until) throw failure
      await new Promise((resolve) => setTimeout(resolve, wait))
      if (kdb.connection.state === "closed") throw failure

      let transport: NativeTransport
      try {
        transport = link.current() ?? (await link.ready())
      } catch (error) {
        // No server to send it to yet.
        failure = error
        continue
      }
      // The server that gets the retry is the one that must recognise it.
      if (!transport.welcome.retrySafeAppends) throw failure
      try {
        return await once()
      } catch (error) {
        failure = error
      }
    }
  }

  async function head(): Promise<bigint> {
    return contextRequest(
      link,
      context,
      FrameKind.GetHead,
      (w, id) => writeU16(w, id),
      (kind, buf, start, end) => {
        if (kind !== FrameKind.Head) throw unexpected(kind, "Head")
        return decodeHead(reader(buf, start, end)).position
      },
    )
  }

  return {
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const criteria = sourceCriteria(condition.query)
      const from = condition.start === undefined || condition.start < 0n ? 0n : condition.start

      const events: EventMessage[] = []
      let marker: ConsistencyMarker = noMarker()

      // A Source is a request answered by a run of Events frames; the last one
      // carries END and the marker. 0n is a valid marker for an empty store, so
      // presence — not truthiness — decides.
      return new Promise<SourcingResult>((resolve, reject) => {
        contextStream(
          link,
          context,
          FrameKind.Source,
          (w, id) => {
            writeU16(w, id)
            writeU64(w, from)
            writeCriteria(w, criteria)
            // Events per frame; 0 lets the server pick its default.
            writeU32(w, 0)
          },
          true,
          (kind, flags, buf, start, end) => {
            if (kind !== FrameKind.Events) throw unexpected(kind, "Events")
            const reached = readEvents(buf, start, end, serializer, events)
            if (reached !== undefined) marker = markerAt(reached)
            if ((flags & FLAG_END) !== 0) resolve({ events, marker })
          },
          reject,
        ).catch(reject)
      })
    },

    async append(newEvents: ReadonlyArray<TaggedEvent>, condition?: AppendCondition): Promise<ConsistencyMarker> {
      return markerAt(await send(outbound(serializer, newEvents), condition))
    },

    open(condition: StreamingCondition): MessageStream<SequencedEvent> {
      return subscription(link, serializer, context, condition, {
        // A query is a selective reader, and a selective reader needs to hear
        // how far the server has looked while nothing matches.
        ...(condition.query !== undefined ? { progressIntervalMs: PROGRESS_INTERVAL_MS } : {}),
        ...streaming,
      })
    },

    async getHeadPosition(): Promise<bigint> {
      return head()
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      return globalSequenceToken(await head())
    },
  }
}
