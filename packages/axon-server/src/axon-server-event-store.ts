import {
  qualifiedNameToString,
  qualifiedNameFromString,
  type Tag,
  type Serializer,
} from "@kronos-ts/core"
import type {
  EventCriteria,
  EventMessage,
  MessageStream,
  SequencedEvent,
  StreamingCondition,
} from "@kronos-ts/core"
import { compileQuery } from "@kronos-ts/core"
import type {
  EventStore,
  SourcingResult,
  SourcingCondition,
  AppendCondition,
  ConsistencyMarker,
  AppendTransaction,
} from "@kronos-ts/core"
import type { TrackingToken } from "@kronos-ts/core"
import { globalSequenceToken, FIRST_TOKEN, positiveInteger } from "@kronos-ts/core"
import { markerAt, noMarker } from "@kronos-ts/core"
import type { AxonServerStoreSource } from "./connection.js"
import { contextView } from "./context-view.js"
import { AxonServerErrorCode, ConcurrencyError, fromGrpc } from "./errors.js"
import { boundedRead } from "./bounded-read.js"
import type {
  Criterion,
  TagsAndNamesCriterion,
  Tag as ProtoTag,
  TaggedEvent,
  Event as ProtoEvent,
  SourceEventsResponse,
  StreamEventsRequest,
  StreamEventsResponse,
} from "./generated/dcb.js"

// ---------------------------------------------------------------------------
// Criteria conversion — framework EventCriteria → proto Criterion[]
// ---------------------------------------------------------------------------

function tagToProto(tag: Tag): ProtoTag {
  const encoder = new TextEncoder()
  return {
    key: encoder.encode(tag.key),
    value: encoder.encode(tag.value),
  }
}

function tagFromProto(tag: ProtoTag): Tag {
  const decoder = new TextDecoder()
  return {
    key: decoder.decode(tag.key),
    value: decoder.decode(tag.value),
  }
}

function criteriaToCriterions(criteria: EventCriteria): Criterion[] {
  switch (criteria.kind) {
    case "tags":
      return [
        {
          tagsAndNames: {
            name: [],
            tag: criteria.tags.map(tagToProto),
          },
        },
      ]

    case "type-restricted":
      // Inner must be tags or any-tag
      const innerTags = criteria.inner.kind === "tags" ? criteria.inner.tags.map(tagToProto) : []
      return [
        {
          tagsAndNames: {
            name: [...criteria.types],
            tag: innerTags,
          },
        },
      ]

    case "either":
      // Flatten all sub-criteria into a list of criterions (OR semantics)
      return criteria.criteria.flatMap(criteriaToCriterions)

    case "any-tag":
      // Match any tagged event — empty criterion
      return [{ tagsAndNames: { name: [], tag: [] } }]
  }
}

// ---------------------------------------------------------------------------
// Event conversion — framework EventMessage ↔ proto Event/TaggedEvent
// ---------------------------------------------------------------------------

function createEventConverters(serializer: Serializer) {
  return {
    eventToProto(event: EventMessage): TaggedEvent {
      const name = qualifiedNameToString(event.name)
      const serialized = serializer.serialize(event.payload, name, event.version)
      return {
        event: {
          identifier: event.identifier,
          timestamp: BigInt(event.timestamp),
          name,
          version: event.version,
          payload: serialized.data,
          metadata: Object.fromEntries(
            Object.entries(event.metadata).map(([k, v]) => [k, String(v)]),
          ),
        },
        tag: event.tags.map(tagToProto),
      }
    },

    eventFromProto(protoEvent: ProtoEvent, tags: ProtoTag[]): EventMessage {
      const payload =
        protoEvent.payload.length > 0
          ? serializer.deserialize({
              data: protoEvent.payload,
              type: protoEvent.name,
              revision: protoEvent.version,
            })
          : {}

      const metadata: Record<string, unknown> = {}
      for (const [k, v] of Object.entries(protoEvent.metadata)) {
        metadata[k] = v
      }

      return {
        kind: "event",
        identifier: protoEvent.identifier,
        name: qualifiedNameFromString(protoEvent.name),
        version: protoEvent.version,
        payload,
        metadata,
        timestamp: Number(protoEvent.timestamp),
        tags: tags.map(tagFromProto),
      }
    },
  }
}

// ---------------------------------------------------------------------------
// AxonServerDcbEventStore — implements our EventStore interface via gRPC
// ---------------------------------------------------------------------------

/**
 * An EventStore over Axon Server's DCB event store, in one context.
 *
 * This bridges the framework's EventStore interface to the gRPC
 * DcbEventStore service, handling conversion between framework types
 * and proto messages.
 *
 * `context` is Axon Server's tenancy boundary and is a per-call header, so
 * several contexts share the one channel `conn` holds — see `contextView`. The
 * serializer is the connection's: one codec for everything this client puts on
 * the wire.
 *
 * The {@link open} method returns a persistent {@link MessageStream} backed by
 * a single gRPC Stream RPC call that stays open indefinitely, aligned with
 * Java's infinite {@code ResultStream}.
 */
/**
 * Axon Server's way of saying "there is nothing at or after that position".
 *
 * Matched on the gRPC status code first — `OUT_OF_RANGE` is 11 — with the
 * server's own message as a second, narrower gate, so an unrelated future
 * OUT_OF_RANGE is not swallowed along with it.
 */
function isStartPastHead(err: unknown): boolean {
  const code = (err as { code?: unknown }).code
  const details = String((err as { details?: unknown }).details ?? "")
  return code === 11 && details.includes("Start sequence cannot be larger than end sequence")
}

/**
 * Axon Server's way of refusing an append whose condition was violated.
 *
 * The server (2025.2) answers it with gRPC status `CANCELLED`, which is 1, and
 * the exception's class in the details; its error-code trailer is the generic
 * `AXONIQ-0001`. A call cancelled for any other reason has different details,
 * so it is not mistaken for a conflict.
 */
function isConditionRefused(err: unknown): boolean {
  const code = (err as { code?: unknown }).code
  const details = String((err as { details?: unknown }).details ?? "")
  return code === 1 && details.includes("ConsistencyConditionException")
}

/**
 * How many `GetTags` calls a read has in flight at once. `Source` and `Stream`
 * answer with an event and its sequence only, so each event's tags are asked
 * for by sequence.
 */
const TAG_LOOKUPS = 64

/** A `Stream` response with the event's tags beside it. */
type TaggedResponse = { event: { sequence: bigint; event: ProtoEvent; tag: ProtoTag[] } }

/**
 * The events of a `Stream` call, each with its tags, in the call's order.
 *
 * Reads up to {@link TAG_LOOKUPS} events ahead of its consumer so their lookups
 * run side by side, and hands an event out once its own tags have arrived and
 * every event before it has been handed out. A call that fails is reported
 * after the events read before the failure. `stop` is called when this ends,
 * however it ends.
 */
async function* withTags(
  responses: AsyncIterable<StreamEventsResponse>,
  tagsOf: (sequence: bigint) => Promise<ProtoTag[]>,
  stop: () => void,
): AsyncGenerator<TaggedResponse> {
  const call = responses[Symbol.asyncIterator]()
  const ahead: Array<Promise<TaggedResponse>> = []
  let reading: Promise<"read"> | undefined
  let ended = false
  let failure: { error: unknown } | undefined

  /** Take one response off the call and start its lookup. Never rejects. */
  async function read(): Promise<"read"> {
    try {
      const response = await call.next()
      if (response.done) {
        ended = true
      } else if (response.value.event?.event) {
        const { sequence, event } = response.value.event
        const lookup = tagsOf(sequence).then((tag) => ({ event: { sequence, event, tag } }))
        // A failed lookup is reported when this event's turn comes.
        lookup.catch(() => {})
        ahead.push(lookup)
      }
    } catch (error) {
      ended = true
      failure = { error }
    }
    reading = undefined
    return "read"
  }

  try {
    for (;;) {
      if (!ended && reading === undefined && ahead.length < TAG_LOOKUPS) reading = read()
      const next = ahead[0]
      if (next === undefined) {
        if (reading !== undefined) await reading
        else if (failure !== undefined) throw failure.error
        else return
        continue
      }
      const settled = () => "tags" as const
      if (reading === undefined || (await Promise.race([next.then(settled, settled), reading])) === "tags") {
        ahead.shift()
        yield await next
      }
    }
  } finally {
    stop()
  }
}

export function axonServerEventStore(conn: AxonServerStoreSource, context: string): EventStore {
  const { connection, serializer, metadata: createAxonMetadata } = contextView(conn, context)
  const { eventToProto, eventFromProto } = createEventConverters(serializer)

  /** The tags stored with the event at `sequence`. */
  async function tagsOf(sequence: bigint, signal: AbortSignal): Promise<ProtoTag[]> {
    const response = await connection.eventStore.getTags({ sequence }, { metadata: createAxonMetadata(), signal })
    return response.tag
  }

  /**
   * The `Stream` call with each event's tags looked up. Cancelling `signal`
   * ends the call and its lookups, and so does a lookup that fails.
   */
  function taggedStream(
    request: StreamEventsRequest,
    options: { metadata: ReturnType<typeof createAxonMetadata>; signal: AbortSignal },
  ): AsyncIterable<TaggedResponse> {
    const call = new AbortController()
    if (options.signal.aborted) call.abort()
    else options.signal.addEventListener("abort", () => call.abort(), { once: true })
    return withTags(
      connection.eventStore.stream(request, { ...options, signal: call.signal }),
      (sequence) => tagsOf(sequence, call.signal),
      () => call.abort(),
    )
  }

  async function sourceOnce(condition: SourcingCondition, signal: AbortSignal): Promise<SourcingResult> {
    const criterions = criteriaToCriterions(compileQuery(condition.query))
    const start = condition.start ?? 0n

    const request = {
      fromSequence: start,
      criterion: criterions,
    }

    const found: Array<{ sequence: bigint; event: ProtoEvent }> = []
    let marker: ConsistencyMarker = noMarker()

    try {
      const stream = connection.eventStore.source(request, { metadata: createAxonMetadata(), signal })
      for await (const response of stream) {
        if (response.event?.event) {
          found.push({ sequence: response.event.sequence, event: response.event.event })
        }
        if (response.consistencyMarker !== undefined) {
          marker = markerAt(response.consistencyMarker)
        }
      }
    } catch (err) {
      // READING PAST THE HEAD IS AN EMPTY ANSWER, NOT AN ERROR — and Axon
      // Server disagrees, so this is where the two vocabularies are
      // reconciled. `Source` fails OUT_OF_RANGE with "Start sequence cannot
      // be larger than end sequence" whenever `fromSequence` is beyond the
      // global head, which is the ORDINARY STEADY STATE of a snapshotted
      // load: an entry written at the head means the very next read resumes
      // at head + 1 and legitimately finds nothing.
      //
      // The marker is `start - 1`, and it is exact rather than conservative:
      // the caller has already accounted for everything up to there (that is
      // what asking to start later MEANS), and nothing can exist after it or
      // the server would not have refused. So an append conditioned on this
      // read is checked against precisely the range that was read.
      if (!isStartPastHead(err)) throw err
      return { events: [], marker: start > 0n ? markerAt(start - 1n) : noMarker() }
    }

    const events: EventMessage[] = []
    for (let from = 0; from < found.length; from += TAG_LOOKUPS) {
      const batch = found.slice(from, from + TAG_LOOKUPS)
      const tags = await Promise.all(batch.map(({ sequence }) => tagsOf(sequence, signal)))
      batch.forEach(({ event }, index) => events.push(eventFromProto(event, tags[index]!)))
    }

    return { events, marker }
  }

  return {
    source(condition: SourcingCondition): Promise<SourcingResult> {
      return boundedRead(connection.config.readTimeoutMs, (signal) => sourceOnce(condition, signal))
    },

    async appendEvents(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<AppendTransaction> {
      const taggedEvents = newEvents.map(eventToProto)
      const request = {
        condition: condition
          ? {
              consistencyMarker: condition.marker.position,
              criterion: criteriaToCriterions(compileQuery(condition.query)),
            }
          : undefined,
        event: taggedEvents,
      }

      // Axon Server's Append RPC is atomic. Nothing is sent until commit(),
      // which sends the one request.
      let responseMarker: bigint | undefined

      return {
        async commit() {
          async function* requestStream() {
            yield request
          }
          try {
            const response = await connection.eventStore.append(requestStream(), {
              metadata: createAxonMetadata(),
            })
            responseMarker = response.consistencyMarker
          } catch (err) {
            if (!isConditionRefused(err)) throw fromGrpc(err)
            throw new ConcurrencyError(
              `consistency condition violated: ${String((err as { details?: unknown }).details)}`,
              AxonServerErrorCode.CONCURRENCY_EXCEPTION,
              { cause: err },
            )
          }
        },
        async afterCommit() {
          return markerAt(responseMarker ?? 0n)
        },
        rollback() {
          // Axon Server: if commit() was never called, nothing was sent
        },
      }
    },

    async append(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<ConsistencyMarker> {
      const tx = await this.appendEvents(newEvents, condition)
      await tx.commit()
      return tx.afterCommit()
    },

    open(condition: StreamingCondition): MessageStream<SequencedEvent> {
      // How many events the stream holds ready: 1000 when its reader names none.
      const readAhead = positiveInteger(condition.readAhead ?? 1000, "readAhead")
      const criterions = condition.query ? criteriaToCriterions(compileQuery(condition.query)) : []

      const request = {
        fromSequence: condition.position,
        criterion: criterions,
      }

      // `close()` must reach the server. This is a server-streaming call: the
      // client sends nothing after the request, so the only way to end it from
      // this side is to cancel it. Abandoning the async iterator does not do
      // that — the read below stays parked on the call until the next
      // event arrives, which at the head of a quiet stream is never, and every
      // closed stream leaves a live call behind on the server. With enough of
      // them open, Axon Server stops answering later `source` reads (a 2-CPU
      // container gets there after a handful): handlers never reply, and
      // commands die at the server's own timeout.
      const cancel = new AbortController()
      const grpcStream = taggedStream(request, {
        metadata: createAxonMetadata(),
        signal: cancel.signal,
      })

      const responses = grpcStream[Symbol.asyncIterator]()

      // Events read from the call and not yet taken: at most `readAhead`.
      const buffer: SequencedEvent[] = []
      let availableCallback: (() => void) | null = null
      let completed = false
      let streamError: Error | undefined
      let reading = false

      // Reads from the call until the buffer holds `readAhead` events, then
      // stops; `next()` starts it again. The DCB `Stream` call has no permits
      // to grant. What slows the server is HTTP/2 flow control: a message is
      // taken off the call only when it is asked for here, so while the buffer
      // is full the call's receive window fills and the server cannot send.
      async function read() {
        if (reading) return
        reading = true
        try {
          while (!completed && buffer.length < readAhead) {
            const response = await responses.next()
            if (completed) break
            if (response.done) {
              // Stream ended (shouldn't happen for infinite stream)
              completed = true
              availableCallback?.()
              break
            }
            const taggedEvent = response.value.event
            if (taggedEvent?.event) {
              buffer.push({
                sequence: taggedEvent.sequence,
                event: eventFromProto(taggedEvent.event, taggedEvent.tag),
              })
              availableCallback?.()
            }
          }
        } catch (err) {
          // Our own cancellation surfaces here as an AbortError; that is the
          // stream closing as asked, not a failure to report.
          if (!completed) {
            const failure = fromGrpc(err)
            streamError = failure instanceof Error ? failure : new Error(String(failure))
          }
          completed = true
          availableCallback?.()
        } finally {
          reading = false
        }
      }

      void read()

      return {
        next() {
          const event = buffer.shift()
          if (event !== undefined) void read()
          return event
        },

        peek() {
          return buffer[0]
        },

        hasNextAvailable() {
          return buffer.length > 0
        },

        isCompleted() {
          return completed && buffer.length === 0
        },

        error() {
          return streamError
        },

        setCallback(callback: () => void) {
          availableCallback = callback
        },

        close() {
          completed = true
          availableCallback = null
          cancel.abort()
        },
      }
    },

    async getHeadPosition(): Promise<bigint> {
      const response = await boundedRead(connection.config.readTimeoutMs, (signal) =>
        connection.eventStore.getHead({}, { metadata: createAxonMetadata(), signal }),
      )
      return response.sequence
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      const response = await boundedRead(connection.config.readTimeoutMs, (signal) =>
        connection.eventStore.getHead({}, { metadata: createAxonMetadata(), signal }),
      )
      return globalSequenceToken(response.sequence)
    },
  }
}
