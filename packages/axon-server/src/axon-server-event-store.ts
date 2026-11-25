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

    /**
     * A `Source` or `Stream` response holds an event and its sequence, and
     * not the tags it was appended with, so the event has none.
     */
    eventFromProto(protoEvent: ProtoEvent): EventMessage {
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
        tags: [],
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

export function axonServerEventStore(conn: AxonServerStoreSource, context: string): EventStore {
  const { connection, serializer, metadata: createAxonMetadata } = contextView(conn, context)
  const { eventToProto, eventFromProto } = createEventConverters(serializer)

  /**
   * The marker for a read from `start` that found nothing because it started
   * at or past the head. An Axon Server marker is the next sequence: the
   * caller accounted for everything before `start`, and nothing exists at or
   * after the head, so the marker is whichever of the two comes first.
   */
  async function markerPastHead(start: bigint, signal: AbortSignal): Promise<ConsistencyMarker> {
    const { sequence: head } = await connection.eventStore.getHead({}, { metadata: createAxonMetadata(), signal })
    return markerAt(head < start ? head : start)
  }

  async function sourceOnce(condition: SourcingCondition, signal: AbortSignal): Promise<SourcingResult> {
    const criterions = criteriaToCriterions(compileQuery(condition.query))
    const start = condition.start ?? 0n

    const request = {
      fromSequence: start,
      criterion: criterions,
    }

    const events: EventMessage[] = []
    let marker: ConsistencyMarker = noMarker()

    try {
      const stream = connection.eventStore.source(request, { metadata: createAxonMetadata(), signal })
      for await (const response of stream) {
        if (response.event?.event) events.push(eventFromProto(response.event.event))
        if (response.consistencyMarker !== undefined) {
          marker = markerAt(response.consistencyMarker)
        }
      }
    } catch (err) {
      // Reading past the head finds nothing; it is not an error. Axon Server
      // 2025.2 fails `Source` with OUT_OF_RANGE when `fromSequence` is beyond
      // the head.
      if (!isStartPastHead(err)) throw err
      return { events: [], marker: await markerPastHead(start, signal) }
    }

    // Axon Server 2026.1 answers a read past the head with no events and the
    // start as its marker, and then refuses every append conditioned on it,
    // because that marker is after its head. A read that starts exactly at the
    // head gets the same answer, which is valid as it is, so only the head can
    // tell the two apart.
    if (events.length === 0 && start > 0n && marker.position === start) {
      return { events: [], marker: await markerPastHead(start, signal) }
    }

    return { events, marker }
  }

  return {
    // Axon Server matches a query against an event's tags and returns the
    // event without them.
    returnsTags: false,

    source(condition: SourcingCondition): Promise<SourcingResult> {
      return boundedRead(connection.config.readTimeoutMs, (signal) => sourceOnce(condition, signal))
    },

    async append(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<ConsistencyMarker> {
      const request = {
        condition: condition
          ? {
              consistencyMarker: condition.marker.position,
              criterion: criteriaToCriterions(compileQuery(condition.query)),
            }
          : undefined,
        event: newEvents.map(eventToProto),
      }
      // One request: Axon Server's Append call takes the whole append or none of it.
      async function* requestStream() {
        yield request
      }
      try {
        const response = await connection.eventStore.append(requestStream(), {
          metadata: createAxonMetadata(),
        })
        return markerAt(response.consistencyMarker ?? 0n)
      } catch (err) {
        if (!isConditionRefused(err)) throw fromGrpc(err)
        throw new ConcurrencyError(
          `consistency condition violated: ${String((err as { details?: unknown }).details)}`,
          AxonServerErrorCode.CONCURRENCY_EXCEPTION,
          { cause: err },
        )
      }
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
      const grpcStream = connection.eventStore.stream(request, {
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
                event: eventFromProto(taggedEvent.event),
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
