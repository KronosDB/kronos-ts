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
import { globalSequenceToken, FIRST_TOKEN } from "@kronos-ts/core"
import { markerAt, noMarker } from "@kronos-ts/core"
import type { AxonServerStoreSource } from "./connection.js"
import { contextView } from "./context-view.js"
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

export function axonServerEventStore(conn: AxonServerStoreSource, context: string): EventStore {
  const { connection, serializer, metadata: createAxonMetadata } = contextView(conn, context)
  const { eventToProto, eventFromProto } = createEventConverters(serializer)


  return {
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const criterions = criteriaToCriterions(compileQuery(condition.query))
      const start = condition.start ?? 0n

      const request = {
        fromSequence: start,
        criterion: criterions,
      }

      const events: EventMessage[] = []
      let marker: ConsistencyMarker = noMarker()

      try {
        const stream = connection.eventStore.source(request, { metadata: createAxonMetadata() })
        for await (const response of stream) {
          if (response.event) {
            const taggedEvent = response.event
            const protoEvent = taggedEvent.event
            if (protoEvent) {
              // DCB source/stream responses carry no tags — the server indexes
              // them write-side but does not echo them back (SequencedEvent has
              // only sequence + event). Reconstructed events get empty tags.
              events.push(eventFromProto(protoEvent, []))
            }
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

      return { events, marker }
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

      // Axon Server's Append RPC is atomic — commit happens on the server
      // We send the request eagerly and wrap the response in a transaction
      let responseMarker: bigint | undefined

      return {
        async commit() {
          async function* requestStream() {
            yield request
          }
          const response = await connection.eventStore.append(requestStream(), {
            metadata: createAxonMetadata(),
          })
          responseMarker = response.consistencyMarker
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
      const criterions = condition.query ? criteriaToCriterions(compileQuery(condition.query)) : []

      const request = {
        fromSequence: condition.position,
        criterion: criterions,
      }

      const grpcStream = connection.eventStore.stream(request, { metadata: createAxonMetadata() })

      // Internal buffer for events pulled from the gRPC stream
      const buffer: SequencedEvent[] = []
      let availableCallback: (() => void) | null = null
      let completed = false
      let streamError: Error | undefined
      let reading = false

      // Background reader: pulls from gRPC stream into buffer
      async function startReading() {
        if (reading) return
        reading = true
        try {
          for await (const response of grpcStream) {
            if (completed) break
            const taggedEvent = response.event
            if (taggedEvent?.event) {
              buffer.push({
                sequence: taggedEvent.sequence,
                event: eventFromProto(taggedEvent.event, []),
              })
              availableCallback?.()
            }
          }
          // Stream ended (shouldn't happen for infinite stream)
          completed = true
          availableCallback?.()
        } catch (err) {
          streamError = err instanceof Error ? err : new Error(String(err))
          completed = true
          availableCallback?.()
        }
      }

      startReading()

      return {
        next() {
          return buffer.shift()
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
          // gRPC stream will be cancelled when the async iterator is abandoned
        },
      }
    },

    async getHeadPosition(): Promise<bigint> {
      const response = await connection.eventStore.getHead({}, { metadata: createAxonMetadata() })
      return response.sequence
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      const response = await connection.eventStore.getHead({}, { metadata: createAxonMetadata() })
      return globalSequenceToken(response.sequence)
    },

  }
}
