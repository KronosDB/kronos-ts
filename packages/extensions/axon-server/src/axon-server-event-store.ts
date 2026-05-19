import {
  qualifiedNameToString,
  qualifiedNameFromString,
  type Tag,
  type Serializer,
} from "@kronos-ts/common"
import type {
  EventCriteria,
  EventMessage,
  MessageStream,
  SequencedEvent,
  StreamingCondition,
} from "@kronos-ts/messaging"
import { createMessageStream } from "@kronos-ts/messaging"
import type {
  EventStore,
  SourcingResult,
  SourcingCondition,
  AppendCondition,
  ConsistencyMarker,
  AppendTransaction,
} from "@kronos-ts/eventsourcing"
import type { TrackingToken } from "@kronos-ts/messaging"
import { globalSequenceToken, FIRST_TOKEN } from "@kronos-ts/messaging"
import { markerAt, noMarker } from "@kronos-ts/eventsourcing"
import type { AxonServerConnection } from "./connection.js"
import type {
  Criterion,
  TagsAndNamesCriterion,
  Tag as ProtoTag,
  TaggedEvent,
  Event as ProtoEvent,
  SourceEventsResponse,
} from "./generated/dcb.js"
import { Metadata } from "nice-grpc"

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
      return [{
        tagsAndNames: {
          name: [],
          tag: criteria.tags.map(tagToProto),
        },
      }]

    case "type-restricted":
      // Inner must be tags or any-tag
      const innerTags = criteria.inner.kind === "tags"
        ? criteria.inner.tags.map(tagToProto)
        : []
      return [{
        tagsAndNames: {
          name: [...criteria.types],
          tag: innerTags,
        },
      }]

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
      const payload = protoEvent.payload.length > 0
        ? serializer.deserialize({ data: protoEvent.payload, type: protoEvent.name, revision: protoEvent.version })
        : {}

      const metadata: Record<string, unknown> = {}
      for (const [k, v] of Object.entries(protoEvent.metadata)) {
        metadata[k] = v
      }

      return {
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
 * Creates an EventStore implementation backed by Axon Server's DCB event store.
 *
 * This bridges the framework's EventStore interface to the gRPC
 * DcbEventStore service, handling conversion between framework types
 * and proto messages.
 *
 * The {@link open} method returns a persistent {@link MessageStream} backed by
 * a single gRPC Stream RPC call that stays open indefinitely, aligned with
 * Java's infinite {@code ResultStream}.
 */
export function createAxonServerEventStore(connection: AxonServerConnection, serializer: Serializer): EventStore {
  const { eventToProto, eventFromProto } = createEventConverters(serializer)

  function createAxonMetadata(): Metadata {
    const axonMetadata = new Metadata()
    axonMetadata.set("AxonIQ-Context", connection.config.context)
    if (connection.config.token) {
      axonMetadata.set("AxonIQ-Access-Token", connection.config.token)
    }
    return axonMetadata
  }

  // Push-based subscriber registry (EventBus.subscribe contract). Axon Server's
  // own distribution is the server-side Stream RPC (see open()); these in-process
  // subscribers are notified best-effort on every successful local append.
  const subscribers = new Set<(events: ReadonlyArray<EventMessage>) => Promise<void>>()
  async function notifySubscribers(events: ReadonlyArray<EventMessage>): Promise<void> {
    for (const sub of subscribers) {
      try {
        await sub(events)
      } catch {
        /* ignore subscriber errors */
      }
    }
  }

  return {
    async source(condition: SourcingCondition): Promise<SourcingResult> {
      const criterions = criteriaToCriterions(condition.criteria)

      const request = {
        fromSequence: condition.start ?? 0n,
        criterion: criterions,
      }

      const events: EventMessage[] = []
      let marker: ConsistencyMarker = noMarker()

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

      return { events, marker }
    },

    async appendEvents(
      newEvents: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<AppendTransaction> {
      const taggedEvents = newEvents.map(eventToProto)
      const request = {
        condition: condition ? {
          consistencyMarker: condition.marker.position,
          criterion: criteriaToCriterions(condition.criteria),
        } : undefined,
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
          const response = await connection.eventStore.append(requestStream(), { metadata: createAxonMetadata() })
          responseMarker = response.consistencyMarker
          await notifySubscribers(newEvents)
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
      const criterions = condition.criteria ? criteriaToCriterions(condition.criteria) : []

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

      return createMessageStream<SequencedEvent>({
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
      })
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

    // EventBus contract — publish = append without condition, then notify
    // in-process subscribers.
    async publish(events: ReadonlyArray<EventMessage>): Promise<void> {
      await this.append(events)
    },

    subscribe(handler: (events: ReadonlyArray<EventMessage>) => Promise<void>): () => void {
      subscribers.add(handler)
      return () => {
        subscribers.delete(handler)
      }
    },
  }
}
