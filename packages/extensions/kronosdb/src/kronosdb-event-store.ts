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
import { messageStream } from "@kronos-ts/messaging"
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
import type { KronosDbConnection } from "./connection.js"
import { kronosMetadata } from "./connection.js"
import { metadataFromStringMap, metadataToStringMap } from "./metadata-conversion.js"

// ---------------------------------------------------------------------------
// Tag conversion — framework Tag (string k/v) ↔ proto Tag (binary k/v)
// ---------------------------------------------------------------------------

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function tagToProto(tag: Tag): { key: Uint8Array; value: Uint8Array } {
  return {
    key: textEncoder.encode(tag.key),
    value: textEncoder.encode(tag.value),
  }
}

function tagFromProto(tag: { key: Uint8Array; value: Uint8Array }): Tag {
  return {
    key: textDecoder.decode(tag.key),
    value: textDecoder.decode(tag.value),
  }
}

// ---------------------------------------------------------------------------
// Criteria conversion — framework EventCriteria → proto Criterion[]
//
// KronosDB Criterion has: names (string[]) + tags (Tag[])
// Semantics: event matches if (names empty OR name in names) AND all tags present
// Multiple criteria are OR'd together.
// ---------------------------------------------------------------------------

function criteriaToCriterions(criteria: EventCriteria): any[] {
  switch (criteria.kind) {
    case "tags":
      return [{
        names: [],
        tags: criteria.tags.map(tagToProto),
      }]

    case "type-restricted": {
      const innerTags = criteria.inner.kind === "tags"
        ? criteria.inner.tags.map(tagToProto)
        : []
      return [{
        names: [...criteria.types],
        tags: innerTags,
      }]
    }

    case "either":
      return criteria.criteria.flatMap(criteriaToCriterions)

    case "any-tag":
      return [{ names: [], tags: [] }]
  }
}

// ---------------------------------------------------------------------------
// Event conversion — framework EventMessage ↔ proto Event/TaggedEvent
// ---------------------------------------------------------------------------

function createEventConverters(serializer: Serializer) {
  return {
    eventToProto(event: EventMessage): any {
      const name = qualifiedNameToString(event.name)
      const serialized = serializer.serialize(event.payload, name, event.version)
      return {
        event: {
          identifier: event.identifier,
          timestamp: BigInt(event.timestamp),
          name,
          version: event.version,
          payload: serialized.data,
          metadata: metadataToStringMap(event.metadata),
        },
        tags: event.tags.map(tagToProto),
      }
    },

    eventFromProto(protoEvent: any, tags?: any[]): EventMessage {
      const payload = protoEvent.payload && protoEvent.payload.length > 0
        ? serializer.deserialize({ data: protoEvent.payload, type: protoEvent.name, revision: protoEvent.version })
        : {}

      return {
        kind: "event",
        identifier: protoEvent.identifier,
        name: qualifiedNameFromString(protoEvent.name),
        version: protoEvent.version,
        payload,
        metadata: metadataFromStringMap(protoEvent.metadata ?? {}),
        timestamp: Number(protoEvent.timestamp),
        tags: (tags ?? []).map(tagFromProto),
      }
    },
  }
}

// ---------------------------------------------------------------------------
// KronosDB Event Store — implements EventStore interface via gRPC
// ---------------------------------------------------------------------------

/**
 * Creates an EventStore implementation backed by KronosDB's gRPC event store.
 *
 * Maps the framework's EventStore interface to KronosDB's EventStore service,
 * handling conversion between framework types and proto messages.
 *
 * Key differences from Axon Server:
 * - Event metadata is `map<string, string>` (not MetadataValue)
 * - Source returns `SequencedEvent` (no tags on read — only on append)
 * - Stream returns `SequencedEvent` directly
 * - Criterion uses flat `names` + `tags` (not TagsAndNamesCriterion wrapper)
 */
export function kronosDbEventStore(connection: KronosDbConnection, serializer: Serializer): EventStore {
  const { eventToProto, eventFromProto } = createEventConverters(serializer)

  function getMetadata() {
    return kronosMetadata(connection.config)
  }

  // Push-based subscriber registry (EventBus.subscribe contract). KronosDB's
  // own distribution is the server-side stream RPC (see open()); these
  // in-process subscribers are notified best-effort on every local append.
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

      // KronosDB requires at least one criterion for tag-index matching.
      // An empty criterion (no names, no tags) matches all events.
      const effectiveCriterions = criterions.length === 0
        ? [{ names: [], tags: [] }]
        : criterions

      const request = {
        fromSequence: condition.start ?? 0n,
        criteria: effectiveCriterions,
        // Events per response message; 0 lets the server pick its default.
        batchSize: 0,
      }

      const events: EventMessage[] = []
      let marker: ConsistencyMarker = noMarker()

      const stream = connection.eventStore.source(request, { metadata: getMetadata() })
      for await (const response of stream) {
        const batch = response.batch
        if (!batch) continue
        for (const seqEvent of batch.events) {
          if (seqEvent.event) {
            // KronosDB doesn't return tags on source — we need to fetch them
            // For now, pass empty tags; tags are only relevant for append conditions
            events.push(eventFromProto(seqEvent.event))
          }
        }
        // The final batch carries the marker; 0n is a valid marker for an
        // empty store, so presence — not truthiness — decides.
        if (batch.consistencyMarker !== undefined) {
          marker = markerAt(batch.consistencyMarker)
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
          criteria: criteriaToCriterions(condition.criteria),
        } : undefined,
        events: taggedEvents,
      }

      let responseMarker: bigint | undefined

      return {
        async commit() {
          const response = await connection.eventStore.append(request, { metadata: getMetadata() })
          responseMarker = response.consistencyMarker
          await notifySubscribers(newEvents)
        },
        async afterCommit() {
          return markerAt(responseMarker ?? 0n)
        },
        rollback() {
          // If commit() was never called, nothing was sent
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

      // KronosDB requires at least one criterion for tag-index matching.
      // An empty criterion (no names, no tags) matches all events.
      const effectiveCriterions = criterions.length === 0
        ? [{ names: [], tags: [] }]
        : criterions

      const PERMIT_BATCH = 500
      const REFILL_THRESHOLD = 0.25

      // Controllable async iterable for sending StreamControl messages.
      let sendControl: ((msg: any) => void) | null = null
      let controlDone = false
      const controlQueue: any[] = []
      let controlResolve: (() => void) | null = null

      async function* controlStream() {
        // First message: subscribe with initial permits.
        yield {
          subscribe: {
            fromSequence: condition.position,
            criteria: effectiveCriterions,
            initialPermits: BigInt(PERMIT_BATCH),
            blacklistedNames: [],
            // Events per response message; 0 lets the server pick. The server
            // never sends more unconsumed events than granted permits, so the
            // effective batch cap is min(server default, outstanding permits).
            batchSize: 0,
          },
        }

        // Subsequent messages: permit grants.
        while (!controlDone) {
          while (controlQueue.length > 0) {
            yield controlQueue.shift()!
          }
          // Wait for more messages to send.
          await new Promise<void>((resolve) => {
            controlResolve = resolve
          })
        }
      }

      function grantPermits(count: number) {
        controlQueue.push({
          permits: { permits: BigInt(count) },
        })
        controlResolve?.()
      }

      const grpcStream = connection.eventStore.stream(controlStream(), { metadata: getMetadata() })

      const buffer: SequencedEvent[] = []
      let availableCallback: (() => void) | null = null
      let completed = false
      let streamError: Error | undefined
      let reading = false
      let remainingPermits = PERMIT_BATCH

      async function startReading() {
        if (reading) return
        reading = true
        try {
          for await (const response of grpcStream) {
            if (completed) break
            // StreamResponse is a oneof { batch, heartbeat } since kronosdb v0.5.
            // For heartbeat frames, response.batch is undefined and this guard skips
            // them transparently — no explicit heartbeat branch needed (RESEARCH.md
            // KDB-03, Pitfall 3). Server emits one heartbeat every ~15 seconds.
            if (response.batch) {
              for (const seqEvent of response.batch.events) {
                if (seqEvent.event) {
                  buffer.push({
                    sequence: seqEvent.sequence,
                    event: eventFromProto(seqEvent.event),
                  })
                }
              }
              if (response.batch.events.length > 0) availableCallback?.()
            }
          }
          completed = true
          availableCallback?.()
        } catch (err) {
          streamError = err instanceof Error ? err : new Error(String(err))
          completed = true
          availableCallback?.()
        }
      }

      startReading()

      function onConsumed() {
        remainingPermits--
        const threshold = Math.floor(PERMIT_BATCH * REFILL_THRESHOLD)
        if (remainingPermits <= threshold && !completed) {
          const grant = PERMIT_BATCH - remainingPermits
          remainingPermits += grant
          grantPermits(grant)
        }
      }

      return messageStream<SequencedEvent>({
        next() {
          const item = buffer.shift()
          if (item) onConsumed()
          return item
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
          controlDone = true
          controlResolve?.()
          availableCallback = null
        },
      })
    },

    async getHeadPosition(): Promise<bigint> {
      const response = await connection.eventStore.getHead({}, { metadata: getMetadata() })
      return response.sequence
    },

    async firstToken(): Promise<TrackingToken> {
      return FIRST_TOKEN
    },

    async latestToken(): Promise<TrackingToken> {
      const response = await connection.eventStore.getHead({}, { metadata: getMetadata() })
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
