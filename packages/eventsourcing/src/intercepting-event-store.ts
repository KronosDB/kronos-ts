import type { EventMessage, DispatchInterceptor, ProcessingContext, StreamingCondition, MessageStream, SequencedEvent } from "@kronos-ts/messaging"
import type { EventStore } from "./event-store.js"
import type { AppendTransaction } from "./event-storage-engine.js"
import type { AppendCondition } from "./append-condition.js"
import type { ConsistencyMarker } from "./consistency-marker.js"
import type { SourcingCondition } from "./sourcing-condition.js"
import type { SourcingResult } from "./event-store.js"

/**
 * An {@link EventStore} decorator that applies dispatch interceptors
 * before events are appended or published.
 *
 * Read operations (source, open, getHeadPosition) pass through to
 * the delegate without interception.
 */
export function createInterceptingEventStore(
  delegate: EventStore,
  dispatchInterceptors: ReadonlyArray<DispatchInterceptor<EventMessage>>,
): EventStore {
  async function interceptEvents(
    events: ReadonlyArray<EventMessage>,
    context?: ProcessingContext,
  ): Promise<EventMessage[]> {
    const intercepted: EventMessage[] = []
    for (const event of events) {
      let msg = event
      for (const interceptor of dispatchInterceptors) {
        msg = await interceptor(msg, context) as EventMessage
      }
      intercepted.push(msg)
    }
    return intercepted
  }

  return {
    // Read operations pass through
    source(condition: SourcingCondition): Promise<SourcingResult> {
      return delegate.source(condition)
    },

    // AppendEvents intercepts events before storage
    async appendEvents(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<AppendTransaction> {
      const intercepted = await interceptEvents(events)
      return delegate.appendEvents(intercepted, condition)
    },

    async append(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<ConsistencyMarker> {
      const intercepted = await interceptEvents(events)
      return delegate.append(intercepted, condition)
    },

    // Publish intercepts events before distribution
    async publish(events: ReadonlyArray<EventMessage>): Promise<void> {
      const intercepted = await interceptEvents(events)
      return delegate.publish(intercepted)
    },

    // Stream/read pass through
    open: (condition: StreamingCondition) => delegate.open(condition),
    getHeadPosition: () => delegate.getHeadPosition(),
    firstToken: () => delegate.firstToken(),
    latestToken: () => delegate.latestToken(),
    subscribe: (handler) => delegate.subscribe(handler),
  }
}
