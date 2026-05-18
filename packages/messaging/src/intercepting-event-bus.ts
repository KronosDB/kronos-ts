import type { EventBus } from "./event-bus.js"
import type { EventMessage } from "./message.js"
import type { DispatchInterceptor } from "./interceptor.js"

/**
 * Wraps an EventBus with dispatch interceptors for event publishing.
 */
export function createInterceptingEventBus(
  delegate: EventBus,
  dispatchInterceptors: ReadonlyArray<DispatchInterceptor<EventMessage>>,
): EventBus {
  return {
    async publish(events) {
      const intercepted: EventMessage[] = []
      for (const event of events) {
        let msg = event
        for (const interceptor of dispatchInterceptors) {
          msg = await interceptor(msg) as EventMessage
        }
        intercepted.push(msg)
      }
      await delegate.publish(intercepted)
    },

    subscribe(handler) {
      return delegate.subscribe(handler)
    },
  }
}
