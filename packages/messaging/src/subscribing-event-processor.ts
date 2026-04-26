import { emptyMetadata, qualifiedNameToString, type Metadata } from "@kronos-ts/common"
import type { EventMessage } from "./message.js"
import type { EventHandlerRegistration, EventHandlerContext } from "./handler.js"
import type { EventHandlersDefinition } from "./event-handler.js"
import type { UoWRunner } from "./unit-of-work.js"
import { runInNewUoW } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import { loggingErrorHandler } from "./tracking-event-processor.js"
import type { SubscribableEventSource } from "./event-bus.js"

// Re-export for backward compatibility
export type { SubscribableEventSource } from "./event-bus.js"

/**
 * A push-based event processor that subscribes directly to an event source.
 *
 * Unlike tracking/streaming processors, the subscribing processor:
 * - Does **not** use a token store (no position tracking)
 * - Does **not** support replay or reset
 * - Processes events synchronously with the publisher
 * - Is suitable for in-memory projections that don't need persistence
 *
 * Aligned with Kronos Framework's `SubscribingEventProcessor`.
 */
export interface SubscribingEventProcessor {
  readonly name: string
  readonly running: boolean
  start(): void
  stop(): void
  /** Always returns false — subscribing processors don't support reset. */
  supportsReset(): boolean
}

export interface SubscribingEventProcessorOptions {
  name: string
  eventSource: SubscribableEventSource
  handlerGroups: ReadonlyArray<EventHandlersDefinition>
  contextFactory: (metadata: Metadata) => EventHandlerContext
  unitOfWorkRunner?: UoWRunner
  errorHandler?: EventProcessingErrorHandler
}

/**
 * Creates a subscribing event processor.
 *
 * The processor subscribes to the event source and processes events
 * within a UnitOfWork as they arrive. Events are delivered on the
 * publisher's call stack (synchronous with append).
 */
export function createSubscribingEventProcessor(
  options: SubscribingEventProcessorOptions,
): SubscribingEventProcessor {
  const {
    name,
    eventSource,
    handlerGroups,
    contextFactory,
    unitOfWorkRunner = runInNewUoW,
    errorHandler = loggingErrorHandler(name),
  } = options

  // Build handler lookup: eventName → handler[]
  const handlerMap = new Map<string, EventHandlerRegistration<any>[]>()
  for (const group of handlerGroups) {
    for (const reg of group.handlers) {
      const eventName = qualifiedNameToString(reg.descriptor.name)
      const existing = handlerMap.get(eventName)
      if (existing) {
        existing.push(reg)
      } else {
        handlerMap.set(eventName, [reg])
      }
    }
  }

  let isRunning = false
  let unsubscribe: (() => void) | null = null

  async function handleEvents(events: ReadonlyArray<EventMessage>) {
    if (!isRunning || events.length === 0) return

    await unitOfWorkRunner(emptyMetadata(), async () => {
      for (const event of events) {
        await deliverEvent(event)
      }
    })
  }

  async function deliverEvent(event: EventMessage) {
    const eventName = qualifiedNameToString(event.name)
    const handlers = handlerMap.get(eventName)
    if (!handlers || handlers.length === 0) return

    const handlerContext = contextFactory(event.metadata)
    for (const reg of handlers) {
      try {
        await reg.handler(event.payload, handlerContext)
      } catch (err) {
        // SubscribingEventProcessor doesn't have position tracking,
        // so pass -1n as position indicator
        await errorHandler.handleError(err, eventName, -1n)
      }
    }
  }

  return {
    get name() { return name },
    get running() { return isRunning },

    start() {
      if (isRunning) return
      isRunning = true
      unsubscribe = eventSource.subscribe(handleEvents)
    },

    stop() {
      isRunning = false
      if (unsubscribe) {
        unsubscribe()
        unsubscribe = null
      }
    },

    supportsReset() {
      return false
    },
  }
}
