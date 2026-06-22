import { emptyMetadata, qualifiedNameToString } from "@kronos-ts/common"
import type { EventMessage } from "./message.js"
import type { EventHandlerRegistration } from "./handler.js"
import type { EventHandlerDefinition } from "./event-handler.js"
import type { UoWRunner } from "./unit-of-work.js"
import { runInNewUoW } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import { loggingErrorHandler } from "./tracking-event-processor.js"
import type { SubscribableEventSource } from "./event-bus.js"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { setResource } from "./processing-state.js"
import { STATE_MANAGER_KEY, EVENT_SCHEDULER_KEY } from "@kronos-ts/eventsourcing"
import type { EventScheduler } from "./event-scheduler.js"
import { COMMAND_BUS_KEY } from "./send.js"
import { QUERY_BUS_KEY } from "./emit-update.js"

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
 * It also does **not** support a dead-letter queue — deliberately. A DLQ exists
 * to let a processor advance its token past a poison pill and reprocess later;
 * a subscribing processor has no token and runs in the publisher's call stack,
 * so a failure must surface to the publisher (via the error handler), not be
 * silently parked. Use a tracking/streaming processor with `.deadLetterQueue()`
 * when you need dead-lettering.
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
  eventHandlers: ReadonlyArray<EventHandlerDefinition>
  /** State manager injected into ALS at handler-invocation entry (D-44). */
  stateManager?: unknown
  /** Command bus injected into ALS at handler-invocation entry (D-44). */
  commandBus?: CommandBus
  /** Query bus injected into ALS at handler-invocation entry (D-44). */
  queryBus?: QueryBus
  /** Event scheduler injected into ALS at handler-invocation entry (read by schedule()). */
  eventScheduler?: EventScheduler
  /** Optional per-event callback fired inside the UoW before handler invocation (e.g. monitoring). */
  onEventDelivery?: () => void
  unitOfWorkRunner?: UoWRunner
  errorHandler?: EventProcessingErrorHandler
  /**
   * Plan 09-01: optional handler enhancer applied to each event handler at
   * registration time. Symmetric to TrackingEventProcessor.handlerEnhancer.
   * When set, each registered handler is wrapped via wrapHandler with
   * messageType "event" and the group name as handlerGroup.
   */
  handlerEnhancer?: HandlerEnhancerDefinition
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
    eventHandlers,
    stateManager,
    commandBus,
    queryBus,
    eventScheduler,
    onEventDelivery,
    unitOfWorkRunner = runInNewUoW,
    errorHandler = loggingErrorHandler(name),
    handlerEnhancer,
  } = options

  // Build handler lookup: eventName → handler[]
  // Plan 09-01: when a handlerEnhancer is supplied, wrap each handler at
  // registration time symmetric to TrackingEventProcessor. Plan 11-02:
  // handlerGroup is now the processor name (no separate group identity).
  const handlerMap = new Map<string, EventHandlerRegistration<any>[]>()
  for (const reg of eventHandlers) {
    const eventName = qualifiedNameToString(reg.descriptor.name)
    const enhanced = handlerEnhancer
      ? {
          ...reg,
          handler: handlerEnhancer.wrapHandler(reg.handler, {
            messageType: "event" as const,
            messageName: eventName,
            handlerGroup: name,
          }),
        }
      : reg
    const existing = handlerMap.get(eventName)
    if (existing) {
      existing.push(enhanced as EventHandlerRegistration<any>)
    } else {
      handlerMap.set(eventName, [enhanced as EventHandlerRegistration<any>])
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

    // D-44 wiring: write framework components into ALS at per-event invocation entry.
    if (stateManager !== undefined) setResource(STATE_MANAGER_KEY, stateManager as any)
    if (commandBus !== undefined) setResource(COMMAND_BUS_KEY, commandBus)
    if (queryBus !== undefined) setResource(QUERY_BUS_KEY, queryBus)
    if (eventScheduler !== undefined) setResource(EVENT_SCHEDULER_KEY, eventScheduler)
    // Optional per-event callback (e.g. monitoring hooks registered inside the UoW).
    if (onEventDelivery) onEventDelivery()

    for (const reg of handlers) {
      try {
        await reg.handler(event)
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
