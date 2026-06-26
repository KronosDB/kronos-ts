import { emptyMetadata, qualifiedNameToString } from "@kronos-ts/common"
import type { EventHandlerRegistration } from "./handler.js"
import type { EventHandlerDefinition } from "./event-handler.js"
import type { StreamableEventSource, MessageStream, SequencedEvent } from "./event-source.js"
import type { UoWRunner } from "./unit-of-work.js"
import { runInNewUoW } from "./unit-of-work.js"
import type { TokenStore } from "./token-store.js"
import type { SequencedDeadLetterQueue, EnqueuePolicy, DeadLetter } from "./dead-letter-queue.js"
import type { SequencingPolicy } from "./sequencing-policy.js"
import { createDeadLetteringDelivery } from "./dead-lettering-handler.js"
import { type DeadLetterListener, noOpDeadLetterListener } from "./dead-letter-listener.js"
import {
  type DeadLetterReprocessor,
  createDeadLetterReprocessor,
} from "./dead-letter-reprocessor.js"
import type { TrackingToken } from "./tracking-token.js"
import {
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isReplaying,
  advanceToken,
} from "./tracking-token.js"
import { REPLAY_STATE_KEY } from "./replay-token.js"
import { applyCorrelationData, type CorrelationDataProvider } from "./correlation-data.js"
import { setResource, onPrepareCommit } from "./processing-state.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import { STATE_MANAGER_KEY, EVENT_SCHEDULER_KEY } from "@kronos-ts/eventsourcing"
import type { EventScheduler } from "./event-scheduler.js"
import { COMMAND_BUS_KEY } from "./send.js"
import { QUERY_BUS_KEY } from "./emit-update.js"

/**
 * A tracking event processor that reads events from a streamable event source
 * and delivers them to registered event handlers.
 *
 * Uses {@link StreamableEventSource.open} to get a persistent {@link MessageStream}.
 * Each batch of events is processed within a UnitOfWork, enabling
 * transactional event processing and coordinated token updates.
 *
 * Supports replay via {@link resetTokens} — the processor can be stopped,
 * reset to a starting position, and restarted.
 */
export interface TrackingEventProcessor {
  readonly name: string
  readonly running: boolean
  /** Current effective position in the event stream. */
  readonly position: bigint
  /** Whether the processor is currently replaying events. */
  readonly replaying: boolean
  start(): Promise<void>
  stop(): void
  /**
   * Reset the processor to replay events from a starting position.
   * The processor must be stopped before calling this.
   */
  resetTokens(startPosition?: bigint, resetContext?: unknown): Promise<void>
  /**
   * Replay parked dead letters back through the handlers (the oldest matching
   * sequence). No-op returning false when no DLQ is configured. Safe to call
   * whether or not the processor is running.
   */
  reprocessDeadLetters(filter?: (sequenceId: string) => boolean): Promise<boolean>
}

export interface TrackingEventProcessorOptions {
  name: string
  eventSource: StreamableEventSource
  eventHandlers: ReadonlyArray<EventHandlerDefinition>
  /** State manager injected into ALS at handler-invocation entry (D-44). */
  stateManager?: unknown
  /** Command bus injected into ALS at handler-invocation entry (D-44). */
  commandBus?: CommandBus
  /** Query bus injected into ALS at handler-invocation entry (D-44). */
  queryBus?: QueryBus
  /** Event scheduler injected into ALS at handler-invocation entry (read by schedule()). */
  eventScheduler?: EventScheduler
  /**
   * Correlation data providers run against each event before its handlers are
   * invoked, so commands/events dispatched from an event handler inherit the
   * triggering event's correlationId/causationId.
   */
  correlationDataProviders?: ReadonlyArray<CorrelationDataProvider>
  /** Optional per-event callback fired inside the UoW before handler invocation (e.g. monitoring). */
  onEventDelivery?: () => void
  unitOfWorkRunner?: UoWRunner
  tokenStore?: TokenStore
  /**
   * Dead letter queue for poison-pill handling. When set, a handler failure
   * parks the event in the DLQ (per {@link enqueuePolicy}) and the batch
   * commits so the token advances past it — instead of redelivering the batch
   * forever. The enqueue runs inside the batch UnitOfWork, so it commits in the
   * same transaction as the token update.
   */
  deadLetterQueue?: SequencedDeadLetterQueue
  /** Decides whether a failed event is enqueued. Default: always enqueue. */
  enqueuePolicy?: EnqueuePolicy
  /** Decides each event's ordered sequence for the DLQ. Default: first tag value. */
  sequencingPolicy?: SequencingPolicy
  /** Observability hook for dead-letter lifecycle events. Default: no-op. */
  deadLetterListener?: DeadLetterListener
  /** When true, resetTokens() also clears this processor's DLQ (Axon allowReset). Default: false. */
  resetClearsDeadLetters?: boolean
  /** When set, automatically drains the DLQ on this interval (ms). Off by default. */
  dlqRetryIntervalMs?: number
  /** Polling interval when no events are available (ms). Default: 500. */
  pollingIntervalMs?: number
  batchSize?: number
  errorHandler?: EventProcessingErrorHandler
  /** Optional handler enhancer applied to all event handlers at setup time. */
  handlerEnhancer?: HandlerEnhancerDefinition
  /** Reset callback invoked from resetTokens(). */
  onReset?: () => Promise<void> | void
}

/**
 * Determines what happens when an event handler fails.
 */
export interface EventProcessingErrorHandler {
  handleError(error: unknown, eventName: string, position: bigint): void | Promise<void>
}

/**
 * Logs errors and continues processing. Default behavior.
 */
export function loggingErrorHandler(processorName: string): EventProcessingErrorHandler {
  return {
    handleError(error, eventName, position) {
      console.error(
        `Event processor "${processorName}": handler failed for "${eventName}" at position ${position}:`,
        error,
      )
    },
  }
}

/**
 * Rethrows errors, aborting the current batch and triggering rollback.
 */
export function propagatingErrorHandler(): EventProcessingErrorHandler {
  return {
    handleError(error) {
      throw error
    },
  }
}

export function createTrackingEventProcessor(
  options: TrackingEventProcessorOptions,
): TrackingEventProcessor {
  const {
    name,
    eventSource,
    eventHandlers,
    stateManager,
    commandBus,
    queryBus,
    eventScheduler,
    correlationDataProviders,
    onEventDelivery,
    unitOfWorkRunner = runInNewUoW,
    tokenStore,
    deadLetterQueue,
    enqueuePolicy,
    sequencingPolicy,
    deadLetterListener = noOpDeadLetterListener(),
    resetClearsDeadLetters = false,
    dlqRetryIntervalMs,
    pollingIntervalMs = 500,
    batchSize = 1,
    errorHandler = loggingErrorHandler(name),
    handlerEnhancer,
    onReset,
  } = options

  const segment = 0

  // Option A: when a DLQ is configured, handler failures are caught and parked
  // (not propagated), so the batch commits and the token advances past the
  // poison pill. Built once; invoked inside the batch UnitOfWork by deliverEvent.
  const deadLetterDelivery = deadLetterQueue
    ? createDeadLetteringDelivery({
        queue: deadLetterQueue,
        policy: enqueuePolicy,
        sequencingPolicy,
        listener: deadLetterListener,
      })
    : undefined

  // Reprocessor: replays a parked letter through the same handlers, with the
  // same ALS resources as live delivery, so dependencies resolve identically.
  const reprocessor: DeadLetterReprocessor | undefined = deadLetterQueue
    ? createDeadLetterReprocessor({
        queue: deadLetterQueue,
        policy: enqueuePolicy,
        unitOfWorkRunner,
        listener: deadLetterListener,
        replay: replayDeadLetter,
      })
    : undefined
  let dlqRetryTimer: ReturnType<typeof setInterval> | null = null

  const handlerMap = new Map<string, Array<EventHandlerRegistration<any>>>()
  for (const reg of eventHandlers) {
    const eventName = qualifiedNameToString(reg.descriptor.name)
    if (!handlerMap.has(eventName)) {
      handlerMap.set(eventName, [])
    }
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
    handlerMap.get(eventName)!.push(enhanced as EventHandlerRegistration<any>)
  }

  let token: TrackingToken = globalSequenceToken(0n)
  let isRunning = false
  let stream: MessageStream<SequencedEvent> | null = null
  let pollTimer: ReturnType<typeof setTimeout> | null = null
  let processing = false

  async function initialize() {
    if (tokenStore) {
      await tokenStore.initializeSegments(name, 1)
      const stored = await tokenStore.get(name, segment)
      if (stored !== undefined) {
        token = stored
      }
    }
  }

  function openStream() {
    stream = eventSource.open({ position: token.position() })
    stream.setCallback(() => {
      if (isRunning && !processing) {
        scheduleImmediate()
      }
    })
  }

  async function poll() {
    if (!isRunning || processing) return
    processing = true

    try {
      if (!stream) {
        openStream()
      }

      // Check for stream errors — reopen if needed
      if (stream!.error()) {
        console.error(`Event processor "${name}": stream error, reopening:`, stream!.error())
        stream!.close()
        stream = null
        openStream()
        processing = false
        return
      }

      const batch: SequencedEvent[] = []
      let event = stream!.next()
      while (event && batch.length < batchSize) {
        batch.push(event)
        if (batch.length < batchSize && stream!.hasNextAvailable()) {
          event = stream!.next()
        } else {
          break
        }
      }

      if (batch.length > 0) {
        await processBatch(batch)
        if (isRunning) {
          if (stream!.hasNextAvailable()) {
            scheduleImmediate()
          }
          // else: stream callback will wake us when events arrive
        }
      } else {
        // If replay is done and no more events, unwrap
        if (isReplayToken(token)) {
          token = globalSequenceToken(token.position())
          if (tokenStore) {
            await tokenStore.store(name, segment, token)
          }
        }
        // No events available — wait for callback or poll again after interval
        if (isRunning) {
          pollTimer = setTimeout(poll, pollingIntervalMs)
        }
      }
    } catch (err) {
      console.error(`Event processor "${name}" error during poll:`, err)
      // Realign the live stream to the committed checkpoint. During batch
      // accumulation the stream cursor (and any read-ahead buffer) advanced
      // past this batch, but `token` was NOT advanced — the failing UnitOfWork
      // never reached PREPARE_COMMIT. Closing discards the stream's buffer so
      // the next poll reopens at token.position() and re-reads — and thus
      // redelivers — the failed batch. Without this the stream cursor outruns
      // the checkpoint and the failed events are skipped until a restart.
      // Mirrors Axon's close-and-reopen-from-token recovery.
      stream?.close()
      stream = null
      if (isRunning) pollTimer = setTimeout(poll, pollingIntervalMs * 2)
    } finally {
      processing = false
    }
  }

  async function processBatch(batch: SequencedEvent[]) {
    let batchEndToken: TrackingToken = token

    await unitOfWorkRunner(emptyMetadata(), async () => {
      for (const sequencedEvent of batch) {
        setResource(REPLAY_STATE_KEY, { replaying: isReplaying(batchEndToken) })

        await deliverEvent(sequencedEvent)

        batchEndToken = advanceToken(batchEndToken, sequencedEvent.sequence + 1n)
      }

      if (tokenStore) {
        onPrepareCommit(async () => {
          await tokenStore.store(name, segment, batchEndToken)
          await tokenStore.extendClaim(name, segment, name)
        })
      }
    })

    token = batchEndToken
  }

  async function deliverEvent(sequencedEvent: SequencedEvent) {
    const event = sequencedEvent.event
    const eventName = qualifiedNameToString(event.name)
    const handlers = handlerMap.get(eventName)
    if (!handlers || handlers.length === 0) return

    // D-44 wiring: write framework components into ALS at per-event invocation entry.
    if (stateManager !== undefined) setResource(STATE_MANAGER_KEY, stateManager as any)
    if (commandBus !== undefined) setResource(COMMAND_BUS_KEY, commandBus)
    if (queryBus !== undefined) setResource(QUERY_BUS_KEY, queryBus)
    if (eventScheduler !== undefined) setResource(EVENT_SCHEDULER_KEY, eventScheduler)
    // Seed correlation data from the triggering event so an automation's
    // outgoing commands/events inherit its lineage.
    if (correlationDataProviders && correlationDataProviders.length > 0) {
      applyCorrelationData(event, correlationDataProviders)
    }
    // Optional per-event callback (e.g. monitoring hooks registered inside the UoW).
    if (onEventDelivery) onEventDelivery()

    // DLQ path: park failures, keep the batch committable (Option A). The DLQ
    // delivery enforces per-sequence ordering and never propagates, so the
    // errorHandler / batch-redelivery path is bypassed while a DLQ is active.
    if (deadLetterDelivery) {
      await deadLetterDelivery.deliver(sequencedEvent, handlers)
      return
    }

    for (const reg of handlers) {
      try {
        await reg.handler({ ...event, sequence: sequencedEvent.sequence })
      } catch (err) {
        await errorHandler.handleError(err, eventName, sequencedEvent.sequence)
      }
    }
  }

  // Replay a parked dead letter through the handlers, re-establishing the same
  // ALS resources as live delivery. Throws on the first handler failure so the
  // reprocessor can requeue the letter (delivery is at-least-once → handlers
  // must be idempotent).
  async function replayDeadLetter(letter: DeadLetter): Promise<void> {
    const event = letter.message
    const eventName = qualifiedNameToString(event.name)
    const handlers = handlerMap.get(eventName)
    if (!handlers || handlers.length === 0) return

    if (stateManager !== undefined) setResource(STATE_MANAGER_KEY, stateManager as any)
    if (commandBus !== undefined) setResource(COMMAND_BUS_KEY, commandBus)
    if (queryBus !== undefined) setResource(QUERY_BUS_KEY, queryBus)
    if (eventScheduler !== undefined) setResource(EVENT_SCHEDULER_KEY, eventScheduler)
    if (correlationDataProviders && correlationDataProviders.length > 0) {
      applyCorrelationData(event, correlationDataProviders)
    }

    const position =
      typeof letter.diagnostics.position === "number" ? BigInt(letter.diagnostics.position) : 0n
    for (const reg of handlers) {
      await reg.handler({ ...event, sequence: position })
    }
  }

  function scheduleImmediate() {
    if (pollTimer !== null) {
      clearTimeout(pollTimer)
    }
    pollTimer = setTimeout(poll, 0)
  }

  return {
    get name() { return name },
    get running() { return isRunning },
    get position() { return token.position() },
    get replaying() { return isReplaying(token) },

    async start() {
      if (isRunning) return
      await initialize()
      isRunning = true
      poll()
      // Optional scheduled DLQ drain — operators can also trigger reprocessing
      // manually via reprocessDeadLetters().
      if (reprocessor && dlqRetryIntervalMs && dlqRetryIntervalMs > 0) {
        dlqRetryTimer = setInterval(() => {
          void reprocessor.reprocessAll().catch((err) => {
            console.error(`Event processor "${name}": scheduled DLQ drain failed:`, err)
          })
        }, dlqRetryIntervalMs)
      }
    },

    stop() {
      isRunning = false
      if (pollTimer !== null) {
        clearTimeout(pollTimer)
        pollTimer = null
      }
      if (dlqRetryTimer !== null) {
        clearInterval(dlqRetryTimer)
        dlqRetryTimer = null
      }
      if (stream) {
        stream.close()
        stream = null
      }
    },

    async reprocessDeadLetters(filter?: (sequenceId: string) => boolean) {
      if (!reprocessor) return false
      return reprocessor.reprocess(filter)
    },

    async resetTokens(startPosition: bigint = 0n, resetContext?: unknown) {
      if (isRunning) {
        throw new Error(`Processor "${name}" must be stopped before resetting tokens`)
      }

      const headPosition = await eventSource.getHeadPosition()

      if (headPosition <= startPosition) {
        token = globalSequenceToken(startPosition)
      } else {
        token = replayToken(
          globalSequenceToken(headPosition),
          globalSequenceToken(startPosition),
          resetContext,
        )
      }

      if (tokenStore) {
        await tokenStore.store(name, segment, token)
      }

      // Axon allowReset: a replay re-derives view state from scratch, so stale
      // dead letters from the prior run are meaningless. Only clear when opted
      // in — otherwise parked letters survive a reset for manual handling.
      if (resetClearsDeadLetters && deadLetterQueue) {
        await deadLetterQueue.clear()
      }

      if (onReset) {
        await onReset()
      }
    },
  }
}
