import { emptyMetadata, qualifiedNameToString, type Metadata } from "@kronos-ts/common"
import type { EventHandlerRegistration, EventHandlerContext } from "./handler.js"
import type { EventHandlersDefinition } from "./event-handler.js"
import type { StreamableEventSource, MessageStream, SequencedEvent } from "./event-source.js"
import type { UoWRunner } from "./unit-of-work.js"
import { runInNewUoW } from "./unit-of-work.js"
import type { TokenStore } from "./token-store.js"
import type { TrackingToken } from "./tracking-token.js"
import {
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isReplaying,
  advanceToken,
} from "./tracking-token.js"
import { REPLAY_STATE_KEY } from "./replay-token.js"
import { setResource, onPrepareCommit } from "./processing-state.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"

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
}

export interface TrackingEventProcessorOptions {
  name: string
  eventSource: StreamableEventSource
  handlerGroups: ReadonlyArray<EventHandlersDefinition>
  contextFactory: (metadata: Metadata) => EventHandlerContext
  unitOfWorkRunner?: UoWRunner
  tokenStore?: TokenStore
  /** Polling interval when no events are available (ms). Default: 500. */
  pollingIntervalMs?: number
  batchSize?: number
  errorHandler?: EventProcessingErrorHandler
  /** Optional handler enhancer applied to all event handlers at setup time. */
  handlerEnhancer?: HandlerEnhancerDefinition
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
    handlerGroups,
    contextFactory,
    unitOfWorkRunner = runInNewUoW,
    tokenStore,
    pollingIntervalMs = 500,
    batchSize = 100,
    errorHandler = loggingErrorHandler(name),
    handlerEnhancer,
  } = options

  const segment = 0

  const handlerMap = new Map<string, Array<EventHandlerRegistration<any>>>()
  for (const group of handlerGroups) {
    for (const reg of group.handlers) {
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
              handlerGroup: group.name,
            }),
          }
        : reg
      handlerMap.get(eventName)!.push(enhanced)
    }
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

    const handlerContext = contextFactory(event.metadata)
    for (const reg of handlers) {
      try {
        await reg.handler(event.payload, handlerContext)
      } catch (err) {
        await errorHandler.handleError(err, eventName, sequencedEvent.sequence)
      }
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
    },

    stop() {
      isRunning = false
      if (pollTimer !== null) {
        clearTimeout(pollTimer)
        pollTimer = null
      }
      if (stream) {
        stream.close()
        stream = null
      }
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

      for (const group of handlerGroups) {
        if (group.onReset) {
          await group.onReset()
        }
      }
    },
  }
}
