import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { EventMessage } from "../messages/message.js"
import type {
  EventProcessor,
  EventProcessorStatus,
  RunningProcessor,
} from "./event-processor.js"
import type { EventHandlerDefinition } from "../handlers/event-handler.js"
import type { MessageStream, SequencedEvent } from "./event-source.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { DeadLetter } from "../stores/dead-letter-queue.js"
import { deadLetteringDelivery } from "./dead-lettering.js"
import { type DeadLetterReprocessor, deadLetterReprocessor } from "./dead-letter-reprocessor.js"
import type { TrackingToken } from "./tracking-token.js"
import {
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isReplaying,
  advanceToken,
  advanceTokenTo,
} from "./tracking-token.js"
import { lineage } from "../buses/intercepting-bus.js"
import type { CommandBus } from "../buses/command-bus.js"
import type { QueryBus } from "../buses/query-bus.js"
import type { StateManagerLike } from "../state/load.js"
import type { EventScheduler } from "./event-scheduler.js"
import { eventHandlerContext, type EventHandlerContext } from "../handlers/handler-context.js"

/**
 * One event handler as the processor sees it: the definition, plus the buses
 * ITS context reaches. The buses ride on the entry rather than on the
 * processor because which bus a handler dispatches through is a property of
 * the handler's deployment, not of the cursor it shares with its neighbours.
 */
export interface ProcessorHandlerEntry {
  readonly definition: EventHandlerDefinition<any, any>
  readonly commandBus?: CommandBus
  readonly queryBus?: QueryBus
  /**
   * Backs THIS handler's `ctx.schedule`. Per entry for the same reason the buses
   * are: which scheduler an automation arms is a property of its deployment, not
   * of the cursor it shares with its neighbours.
   */
  readonly eventScheduler?: EventScheduler
}

/**
 * The lineage rule as CORRELATION DATA — the same transform `lineage` applies
 * to a message crossing a bus, read off the result so it can be merged onto
 * what a handler emits instead of onto the event itself.
 */
function lineageOf(event: EventMessage): Record<string, string> {
  const stamped = lineage(event)
  return {
    correlationId: String(stamped.metadata.correlationId),
    causationId: String(stamped.metadata.causationId),
  }
}

export interface RunEventProcessorOptions {
  readonly processor: EventProcessor
  readonly handlers: ReadonlyArray<ProcessorHandlerEntry>
  /** Backs `ctx.load` — the state manager for this processor's event store. */
  readonly stateManager?: StateManagerLike
  /** Backs `ctx.schedule` / `ctx.scheduleAfter` / `ctx.cancelSchedule`. */
  readonly eventScheduler?: EventScheduler
  /** Polling interval when no events are available (ms). Default: 500. */
  readonly pollingIntervalMs?: number
}

/**
 * Start one delivery: read the log from the committed token, hand each event to
 * the handlers registered for it, and commit the new token in the same unit of
 * work the handlers wrote in.
 *
 * Internal to assembly — a host reaches the result as `app.processors`.
 *
 * Failure semantics are the two the surface names, and nothing in between:
 * - No dead-letter queue → the error PROPAGATES. The batch rolls back, the
 *   token does not advance, the stream is realigned to the checkpoint and the
 *   batch is redelivered with backoff. A read model never silently skips.
 * - Dead-letter queue → the failed event and everything behind it in its lane
 *   are parked, the batch commits, and the token advances past the pill.
 */
export function runEventProcessor(options: RunEventProcessorOptions): RunningProcessor {
  const { processor, handlers, stateManager, eventScheduler } = options
  const { name, eventStore, tokenStore, unitOfWork: newUoW, deadLetterQueue, sequence } = processor
  const batchSize = processor.batchSize ?? 1
  const pollingIntervalMs = options.pollingIntervalMs ?? 500

  const segment = 0

  // With a queue configured, handler failures are caught and parked (not
  // propagated), so the batch commits and the token advances past the poison
  // pill. `sequence` is guaranteed present alongside a queue — `eventProcessor`
  // rejects the other combination at construction.
  const deadLetterDelivery =
    deadLetterQueue && sequence
      ? deadLetteringDelivery({ queue: deadLetterQueue, processingGroup: name, sequence })
      : undefined

  // Replays a parked letter through the same handlers, with a context built
  // from the reprocess unit of work exactly as live delivery does.
  const reprocessor: DeadLetterReprocessor | undefined = deadLetterQueue
    ? deadLetterReprocessor({
        queue: deadLetterQueue,
        processingGroup: name,
        unitOfWork: newUoW,
        replay: replayDeadLetter,
      })
    : undefined

  const handlerMap = new Map<string, ProcessorHandlerEntry[]>()
  for (const entry of handlers) {
    const eventName = qualifiedNameToString(entry.definition.descriptor.name)
    const bucket = handlerMap.get(eventName)
    if (bucket) bucket.push(entry)
    else handlerMap.set(eventName, [entry])
  }

  let token: TrackingToken = globalSequenceToken(0n)
  let isRunning = false
  let stream: MessageStream<SequencedEvent> | null = null
  let pollTimer: ReturnType<typeof setTimeout> | null = null
  let processing = false
  let caughtUp = false
  let lastError: Error | undefined

  async function initialize() {
    await tokenStore.initializeSegments(name, 1)
    const stored = await tokenStore.get(name, segment)
    if (stored !== undefined) token = stored
  }

  function openStream() {
    stream = eventStore.open({ position: token.position(), token })
    stream.setCallback(() => {
      if (isRunning && !processing) scheduleImmediate()
    })
  }

  async function poll() {
    if (!isRunning || processing) return
    processing = true

    try {
      if (!stream) openStream()

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
        if (batch.length < batchSize && stream!.hasNextAvailable()) event = stream!.next()
        else break
      }

      if (batch.length > 0) {
        caughtUp = false
        await processBatch(batch)
        // A clean batch clears any prior error — the processor has recovered.
        lastError = undefined
        if (isRunning) {
          if (stream!.hasNextAvailable()) scheduleImmediate()
          // Drained everything currently available — caught up until the
          // stream callback wakes us with new events.
          else caughtUp = true
        }
      } else {
        caughtUp = true
        // If replay is done and no more events, unwrap
        if (isReplayToken(token)) {
          token = globalSequenceToken(token.position())
          await tokenStore.store(name, segment, token)
        }
        if (isRunning) pollTimer = setTimeout(poll, pollingIntervalMs)
      }
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err))
      console.error(`Event processor "${name}" error during poll:`, err)
      // Realign the live stream to the committed checkpoint. During batch
      // accumulation the stream cursor (and any read-ahead buffer) advanced
      // past this batch, but `token` was NOT advanced — the failing UnitOfWork
      // never reached PREPARE_COMMIT. Closing discards the stream's buffer so
      // the next poll reopens at token.position() and re-reads — and thus
      // redelivers — the failed batch. Mirrors Axon's close-and-reopen-from-
      // token recovery.
      stream?.close()
      stream = null
      if (isRunning) pollTimer = setTimeout(poll, pollingIntervalMs * 2)
    } finally {
      processing = false
    }
  }

  /**
   * A FRESH context per invocation, closed over the batch's unit of work, the
   * event being delivered, and the buses of the handler receiving it. Nothing
   * is ambient: what a handler can reach is exactly what is bound here.
   *
   * The unit of work spans the BATCH; the message does not. Each event gets its
   * own context carrying its own message, so what a handler emits is stamped
   * with the event that actually caused it — the event leg of the same rule the
   * command leg gets from `lineage` on the bus.
   */
  function contextFor(
    uow: UnitOfWork,
    message: EventMessage,
    entry: ProcessorHandlerEntry,
  ): EventHandlerContext {
    const scheduler = entry.eventScheduler ?? eventScheduler
    return eventHandlerContext({
      uow,
      message,
      ...(stateManager !== undefined ? { stateManager } : {}),
      ...(entry.commandBus !== undefined ? { commandBus: entry.commandBus } : {}),
      ...(entry.queryBus !== undefined ? { queryBus: entry.queryBus } : {}),
      ...(scheduler !== undefined ? { eventScheduler: scheduler } : {}),
    })
  }

  async function processBatch(batch: SequencedEvent[]) {
    let batchEndToken: TrackingToken = token

    await newUoW().execute(async (uow) => {
      for (const sequencedEvent of batch) {
        uow.replaying = isReplaying(batchEndToken)

        await deliverEvent(sequencedEvent, uow)

        // Prefer the engine's own cursor token (carries the commit-order key for
        // gap-free resume); fall back to a position+1 token for dense-sequence
        // engines that don't supply one.
        batchEndToken = sequencedEvent.token
          ? advanceTokenTo(batchEndToken, sequencedEvent.token)
          : advanceToken(batchEndToken, sequencedEvent.sequence + 1n)
      }

      uow.onPrepareCommit(async () => {
        await tokenStore.store(name, segment, batchEndToken, uow)
        await tokenStore.extendClaim(name, segment, name, uow)
      })
    })

    token = batchEndToken
  }

  async function deliverEvent(sequencedEvent: SequencedEvent, uow: UnitOfWork) {
    const event = sequencedEvent.event
    const eventName = qualifiedNameToString(event.name)
    const entries = handlerMap.get(eventName)
    if (!entries || entries.length === 0) return

    // The event leg of the lineage rule. A command gets this from `lineage` on
    // its bus, which stamps the handled message so `ctx` carries it outward by
    // copying. An event has no bus, so the processor stamps the same rule from
    // the event's own identifier — onto the unit of work rather than onto the
    // event, because the event's OWN metadata already records which command
    // caused it and a handler must still be able to read that.
    uow.contributeCorrelationData(lineageOf(event))

    if (deadLetterDelivery) {
      await deadLetterDelivery.deliver(
        { ...sequencedEvent, event },
        entries.map((entry) => ({
          definition: entry.definition,
          context: contextFor(uow, event, entry),
        })),
        uow,
      )
      return
    }

    for (const entry of entries) {
      await entry.definition.handler(
        { ...event, sequence: sequencedEvent.sequence },
        contextFor(uow, event, entry),
      )
    }
  }

  // Replay a parked dead letter through the handlers, with a context built the
  // same way live delivery builds one so dependencies resolve identically.
  // Throws on the first handler failure so the reprocessor can requeue the
  // letter (delivery is at-least-once → handlers must be idempotent).
  async function replayDeadLetter(letter: DeadLetter, uow: UnitOfWork): Promise<void> {
    const event = letter.message
    const eventName = qualifiedNameToString(event.name)
    const entries = handlerMap.get(eventName)
    if (!entries || entries.length === 0) return

    uow.contributeCorrelationData(lineageOf(event))

    const position =
      typeof letter.diagnostics.position === "number" ? BigInt(letter.diagnostics.position) : 0n
    for (const entry of entries) {
      await entry.definition.handler(
        { ...event, sequence: position },
        contextFor(uow, event, entry),
      )
    }
  }

  function scheduleImmediate() {
    if (pollTimer !== null) clearTimeout(pollTimer)
    pollTimer = setTimeout(poll, 0)
  }

  return {
    get name() {
      return name
    },
    get running() {
      return isRunning
    },
    get position() {
      return token.position()
    },
    get replaying() {
      return isReplaying(token)
    },

    status(): EventProcessorStatus {
      return {
        running: isRunning,
        error: lastError,
        position: token.position(),
        caughtUp,
        replaying: isReplaying(token),
      }
    },

    async start() {
      if (isRunning) return
      await initialize()
      isRunning = true
      void poll()
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

    async reprocessDeadLetters(filter?: (sequenceId: string) => boolean) {
      if (!reprocessor) return false
      return reprocessor.reprocess(filter)
    },

    async resetTokens(startPosition: bigint = 0n, resetContext?: unknown) {
      if (isRunning) {
        throw new Error(`Processor "${name}" must be stopped before resetting tokens`)
      }

      const headPosition = await eventStore.getHeadPosition()

      token =
        headPosition <= startPosition
          ? globalSequenceToken(startPosition)
          : replayToken(
              globalSequenceToken(headPosition),
              globalSequenceToken(startPosition),
              resetContext,
            )

      await tokenStore.store(name, segment, token)
    },
  }
}
