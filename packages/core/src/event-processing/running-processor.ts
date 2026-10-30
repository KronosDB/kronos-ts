import { qualifiedNameToString, type EventMessage } from "../messaging/messages.js"
import type {
  EventProcessor,
  EventProcessorStatus,
  RunningProcessor,
} from "./processor.js"
import type { EventHandler } from "./handler.js"
import type { MessageStream, SequencedEvent } from "./source.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { DeadLetter } from "./dead-letter-queue.js"
import type { Sequence } from "./sequence.js"
import { deadLetteringDelivery } from "./dead-lettering.js"
import { type DeadLetterReprocessor, deadLetterReprocessor } from "./dead-letter-reprocessor.js"
import {
  type TrackingToken,
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isReplaying,
  advanceTokenTo,
} from "./tracking-token.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import type { CommandBus } from "../command-handling/bus.js"
import type { QueryBus, SubscriptionCapableQueryBus } from "../query-handling/bus.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { eventHandlerContext, type EventHandlerContext } from "./context.js"

/** The longest wait between retries of a failed batch (ms). */
const MAX_RETRY_DELAY_MS = 60_000

/** Events per batch while the processor is behind, unless `batched` says otherwise. */
const DEFAULT_REPLAY_BATCH_SIZE = 1000

/**
 * One event handler as the processor sees it: the definition, plus the buses
 * ITS context reaches. The buses ride on the entry rather than on the
 * processor because which bus a handler dispatches through is a property of
 * the handler's deployment, not of the cursor it shares with its neighbours.
 */
export type ProcessorHandlerEntry<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = {
  readonly definition: EventHandler<any, EventHandlerContext<E, SubscriptionCapableQueryBus<U>, U>>
  readonly commandBus?: CommandBus<U>
  readonly queryBus?: QueryBus<U>
  /**
   * The log THIS handler's `ctx.load` sources from. Per entry, like the buses —
   * and absent means the log the processor itself reads, which is the honest
   * default: an automation reacting to a log almost always decides against it.
   */
  readonly eventStore?: E
}

export type RunEventProcessorOptions<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = {
  readonly processor: EventProcessor<U>
  readonly handlers: ReadonlyArray<ProcessorHandlerEntry<U, E>>
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
 *   batch is redelivered after a delay. The first delay is twice the polling
 *   interval; every consecutive failure doubles it, up to 60 seconds. A
 *   successful batch, or a stop and start, returns it to the first delay. A
 *   read model never silently skips.
 * - Dead-letter queue → the failed event and everything behind it in its lane
 *   are parked, the batch commits, and the token advances past the pill.
 */
export function runEventProcessor<U extends UnitOfWork, E extends EventStore = EventStore>(
  options: RunEventProcessorOptions<U, E>,
): RunningProcessor {
  const { processor, handlers } = options
  const { name, eventStore, tokenStore, unitOfWork: newUoW, deadLetterQueue, sequence, concurrent: buckets } = processor
  // `batched(p, n)` is n at the head and behind it; unwrapped, one at the head
  // and the larger batch only while behind. A retry is one either way.
  const batchSize = processor.batchSize ?? 1
  const replayBatchSize = processor.batchSize ?? DEFAULT_REPLAY_BATCH_SIZE
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

  const handlerMap = new Map<string, ProcessorHandlerEntry<U, E>[]>()
  for (const entry of handlers) {
    const eventName = qualifiedNameToString(entry.definition.descriptor.name)
    const bucket = handlerMap.get(eventName)
    if (bucket) bucket.push(entry)
    else handlerMap.set(eventName, [entry])
  }

  // WHAT THE STORE IS ASKED FOR: the types somebody handles, and nothing else.
  // The handlers are the whole truth about which events matter, so a store that
  // can filter (KronosDB does it on the server, Postgres in its WHERE clause)
  // never sends the rest. It is a request — `deliverEvent` still drops an event
  // nobody handles, so a store that ignores the query is just slower.
  const query: EventQuery | undefined = handlerMap.size > 0 ? { types: [...handlerMap.keys()] } : undefined

  let token: TrackingToken = globalSequenceToken(0n)
  // Where the log ended when this run began (or was reset): a token short of it
  // is BEHIND, which is when the larger batch pays. Unknown is not an error —
  // the processor then goes by whether its last batch drained the stream.
  let backlogEnd: bigint | undefined
  // Where the batch that just failed ended. Until the token is past it the
  // processor takes the normal batch size, however far behind it is: a failed
  // batch of a thousand tells nothing about WHICH event failed, and retrying it
  // whole would fail whole, forever, on a poison pill near its end.
  let cautiousUntil = 0n
  let attemptEnd = 0n
  let isRunning = false
  let stream: MessageStream<SequencedEvent> | null = null
  let pollTimer: ReturnType<typeof setTimeout> | null = null
  // The between-batches hop. `setImmediate`, not `setTimeout(…, 0)`: both
  // runtimes clamp a zero timeout to a millisecond, which at batch size 1 is
  // a hard ceiling of a thousand events a second before any real work. Not
  // unref'd either — see `scheduleImmediate`.
  let pollImmediate: ReturnType<typeof setImmediate> | null = null
  let processing = false
  let caughtUp = false
  let lastError: Error | undefined
  // The wait before the next retry of a failed batch. Starts at the first delay,
  // doubles on every consecutive failure up to MAX_RETRY_DELAY_MS, and returns
  // to the first delay after a successful batch or a restart.
  const firstRetryDelayMs = pollingIntervalMs * 2
  let retryDelayMs = firstRetryDelayMs

  async function initialize() {
    await tokenStore.initializeSegments(name, 1)
    const stored = await tokenStore.get(name, segment)
    if (stored !== undefined) token = stored
    backlogEnd = await eventStore.getHeadPosition().catch(() => undefined)
  }

  function openStream() {
    stream = eventStore.open({
      position: token.position(),
      token,
      ...(query !== undefined ? { query } : {}),
      readAhead: replayBatchSize,
    })
    stream.setCallback(() => {
      if (isRunning && !processing) scheduleImmediate()
    })
  }

  /**
   * A stream that filters says how far it has looked. Through a stretch with
   * nothing for this processor in it the token would otherwise stay where the
   * last event left it, and a restart would read the stretch again. Only the
   * position moves — no handler ran, so there is nothing for the write to be
   * atomic with — and `advanceTokenTo` ends a replay when the position passes
   * where the reset began.
   */
  async function moveThroughQuietStretch() {
    const looked = stream?.progressToken?.()
    if (looked !== undefined && looked.position() > token.position()) {
      token = advanceTokenTo(token, looked)
      await tokenStore.store(name, segment, token)
    }
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

      // Far behind the head, a batch is as many events as are ready, up to the
      // replay size; at the head, the normal size. A processor that just failed
      // retries ONE EVENT AT A TIME until it is past the failure, so the event
      // that fails ends up alone in its batch instead of dragging a thousand
      // others through every retry.
      const behind = isReplaying(token) || (backlogEnd !== undefined && token.position() < backlogEnd)
      const target = token.position() < cautiousUntil ? 1 : behind || !caughtUp ? replayBatchSize : batchSize
      attemptEnd = 0n

      const batch: SequencedEvent[] = []
      let event = stream!.next()
      while (event && batch.length < target) {
        batch.push(event)
        if (batch.length < target && stream!.hasNextAvailable()) event = stream!.next()
        else break
      }

      if (batch.length > 0) {
        caughtUp = false
        attemptEnd = batch[batch.length - 1]!.sequence + 1n
        await processBatch(batch)
        // A clean batch clears any prior error and the retry backoff — the
        // processor has recovered.
        lastError = undefined
        retryDelayMs = firstRetryDelayMs
        if (isRunning) {
          if (stream!.hasNextAvailable()) scheduleImmediate()
          // Drained everything currently available — caught up until the
          // stream callback wakes us with new events.
          else {
            caughtUp = true
            await moveThroughQuietStretch()
            // A replay ends on a poll that finds nothing, and with the stream
            // dry there is no event to wake one.
            if (isReplayToken(token)) {
              pollTimer = setTimeout(poll, pollingIntervalMs)
              pollTimer.unref?.()
            }
          }
        }
      } else {
        caughtUp = true
        await moveThroughQuietStretch()
        // If replay is done and no more events, unwrap. A stream that reports
        // progress is the one to say when: it can be empty simply because the
        // next events are still on the wire.
        if (isReplayToken(token) && stream!.progressToken === undefined) {
          token = globalSequenceToken(token.position())
          await tokenStore.store(name, segment, token)
        }
        if (isRunning) { pollTimer = setTimeout(poll, pollingIntervalMs); pollTimer.unref?.() }
      }
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err))
      cautiousUntil = attemptEnd
      // A stopped processor schedules no retry, so it neither waits nor backs
      // off further.
      const retryInMs = isRunning ? retryDelayMs : undefined
      console.error(
        retryInMs === undefined
          ? `Event processor "${name}" error during poll:`
          : `Event processor "${name}" error during poll, retrying in ${retryInMs}ms:`,
        err,
      )
      // Realign the live stream to the committed checkpoint. During batch
      // accumulation the stream cursor (and any read-ahead buffer) advanced
      // past this batch, but `token` was NOT advanced — the failing UnitOfWork
      // never reached PREPARE_COMMIT. Closing discards the stream's buffer so
      // the next poll reopens at token.position() and re-reads — and thus
      // redelivers — the failed batch. Mirrors Axon's close-and-reopen-from-
      // token recovery.
      stream?.close()
      stream = null
      if (retryInMs !== undefined) {
        retryDelayMs = Math.min(retryDelayMs * 2, Math.max(MAX_RETRY_DELAY_MS, firstRetryDelayMs))
        pollTimer = setTimeout(poll, retryInMs)
        pollTimer.unref?.()
      }
    } finally {
      processing = false
    }
  }

  /**
   * A FRESH context per invocation, closed over the batch's unit of work, the
   * event being delivered, and the buses of the handler receiving it. Nothing
   * is ambient: what a handler can reach is exactly what is bound here.
   *
   * The unit of work spans the BATCH; the message does not — each event is
   * handed to its handler individually. What a handler emits carries nothing of
   * the triggering event unless the host wrapped that handler in
   * `correlatingHandler(next, from)`, which reads the event it is given and
   * attaches the cargo itself. The processor stamps nothing: it has no policy
   * to stamp.
   */
  function contextFor(
    uow: U,
    entry: ProcessorHandlerEntry<U, E>,
    replaying: boolean,
  ): EventHandlerContext<E, SubscriptionCapableQueryBus<U>, U> {
    // `ctx.load` sources from the entry's own log when it has one, and
    // otherwise from the log this delivery reads — the same store, in every
    // arrangement anybody actually writes.
    const loadFrom = entry.eventStore ?? eventStore
    const context = eventHandlerContext<U, E, SubscriptionCapableQueryBus<U>>({
      uow,
      eventStore: loadFrom as E,
      ...(entry.commandBus !== undefined ? { commandBus: entry.commandBus } : {}),
      ...(entry.queryBus !== undefined ? { queryBus: entry.queryBus } : {}),
    })
    // Fixed per EVENT, not read off the batch's task: with buckets side by
    // side the task's flag is shared by events on both sides of a replay's end.
    return { ...context, isReplay: () => replaying }
  }

  async function processBatch(batch: SequencedEvent[]) {
    let batchEndToken: TrackingToken = token
    // Whether each event is delivered as part of a replay: the processor is
    // replaying, and the token just past this event is still within where it
    // had got to when it was reset. Per event — a replay can end inside a
    // batch — and read off the EVENT, not the token before it: through a
    // filtered stream the next event can be well past the reset point.
    const replaying: boolean[] = []
    for (const sequencedEvent of batch) {
      // Prefer the engine's own cursor token (carries the commit-order key for
      // gap-free resume); fall back to a position+1 token for dense-sequence
      // engines that don't supply one.
      const past = sequencedEvent.token ?? globalSequenceToken(sequencedEvent.sequence + 1n)
      replaying.push(isReplayToken(batchEndToken) && batchEndToken.tokenAtReset.covers(past))
      batchEndToken = advanceTokenTo(batchEndToken, past)
    }

    // The MINTED handle is the batch's task — see the note in `localCommandBus`
    // for why it is read here rather than off `execute`'s parameter.
    const uow = newUoW()
    await uow.execute(async () => {
      if (buckets === undefined) {
        for (let i = 0; i < batch.length; i++) {
          uow.replaying = replaying[i]!
          await deliverEvent(batch[i]!, uow, replaying[i]!)
        }
      } else {
        uow.replaying = replaying[0] ?? false
        await deliverSideBySide(batch, replaying, uow, buckets)
      }

      // Only the token. No claim is ever taken on this path (`claimToken` has
      // no caller), so the per-batch `extendClaim` that used to follow was a
      // statement matching zero rows on every commit — a third of the
      // statements in a batch-size-1 transaction, for nothing. Claims get
      // their own design when a processor actually takes one.
      uow.onPrepareCommit(async () => {
        await tokenStore.store(name, segment, batchEndToken, uow)
      })
    })

    token = batchEndToken
  }

  /**
   * The batch's buckets, side by side: each bucket's events one after another
   * in log order, the buckets all at once. Only events somebody handles are
   * bucketed — the function is written for those.
   *
   * Every bucket SETTLES before the batch does. A failure does not cut the
   * others short: they are still running against the batch's transaction, and
   * rolling it back under them would turn one failure into many. The error
   * that fails the batch is the one at the earliest position.
   */
  async function deliverSideBySide(batch: SequencedEvent[], replaying: boolean[], uow: U, bucketOf: Sequence) {
    const lanes = new Map<string, number[]>()
    for (let i = 0; i < batch.length; i++) {
      const event = batch[i]!.event
      if (!handlerMap.has(qualifiedNameToString(event.name))) continue
      const bucket = bucketOf(event)
      const lane = lanes.get(bucket)
      if (lane) lane.push(i)
      else lanes.set(bucket, [i])
    }
    const failures: Array<{ at: number; error: unknown }> = []
    await Promise.all(
      [...lanes.values()].map(async (lane) => {
        for (const i of lane) {
          try {
            await deliverEvent(batch[i]!, uow, replaying[i]!)
          } catch (error) {
            failures.push({ at: i, error })
            return
          }
        }
      }),
    )
    if (failures.length > 0) throw failures.reduce((first, f) => (f.at < first.at ? f : first)).error
  }

  async function deliverEvent(sequencedEvent: SequencedEvent, uow: U, replaying: boolean) {
    const event = sequencedEvent.event
    const eventName = qualifiedNameToString(event.name)
    const entries = handlerMap.get(eventName)
    if (!entries || entries.length === 0) return

    if (deadLetterDelivery) {
      await deadLetterDelivery.deliver(
        { ...sequencedEvent, event },
        entries.map((entry) => ({
          definition: entry.definition,
          context: contextFor(uow, entry, replaying),
        })),
        uow,
      )
      return
    }

    for (const entry of entries) {
      await entry.definition.handler(
        { ...event, sequence: sequencedEvent.sequence },
        contextFor(uow, entry, replaying),
      )
    }
  }

  // Replay a parked dead letter through the handlers, with a context built the
  // same way live delivery builds one so dependencies resolve identically.
  // Throws on the first handler failure so the reprocessor can requeue the
  // letter (delivery is at-least-once → handlers must be idempotent).
  async function replayDeadLetter(letter: DeadLetter, uow: U): Promise<void> {
    const event = letter.message
    const eventName = qualifiedNameToString(event.name)
    const entries = handlerMap.get(eventName)
    if (!entries || entries.length === 0) return

    const position =
      typeof letter.diagnostics.position === "number" ? BigInt(letter.diagnostics.position) : 0n
    for (const entry of entries) {
      await entry.definition.handler({ ...event, sequence: position }, contextFor(uow, entry, uow.replaying))
    }
  }

  function scheduleImmediate() {
    cancelScheduledPoll()
    // NOT unref'd. Node does not count an unref'd immediate when it decides how
    // long to wait for I/O, so the next batch would wait for the next socket
    // read or timer — a millisecond or two a batch on a busy loop, and as long
    // as the loop is quiet on an idle one. An immediate runs on the next turn
    // of the loop anyway; it cannot keep a process alive.
    pollImmediate = setImmediate(poll)
  }

  function cancelScheduledPoll() {
    if (pollTimer !== null) {
      clearTimeout(pollTimer)
      pollTimer = null
    }
    if (pollImmediate !== null) {
      clearImmediate(pollImmediate)
      pollImmediate = null
    }
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
      retryDelayMs = firstRetryDelayMs
      isRunning = true
      void poll()
    },

    stop() {
      isRunning = false
      cancelScheduledPoll()
      if (stream) {
        stream.close()
        stream = null
      }
    },

    async reprocessDeadLetters(filter?: (sequenceId: string) => boolean) {
      if (!reprocessor) return false
      return reprocessor.reprocess(filter)
    },

    async resetTokens(startPosition: bigint = 0n, reset?: (uow: UnitOfWork) => Promise<void> | void) {
      if (isRunning) {
        throw new Error(`Processor "${name}" must be stopped before resetting tokens`)
      }

      // ONE unit of work: what `reset` clears and the token that makes the
      // processor fold it again commit together, and a `reset` that throws
      // leaves both as they were.
      let next: TrackingToken = globalSequenceToken(startPosition)
      const uow = newUoW()
      await uow.execute(async () => {
        // The replay runs up to where the processor HAD GOT TO — not the head of
        // the log: an event it never handled is not a replay, however far
        // behind it was. Reset again mid-replay and it is still the first
        // reset's point. Never run, or reset to where it already is or past it,
        // and nothing is replayed.
        const stored = await tokenStore.get(name, segment, uow)
        const reached = stored !== undefined && isReplayToken(stored) ? stored.tokenAtReset : stored
        if (reached !== undefined && !next.covers(reached)) next = replayToken(reached, next)
        await reset?.(uow)
        uow.onPrepareCommit(async () => {
          await tokenStore.store(name, segment, next, uow)
        })
      })

      token = next
      backlogEnd = await eventStore.getHeadPosition().catch(() => undefined)
    },
  }
}
