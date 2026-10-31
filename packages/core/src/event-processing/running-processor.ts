import { qualifiedNameToString, type EventMessage } from "../messaging/messages.js"
import type {
  EventProcessor,
  EventProcessorStatus,
  RunningProcessor,
  SegmentStatus,
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
  isMergedToken,
  handledByMergedHalf,
  mergedToken,
  reachedBy,
} from "./tracking-token.js"
import { UnableToClaimTokenError } from "./token-store.js"
import { type Segment, inSegment, mergeableSegmentId, segmentsOf, sequenceHash, splitSegment } from "./segment.js"
import { sharedStream } from "./shared-stream.js"
import { generateIdentifier } from "../messaging/identifier.js"
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
  /**
   * How often a node looks for free segments, renews the claims it holds, and
   * writes where a quiet segment has got to (ms). Default: 5000 — half the
   * stores' default claim timeout, so a live node never lets a claim lapse.
   */
  readonly claimIntervalMs?: number
}

/** A segment this node holds and handles. */
type Worker = {
  readonly segment: Segment
  readonly token: () => TrackingToken
  readonly status: () => SegmentStatus
  /** Renew the claim by writing the token, fenced — which is also how a node learns it lost one. */
  readonly renew: () => Promise<void>
  /** Stop handling; write where it got to; release the claim unless told to keep it. */
  readonly stop: (release: boolean) => Promise<void>
}

/**
 * Start one delivery: read the log from the committed token, hand each event to
 * the handlers registered for it, and commit the new token in the same unit of
 * work the handlers wrote in.
 *
 * Internal to assembly — a host reaches the result as `app.processors`.
 *
 * A processor is one or more SEGMENTS, each with its own token and claim. A
 * node handles the segments it holds — claimed on start and every claim
 * interval after, renewed while held, released on stop — and every token write
 * is fenced on the claim. The segments a node holds share one read of the log.
 *
 * Failure semantics are the two the surface names, and nothing in between:
 * - No dead-letter queue → the error PROPAGATES. The batch rolls back, the
 *   token does not advance, the stream is realigned to the checkpoint and the
 *   batch is redelivered after a delay. The first delay is twice the polling
 *   interval; every consecutive failure doubles it, up to 60 seconds. A
 *   successful batch, or a stop and start, returns it to the first delay. A
 *   read model never silently skips. Other segments go on.
 * - Dead-letter queue → the failed event and everything behind it in its lane
 *   are parked, the batch commits, and the token advances past the pill.
 */
export function runEventProcessor<U extends UnitOfWork, E extends EventStore = EventStore>(
  options: RunEventProcessorOptions<U, E>,
): RunningProcessor {
  const { processor, handlers } = options
  const { name, eventStore, tokenStore, unitOfWork: newUoW, deadLetterQueue, sequence, concurrent: sideBySide } = processor
  // `batched(p, n)` is n at the head and behind it; unwrapped, one at the head
  // and the larger batch only while behind. A retry is one either way.
  const batchSize = processor.batchSize ?? 1
  const replayBatchSize = processor.batchSize ?? DEFAULT_REPLAY_BATCH_SIZE
  const pollingIntervalMs = options.pollingIntervalMs ?? 500
  const claimIntervalMs = options.claimIntervalMs ?? 5000
  const firstRetryDelayMs = pollingIntervalMs * 2

  const initialSegments = processor.segments?.count ?? 1
  const sequenceOf = processor.segments?.sequenceOf
  // Who this node is to the token store. One per running processor: two in one
  // process are two claimants, exactly as two processes would be.
  const owner = generateIdentifier()

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
  const log = sharedStream(eventStore, { ...(query !== undefined ? { query } : {}), readAhead: replayBatchSize })

  const handled = (event: EventMessage) => handlerMap.has(qualifiedNameToString(event.name))
  // Only events somebody handles are sequenced — the function is written for those.
  const hashOf = (event: EventMessage) => (sequenceOf === undefined ? 0 : sequenceHash(sequenceOf(event)))

  let isRunning = false
  let claimTimer: ReturnType<typeof setInterval> | null = null
  let claiming: Promise<void> | null = null
  let stopping: Promise<void> = Promise.resolve()
  const workers = new Map<number, Worker>()
  // How the segments stood when the processor last stopped — what `status()`
  // reports until it runs again.
  let parked: SegmentStatus[] = []
  // Where each segment this node knows of has got to — the ones it holds, and
  // the ones it last reset. What `position` and `replaying` report.
  const known = new Map<number, TrackingToken>()
  // Segments released on purpose, and until when this node leaves them alone.
  const releasedUntil = new Map<number, number>()
  // Where the log ended when this run began (or was reset): a token short of it
  // is BEHIND, which is when the larger batch pays. Unknown is not an error —
  // a segment then goes by whether its last batch drained the stream.
  let backlogEnd: bigint | undefined

  async function segmentsNow(): Promise<Segment[]> {
    let ids = await tokenStore.fetchSegments(name)
    if (ids.length === 0) {
      await tokenStore.initializeSegments(name, initialSegments)
      ids = await tokenStore.fetchSegments(name)
    }
    return segmentsOf(ids)
  }

  /** Renew what this node holds, and claim what is free. */
  async function claimRound() {
    for (const worker of [...workers.values()]) await worker.renew()
    const segments = await segmentsNow()
    const available = new Set(await tokenStore.fetchAvailableSegments(name))
    for (const segment of segments) {
      if (!isRunning) return
      if (workers.has(segment.id) || !available.has(segment.id)) continue
      if ((releasedUntil.get(segment.id) ?? 0) > Date.now()) continue
      let stored: TrackingToken | undefined
      try {
        stored = await tokenStore.claimToken(name, segment.id, owner)
      } catch (error) {
        if (error instanceof UnableToClaimTokenError) continue
        throw error
      }
      if (!isRunning) {
        await tokenStore.releaseClaim(name, segment.id, owner)
        return
      }
      workers.set(segment.id, startWorker(segment, stored))
    }
  }

  function claimSoon() {
    if (claiming !== null || !isRunning) return
    claiming = claimRound()
      .catch((error) => console.error(`Event processor "${name}": claiming segments failed:`, error))
      .finally(() => {
        claiming = null
      })
  }

  /** Take a segment for an operation on it: from the worker handling it, or by claiming it. */
  async function holdForOperation(id: number): Promise<"held" | "claimed"> {
    const worker = workers.get(id)
    if (worker !== undefined) {
      workers.delete(id)
      await worker.stop(false)
      return "held"
    }
    await tokenStore.claimToken(name, id, owner)
    return "claimed"
  }

  /** After an operation: handle what this node now holds, or let it go when not running. */
  async function resume(segment: Segment, token: TrackingToken | undefined) {
    if (isRunning) workers.set(segment.id, startWorker(segment, token))
    else await tokenStore.releaseClaim(name, segment.id, owner)
  }

  function startWorker(segment: Segment, stored: TrackingToken | undefined): Worker {
    let token: TrackingToken = stored ?? globalSequenceToken(0n)
    known.set(segment.id, token)
    // Where the batch that just failed ended. Until the token is past it the
    // segment takes the normal batch size, however far behind it is: a failed
    // batch of a thousand tells nothing about WHICH event failed, and retrying it
    // whole would fail whole, forever, on a poison pill near its end.
    let cautiousUntil = 0n
    let attemptEnd = 0n
    let active = true
    let stream: MessageStream<SequencedEvent> | null = null
    let pollTimer: ReturnType<typeof setTimeout> | null = null
    // The between-batches hop. `setImmediate`, not `setTimeout(…, 0)`: both
    // runtimes clamp a zero timeout to a millisecond, which at batch size 1 is
    // a hard ceiling of a thousand events a second before any real work. Not
    // unref'd either — see `scheduleImmediate`.
    let pollImmediate: ReturnType<typeof setImmediate> | null = null
    let processing = false
    let inFlight: Promise<void> = Promise.resolve()
    let renewing: Promise<void> = Promise.resolve()
    let caughtUp = false
    let lastError: Error | undefined
    // The wait before the next retry of a failed batch. Starts at the first delay,
    // doubles on every consecutive failure up to MAX_RETRY_DELAY_MS, and returns
    // to the first delay after a successful batch or a restart.
    let retryDelayMs = firstRetryDelayMs
    // With more than one segment, almost every event is in SOME OTHER segment,
    // so a quiet stretch is the normal case for each of them. Writing it at once
    // would be a token write per segment per event at the head; it is written
    // when the claim is renewed instead, and on stop. With one segment there is
    // nobody else's event to pass over, and it is written at once, as always.
    const deferQuiet = segment.mask !== 0
    let unwritten = false

    const accepts = (e: SequencedEvent) => handled(e.event) && inSegment(segment, hashOf(e.event))

    const set = (next: TrackingToken) => {
      token = next
      known.set(segment.id, next)
    }

    async function write(next: TrackingToken, uow?: U) {
      await tokenStore.store(name, segment.id, next, uow, owner)
    }

    /** Another node holds the claim now: stop handling this segment. */
    function lose() {
      if (!active) return
      active = false
      cancelScheduledPoll()
      stream?.close()
      stream = null
      if (workers.get(segment.id) === worker) workers.delete(segment.id)
      known.delete(segment.id)
      console.error(`Event processor "${name}": segment ${segment.id} is claimed by another node; no longer handling it`)
    }

    function openStream() {
      stream = log.open({ position: token.position(), token }, accepts)
      stream.setCallback(() => {
        if (active && !processing) scheduleImmediate()
      })
    }

    /**
     * A stream says how far it has looked. Through a stretch with nothing for
     * this segment in it the token would otherwise stay where the last event
     * left it, and a restart would read the stretch again. Only the position
     * moves — no handler ran, so there is nothing for the write to be atomic
     * with — and `advanceTokenTo` ends a replay when the position passes where
     * the reset began.
     */
    async function moveThroughQuietStretch() {
      const looked = stream?.progressToken?.()
      if (looked !== undefined && looked.position() > token.position()) {
        set(advanceTokenTo(token, looked))
        if (deferQuiet) unwritten = true
        else await write(token)
      }
    }

    function poll() {
      if (!active || processing) return
      processing = true
      inFlight = pollOnce().finally(() => {
        processing = false
      })
    }

    async function pollOnce() {
      try {
        // A renewal writes the token it read before this batch; let it land
        // first, or it could land after the batch and put the token back.
        await renewing
        if (!active) return
        if (!stream) openStream()

        // Check for stream errors — reopen if needed
        if (stream!.error()) {
          console.error(`Event processor "${name}": stream error, reopening:`, stream!.error())
          stream!.close()
          stream = null
          openStream()
          return
        }

        // Far behind the head, a batch is as many events as are ready, up to the
        // replay size; at the head, the normal size. A segment that just failed
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
          // segment has recovered.
          lastError = undefined
          retryDelayMs = firstRetryDelayMs
          if (active) {
            if (stream!.hasNextAvailable()) scheduleImmediate()
            // Drained everything currently available — caught up until the
            // stream callback wakes us with new events.
            else {
              caughtUp = true
              await moveThroughQuietStretch()
              // A replay ends on a poll that finds nothing, and with the stream
              // dry there is no event to wake one.
              if (isReplayToken(token)) schedule(pollingIntervalMs)
            }
          }
        } else {
          caughtUp = true
          await moveThroughQuietStretch()
          if (active) schedule(pollingIntervalMs)
        }
      } catch (err) {
        if (err instanceof UnableToClaimTokenError) {
          lose()
          return
        }
        lastError = err instanceof Error ? err : new Error(String(err))
        cautiousUntil = attemptEnd
        // A stopped segment schedules no retry, so it neither waits nor backs
        // off further.
        const retryInMs = active ? retryDelayMs : undefined
        console.error(
          retryInMs === undefined
            ? `Event processor "${name}" error during poll:`
            : `Event processor "${name}" error during poll, retrying in ${retryInMs}ms:`,
          err,
        )
        // Realign the stream to the committed checkpoint. During batch
        // accumulation the stream cursor advanced past this batch, but `token`
        // was NOT advanced — the failing UnitOfWork never reached
        // PREPARE_COMMIT. Closing lets the next poll reopen at
        // token.position() and re-read — and thus redeliver — the failed batch.
        // Mirrors Axon's close-and-reopen-from-token recovery.
        stream?.close()
        stream = null
        if (retryInMs !== undefined) {
          retryDelayMs = Math.min(retryDelayMs * 2, Math.max(MAX_RETRY_DELAY_MS, firstRetryDelayMs))
          schedule(retryInMs)
        }
      }
    }

    async function processBatch(batch: SequencedEvent[]) {
      let batchEndToken: TrackingToken = token
      // Whether each event is delivered as part of a replay: the segment is
      // replaying, and the token just past this event is still within where it
      // had got to when it was reset. Per event — a replay can end inside a
      // batch — and read off the EVENT, not the token before it: through a
      // filtered stream the next event can be well past the reset point.
      const replaying: boolean[] = []
      // Whether each event is new to this segment. Only false just after a
      // merge, for an event the half that was ahead had already handled.
      const fresh: boolean[] = []
      for (const sequencedEvent of batch) {
        // Prefer the engine's own cursor token (carries the commit-order key for
        // gap-free resume); fall back to a position+1 token for dense-sequence
        // engines that don't supply one.
        const past = sequencedEvent.token ?? globalSequenceToken(sequencedEvent.sequence + 1n)
        replaying.push(isReplayToken(batchEndToken) && batchEndToken.tokenAtReset.covers(past))
        fresh.push(!(isMergedToken(batchEndToken) && handledByMergedHalf(batchEndToken, hashOf(sequencedEvent.event), past)))
        batchEndToken = advanceTokenTo(batchEndToken, past)
      }

      // The MINTED handle is the batch's task — see the note in `localCommandBus`
      // for why it is read here rather than off `execute`'s parameter.
      const uow = newUoW()
      await uow.execute(async () => {
        if (sideBySide === undefined) {
          for (let i = 0; i < batch.length; i++) {
            if (!fresh[i]) continue
            uow.replaying = replaying[i]!
            await deliverEvent(batch[i]!, uow, replaying[i]!)
          }
        } else {
          uow.replaying = replaying[0] ?? false
          await deliverSideBySide(batch, replaying, fresh, uow, sideBySide)
        }

        // Only the token, fenced on the claim: a node that lost its claim fails
        // here and rolls the batch back.
        uow.onPrepareCommit(async () => {
          await write(batchEndToken, uow)
        })
      })

      set(batchEndToken)
      unwritten = false
    }

    function schedule(ms: number) {
      pollTimer = setTimeout(poll, ms)
      pollTimer.unref?.()
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

    const worker: Worker = {
      segment,
      token: () => token,
      status: () => ({
        segment: segment.id,
        mask: segment.mask,
        position: token.position(),
        caughtUp,
        replaying: isReplaying(token),
        ...(lastError !== undefined ? { error: lastError } : {}),
      }),
      async renew() {
        // A batch in flight renews the claim with its own write.
        if (!active || processing) return
        // A WRITE, not `extendClaim`: the fenced write refuses a node that lost
        // its claim, where an extension would quietly match nothing — and a
        // node stuck retrying a failing handler would never find out. It also
        // writes where a quiet segment has got to.
        const attempt = write(token)
          .then(() => {
            unwritten = false
          })
          .catch((error) => {
            if (error instanceof UnableToClaimTokenError) lose()
            else throw error
          })
        renewing = attempt.catch(() => {})
        await attempt
      },
      async stop(release) {
        if (!active) return
        active = false
        cancelScheduledPoll()
        stream?.close()
        stream = null
        await inFlight
        try {
          if (unwritten) await write(token)
          if (release) await tokenStore.releaseClaim(name, segment.id, owner)
        } catch (error) {
          if (!(error instanceof UnableToClaimTokenError)) throw error
        }
      },
    }

    poll()
    return worker
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
    // Fixed per EVENT, not read off the batch's task: with sequences side by
    // side the task's flag is shared by events on both sides of a replay's end.
    return { ...context, isReplay: () => replaying }
  }

  /**
   * The batch's sequences, side by side: each sequence's events one after
   * another in log order, the sequences all at once. Only events somebody
   * handles are sequenced — the function is written for those.
   *
   * Every sequence SETTLES before the batch does. A failure does not cut the
   * others short: they are still running against the batch's transaction, and
   * rolling it back under them would turn one failure into many. The error
   * that fails the batch is the one at the earliest position.
   */
  async function deliverSideBySide(batch: SequencedEvent[], replaying: boolean[], fresh: boolean[], uow: U, sequenceOf: Sequence) {
    const lanes = new Map<string, number[]>()
    for (let i = 0; i < batch.length; i++) {
      const event = batch[i]!.event
      if (!fresh[i] || !handled(event)) continue
      const id = sequenceOf(event)
      const lane = lanes.get(id)
      if (lane) lane.push(i)
      else lanes.set(id, [i])
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

  function position(): bigint {
    let low: bigint | undefined
    for (const token of known.values()) if (low === undefined || token.position() < low) low = token.position()
    return low ?? 0n
  }
  const replaying = () => [...known.values()].some(isReplaying)

  return {
    get name() {
      return name
    },
    get running() {
      return isRunning
    },
    get position() {
      return position()
    },
    get replaying() {
      return replaying()
    },

    status(): EventProcessorStatus {
      const held = workers.size > 0 || isRunning ? [...workers.values()].map((w) => w.status()) : parked
      const error = held.find((s) => s.error !== undefined)?.error
      return {
        running: isRunning,
        ...(error !== undefined ? { error } : {}),
        position: position(),
        caughtUp: held.length > 0 && held.every((s) => s.caughtUp),
        replaying: replaying(),
      }
    },

    segments() {
      return [...workers.values()].map((w) => w.status()).sort((a, b) => a.segment - b.segment)
    },

    async start() {
      if (isRunning) return
      await stopping
      isRunning = true
      backlogEnd = await eventStore.getHeadPosition().catch(() => undefined)
      try {
        await claimRound()
      } catch (error) {
        isRunning = false
        throw error
      }
      claimTimer = setInterval(claimSoon, claimIntervalMs)
      claimTimer.unref?.()
    },

    stop() {
      isRunning = false
      if (claimTimer !== null) {
        clearInterval(claimTimer)
        claimTimer = null
      }
      const held = [...workers.values()]
      parked = held.map((w) => w.status())
      workers.clear()
      stopping = Promise.all([claiming, ...held.map((w) => w.stop(true))]).then(() => undefined)
      return stopping
    },

    async reprocessDeadLetters(filter?: (sequenceId: string) => boolean) {
      if (!reprocessor) return false
      return reprocessor.reprocess(filter)
    },

    async resetTokens(startPosition: bigint = 0n, reset?: (uow: UnitOfWork) => Promise<void> | void) {
      if (isRunning) {
        throw new Error(`Processor "${name}" must be stopped before resetting tokens`)
      }
      await stopping

      // Every segment, held by this node for the reset: a node still running
      // the processor would go on from where it was and write over the reset.
      const segments = await segmentsNow()
      const claimed: number[] = []
      try {
        for (const { id } of segments) {
          try {
            await tokenStore.claimToken(name, id, owner)
          } catch (error) {
            if (!(error instanceof UnableToClaimTokenError)) throw error
            throw new Error(
              `Processor "${name}" cannot be reset: segment ${id} is held by another node. ` +
                `Stop the processor on every node first.`,
            )
          }
          claimed.push(id)
        }

        // ONE unit of work: what `reset` clears and the tokens that make the
        // processor fold it again commit together, and a `reset` that throws
        // leaves both as they were.
        const start = globalSequenceToken(startPosition)
        const next = new Map<number, TrackingToken>()
        const uow = newUoW()
        await uow.execute(async () => {
          for (const { id } of segments) {
            // The replay runs up to where the segment HAD GOT TO — not the head
            // of the log: an event it never handled is not a replay, however far
            // behind it was. Reset again mid-replay and it is still the first
            // reset's point. Never run, or reset to where it already is or past
            // it, and nothing is replayed.
            const stored = await tokenStore.get(name, id, uow)
            const reached = stored === undefined ? undefined : reachedBy(stored)
            next.set(id, reached !== undefined && !start.covers(reached) ? replayToken(reached, start) : start)
          }
          await reset?.(uow)
          uow.onPrepareCommit(async () => {
            for (const [id, token] of next) await tokenStore.store(name, id, token, uow, owner)
          })
        })

        known.clear()
        for (const [id, token] of next) known.set(id, token)
        backlogEnd = await eventStore.getHeadPosition().catch(() => undefined)
      } finally {
        for (const id of claimed) await tokenStore.releaseClaim(name, id, owner).catch(() => {})
      }
    },

    async releaseSegment(id: number, forMs = claimIntervalMs * 2) {
      releasedUntil.set(id, Date.now() + forMs)
      const worker = workers.get(id)
      if (worker === undefined) return
      workers.delete(id)
      known.delete(id)
      await worker.stop(true)
    },

    async splitSegment(id: number) {
      if (sequenceOf === undefined) {
        throw new Error(
          `Processor "${name}": a segment can only be split when the processor says which events go together — ` +
            `wrap it in segmented(p, count, sequenceOf).`,
        )
      }
      const segment = (await segmentsNow()).find((s) => s.id === id)
      if (segment === undefined) throw new Error(`Processor "${name}" has no segment ${id}`)
      const [first, second] = splitSegment(segment)
      const how = await holdForOperation(id)
      let token: TrackingToken | undefined
      try {
        const uow = newUoW()
        await uow.execute(async () => {
          token = await tokenStore.get(name, id, uow)
          if (token !== undefined && isMergedToken(token)) {
            throw new Error(`Processor "${name}": segment ${id} is still catching up after a merge; split it once it has`)
          }
          await tokenStore.claimToken(name, second.id, owner, uow)
          uow.onPrepareCommit(async () => {
            await tokenStore.store(name, second.id, token ?? globalSequenceToken(0n), uow, owner)
          })
        })
      } catch (error) {
        if (how === "held") await resume(segment, token ?? known.get(id))
        else await tokenStore.releaseClaim(name, id, owner)
        throw error
      }
      known.delete(id)
      await resume(first, token)
      await resume(second, token)
    },

    async mergeSegment(id: number) {
      const segments = await segmentsNow()
      const segment = segments.find((s) => s.id === id)
      if (segment === undefined) throw new Error(`Processor "${name}" has no segment ${id}`)
      if (segment.mask === 0) throw new Error(`Processor "${name}" has one segment; there is nothing to merge it with`)
      const partnerId = mergeableSegmentId(segment)
      const partner = segments.find((s) => s.id === partnerId && s.mask === segment.mask)
      if (partner === undefined) {
        throw new Error(
          `Processor "${name}": segment ${id} cannot be merged yet — its other half, segment ${partnerId}, ` +
            `has been split further. Merge that first.`,
        )
      }
      const held: Array<[Segment, "held" | "claimed"]> = []
      const tokens = new Map<number, TrackingToken | undefined>()
      try {
        held.push([segment, await holdForOperation(segment.id)])
        held.push([partner, await holdForOperation(partner.id)])
        const keep = Math.min(segment.id, partner.id)
        const drop = Math.max(segment.id, partner.id)
        const merged: Segment = { id: keep, mask: segment.mask >>> 1 }
        let token: TrackingToken | undefined
        const uow = newUoW()
        await uow.execute(async () => {
          for (const s of [segment, partner]) {
            const t = await tokenStore.get(name, s.id, uow)
            tokens.set(s.id, t)
            if (t !== undefined && (isReplayToken(t) || isMergedToken(t))) {
              throw new Error(
                `Processor "${name}": segment ${s.id} is ${isReplayToken(t) ? "replaying" : "still catching up after a merge"}; ` +
                  `merge once it has finished`,
              )
            }
          }
          const zero = globalSequenceToken(0n)
          token = mergedToken(segment.mask, [
            { segment: segment.id, token: tokens.get(segment.id) ?? zero },
            { segment: partner.id, token: tokens.get(partner.id) ?? zero },
          ])
          uow.onPrepareCommit(async () => {
            await tokenStore.store(name, keep, token!, uow, owner)
            await tokenStore.deleteToken(name, drop, uow)
          })
        })
        known.delete(drop)
        known.delete(keep)
        await resume(merged, token)
      } catch (error) {
        for (const [s, how] of held) {
          if (how === "held") await resume(s, tokens.get(s.id) ?? known.get(s.id))
          else await tokenStore.releaseClaim(name, s.id, owner).catch(() => {})
        }
        throw error
      }
    },
  }
}
