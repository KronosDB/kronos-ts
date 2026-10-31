import type { EventStore } from "../event-sourcing/event-store.js"
import type { TokenStore } from "./token-store.js"
import type { SequencedDeadLetterQueue } from "./dead-letter-queue.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { Sequence } from "./sequence.js"

/**
 * A point-in-time snapshot of a processor's progress — the kronos analog of
 * AF5's `EventTrackerStatus`. kronos processors are single-segment, so this is
 * one snapshot per processor rather than a per-segment map.
 */
export type EventProcessorStatus = {
  /** Whether the processor's polling/streaming loop is active. */
  readonly running: boolean
  /**
   * The most recent unrecovered processing error, if any. Cleared on the next
   * successful batch. A non-undefined value is the kronos `isErrorState`.
   */
  readonly error?: Error
  /** Current committed position in the event stream. */
  readonly position: bigint
  /** Whether the processor has consumed all currently-available events. */
  readonly caughtUp: boolean
  /** Whether the processor is currently replaying (reset) the stream. */
  readonly replaying: boolean
}

/**
 * A processor as a VALUE — everything about where this delivery reads from,
 * where it keeps its place, and what it commits in. Not a builder, not a
 * module, not a registration: a record you can hold, compare, and hand to as
 * many event handlers as belong to it.
 *
 * `name` is the DURABLE identity. Tokens persist under it across restarts, so
 * two entries naming the same processor ARE the same delivery — which is why
 * `kronos` groups event handlers by this name rather than by object identity.
 *
 * `U` is the unit of work each batch commits in — whatever `unitOfWork` mints.
 * It defaults to the bare {@link UnitOfWork} and is threaded through to the
 * event handler's `ctx.unitOfWork`, so a handler that demands a composed
 * capability does not typecheck against a processor built without one.
 */
export type EventProcessor<U extends UnitOfWork = UnitOfWork> = {
  /** Durable identity — the key this processor's token is stored under. */
  readonly name: string
  /** The log this processor reads. Also names the state manager `ctx.load` uses. */
  readonly eventStore: EventStore
  /**
   * Where the position is kept. Never defaulted — see {@link eventProcessor}.
   *
   * KEYED ON `U`, so a store that writes through a family's transaction and a
   * unit of work minted by another family cannot both be on this value.
   */
  readonly tokenStore: TokenStore<U>
  /** What each batch commits in. Never defaulted — see {@link eventProcessor}. */
  readonly unitOfWork: () => U
  /**
   * Events per batch — one unit of work, one token write — at the head and
   * behind it. Set by {@link batched}. Absent: one at the head, and up to
   * 1000 while the processor is BEHIND (replaying after a reset, or catching up
   * on a backlog), where the per-batch cost is what dominates.
   */
  readonly batchSize?: number
  /**
   * How long the processor waits before looking again when the stream has
   * nothing ready, and the base of its retry delay after a failed batch (ms).
   * Set by {@link polling}. Absent: 500. New events wake it sooner either way.
   */
  readonly pollingIntervalMs?: number
  /**
   * Which sequence each event parks under when it fails. Set by
   * {@link deadLettering}, together with {@link deadLetterQueue}.
   */
  readonly sequence?: Sequence
  /** Where a failing event parks. Absent: the batch fails and is retried. Keyed on `U`. */
  readonly deadLetterQueue?: SequencedDeadLetterQueue<U>
  /**
   * Which sequence each event belongs to when the batch's sequences run SIDE
   * BY SIDE. Absent means one event at a time, in log order. Set by
   * {@link concurrent}; the same function as {@link sequence} when both are set.
   */
  readonly concurrent?: Sequence
  /**
   * How many segments the processor starts with, and the sequence each event
   * belongs to — which decides its segment. Set by {@link segmented}. Absent:
   * one segment, which one node at a time holds.
   */
  readonly segments?: { readonly count: number; readonly sequenceOf: Sequence }
}

/**
 * Describe one event delivery: what it is called, the log it reads, where it
 * keeps its place, and what each batch commits in.
 *
 * ```ts
 * const balances = eventProcessor("balances", eventStore, postgresTokenStore(pg), postgresUnitOfWork(unitOfWork, pg))
 *
 * kronos({ eventHandlers: projections.map((h) => ({ ...h, commandBus, queryBus, processor: balances })) })
 * ```
 *
 * `tokenStore` and `unitOfWork` are CONSTITUTIVE, never defaulted. A missing
 * token store boots fine and then replays the whole log on every restart; a
 * missing (or merely non-transactional) unit of work commits the projection
 * write and the token update as two unrelated effects, so a crash between them
 * either replays an event the projection already applied or skips one it never
 * did. Both surface much later as a read model nobody can explain, which is
 * why they are parameters rather than defaults.
 *
 * `U` IS READ OFF `unitOfWork`: `() => U` is the one COVARIANT mention of the
 * task, so it is the one the checker can read an answer off, and the token
 * store CHECKS against that answer rather than contributing to it — the
 * factory says what the tasks are, and the store says whether it can live
 * with that.
 *
 * What the processor does with its batch is said by wrapping the value:
 * {@link batched}, {@link concurrent}, {@link deadLettering}, {@link segmented}.
 */
export function eventProcessor<U extends UnitOfWork = UnitOfWork>(
  name: string,
  eventStore: EventStore,
  tokenStore: TokenStore<NoInfer<U>>,
  unitOfWork: () => U,
): EventProcessor<U> {
  return { name, eventStore, tokenStore, unitOfWork }
}

// ---------------------------------------------------------------------------
// What a processor does with its batch, said as functions over the value.
// Each one returns a new value with one more thing said about it, so they
// compose in any order through whatever `pipe` the host uses:
//
//   pipe(
//     eventProcessor(name, eventStore, tokenStore, unitOfWork),
//     (p) => batched(p, 1000),
//     (p) => concurrent(p, byAccount),
//   )
// ---------------------------------------------------------------------------

/**
 * Up to `size` items per batch, on anything that works in batches: an event
 * processor, or a store's polling worker such as `postgresSchedulingEventStore`.
 *
 * On a processor: up to `size` events per batch — one unit of work, one token
 * write — at the head and behind it alike. A batch only ever takes the events
 * that are READY, so at the head it is the one or two that just arrived and the
 * read model is as fresh as it ever was; behind, it is `size`. A failed batch is
 * retried one event at a time until the token is past it, so a poison event
 * ends up alone. THE BOUND IS MEMORY: the batch is held decoded until it
 * commits, and the stream reads ahead by as many.
 */
export function batched<T extends { readonly batchSize?: number }>(worker: T, size: number): T {
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`batched(${nameOf(worker)}): a batch is a whole number, at least 1 — got ${size}`)
  }
  return { ...worker, batchSize: size }
}

/**
 * Look again every `ms` when there is nothing to do, on anything that polls:
 * an event processor, or a store's polling worker such as
 * `postgresSchedulingEventStore`. A processor's stream wakes it as soon as an
 * event arrives, so for a processor this is the fallback, and the base of the
 * delay before a failed batch is retried.
 */
export function polling<T extends { readonly pollingIntervalMs?: number }>(worker: T, ms: number): T {
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new Error(`polling(${nameOf(worker)}): an interval is a positive number of milliseconds — got ${ms}`)
  }
  return { ...worker, pollingIntervalMs: ms }
}

function nameOf(worker: object): string {
  return "name" in worker && typeof worker.name === "string" ? `"${worker.name}"` : ""
}

/**
 * Run the batch's sequences side by side. Events `sequenceOf` puts in the
 * same sequence are handled one after another, in log order; events in
 * different sequences are not ordered against each other at all. The batch
 * still commits as ONE unit of work: without a dead-letter queue a failure in
 * any sequence fails the batch once every sequence has settled, and the whole
 * batch is
 * redelivered.
 *
 * Every handler on this processor runs concurrently across sequences, so a
 * handler must touch only what belongs to its event's sequence. Unrecognised
 * events go wherever the function sends them: `e.identifier` gives each a
 * sequence of its own, a constant puts them all in one.
 */
export function concurrent<U extends UnitOfWork>(processor: EventProcessor<U>, sequenceOf: Sequence): EventProcessor<U> {
  sameSequence(processor, sequenceOf, "concurrent")
  return { ...processor, concurrent: sequenceOf }
}

/**
 * Park a failing event in `queue`, under its sequence, and everything behind
 * it in that sequence, so the batch commits and the token moves past it. Other
 * sequences go on.
 *
 * With {@link concurrent} the two must sequence events THE SAME WAY: a sequence
 * that runs side by side with another cannot be parked behind it.
 */
export function deadLettering<U extends UnitOfWork>(
  processor: EventProcessor<U>,
  queue: SequencedDeadLetterQueue<U>,
  sequenceOf: Sequence,
): EventProcessor<U> {
  sameSequence(processor, sequenceOf, "deadLettering")
  return { ...processor, deadLetterQueue: queue, sequence: sequenceOf }
}

/**
 * Split the processor into `count` segments (a power of two), each with its own
 * token and its own claim. An event's segment is decided by its sequence, so a
 * sequence stays in one segment and keeps its order; different segments are
 * handled independently — by different nodes, or side by side on one — each
 * committing its own batches.
 *
 * `count` is what the processor STARTS with: it is read the first time the
 * processor runs, and from then on the token rows are the truth. Change it
 * with `splitSegment` and `mergeSegment` on the running processor. A node
 * claims every segment it finds free, so the first node to start holds them
 * all until segments are released to others (`releaseSegment`).
 *
 * With {@link concurrent} and {@link deadLettering} all three must be given the
 * same function.
 */
export function segmented<U extends UnitOfWork>(
  processor: EventProcessor<U>,
  count: number,
  sequenceOf: Sequence,
): EventProcessor<U> {
  if (!Number.isInteger(count) || count < 1 || count > 2 ** 16 || (count & (count - 1)) !== 0) {
    throw new Error(`segmented("${processor.name}"): the number of segments is a power of two, from 1 to 65536 — got ${count}`)
  }
  sameSequence(processor, sequenceOf, "segmented")
  return { ...processor, segments: { count, sequenceOf } }
}

function sameSequence(processor: EventProcessor<any>, sequenceOf: Sequence, wrapper: string) {
  const existing = [processor.concurrent, processor.sequence, processor.segments?.sequenceOf]
  if (existing.some((s) => s !== undefined && s !== sequenceOf)) {
    throw new Error(
      `${wrapper}("${processor.name}"): this processor already sequences its events with another function. ` +
        `concurrent, deadLettering and segmented must be given the same one — a sequence that runs side by ` +
        `side with another cannot be parked behind it, and one spread over two segments loses its order.`,
    )
  }
}

/** One segment a node holds. */
export type SegmentStatus = {
  readonly segment: number
  /** Which sequences are in it: those whose hash, masked, equals `segment`. */
  readonly mask: number
  readonly position: bigint
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly error?: Error
}

/**
 * A processor that is actually running — what `kronos` hands back on
 * `app.processors`, and what a distributed control plane drives.
 *
 * The value ({@link EventProcessor}) says what a delivery IS; this says what it
 * is DOING. They are different things with different lifetimes, so they are
 * different types.
 */
export type RunningProcessor = {
  readonly name: string
  readonly running: boolean
  /** Current effective position in the event stream: the lowest of the segments this node knows. */
  readonly position: bigint
  /** Whether the processor is currently replaying events. */
  readonly replaying: boolean
  /**
   * Run: claim every segment that is free, now and every few seconds after,
   * and handle each one this node holds. A node that holds none waits.
   */
  start(): Promise<void>
  /** Stop handling, write where each segment got to, and release the claims. */
  stop(): Promise<void>
  /**
   * Point-in-time progress snapshot (running / error / position / caughtUp /
   * replaying) — the surface an admin UI reads to show processor health.
   */
  status(): EventProcessorStatus
  /**
   * Reset the processor to replay events from `startPosition` (default: the
   * start of the log). The processor must be stopped first, and started again
   * after.
   *
   * The replay runs up to where the processor had got to; `ctx.isReplay()` is
   * true for those events and false for every event after. A reset to where
   * the processor already is, or past it, replays nothing.
   *
   * `reset` runs in the unit of work that stores the new token — clear what
   * the processor's handlers wrote there, and the clearing and the reset
   * commit together. If it throws, neither happens.
   */
  resetTokens(startPosition?: bigint, reset?: (uow: UnitOfWork) => Promise<void> | void): Promise<void>
  /** The segments this node holds, and how each is doing. */
  segments(): ReadonlyArray<SegmentStatus>
  /**
   * Let go of a segment so another node can claim it. This node does not claim
   * it back for `forMs` (default: two claim intervals).
   */
  releaseSegment(segment: number, forMs?: number): Promise<void>
  /**
   * Split a segment in two. This node must hold it or be able to claim it, and
   * holds both halves after. Needs {@link segmented}.
   */
  splitSegment(segment: number): Promise<void>
  /**
   * Merge a segment with its other half. This node must hold both or be able to
   * claim them, and neither may be replaying.
   */
  mergeSegment(segment: number): Promise<void>
  /**
   * Replay parked dead letters back through the handlers (the oldest matching
   * lane). No-op returning false when no dead-letter queue is configured. Safe
   * to call whether or not the processor is running.
   */
  reprocessDeadLetters(filter?: (sequenceId: string) => boolean): Promise<boolean>
}
