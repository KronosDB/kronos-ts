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
   * Which bucket each event parks under when it fails. Set by
   * {@link deadLettering}, together with {@link deadLetterQueue}.
   */
  readonly sequence?: Sequence
  /** Where a failing event parks. Absent: the batch fails and is retried. Keyed on `U`. */
  readonly deadLetterQueue?: SequencedDeadLetterQueue<U>
  /**
   * Which bucket each event belongs to when the batch's buckets run SIDE BY
   * SIDE. Absent means one event at a time, in log order. Set by
   * {@link concurrent}; the same function as {@link sequence} when both are set.
   */
  readonly concurrent?: Sequence
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
 * {@link batched}, {@link concurrent}, {@link deadLettering}.
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
 * Up to `size` events per batch — one unit of work, one token write — at the
 * head and behind it alike. A batch only ever takes the events that are READY,
 * so at the head it is the one or two that just arrived and the read model is
 * as fresh as it ever was; behind, it is `size`. A failed batch is retried one
 * event at a time until the token is past it, so a poison event ends up alone.
 *
 * THE BOUND IS MEMORY: the batch is held decoded until it commits, and the
 * stream reads ahead by as many.
 */
export function batched<U extends UnitOfWork>(processor: EventProcessor<U>, size: number): EventProcessor<U> {
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`batched("${processor.name}"): a batch is a whole number of events, at least 1 — got ${size}`)
  }
  return { ...processor, batchSize: size }
}

/**
 * Run the batch's buckets side by side. Events `buckets` puts in the same
 * bucket are handled one after another, in log order; events in different
 * buckets are not ordered against each other at all. The batch still commits
 * as ONE unit of work: without a dead-letter queue a failure in any bucket
 * fails the batch once every bucket has settled, and the whole batch is
 * redelivered.
 *
 * Every handler on this processor runs concurrently across buckets, so a
 * handler must touch only what belongs to its event's bucket. Unrecognised
 * events go wherever the function sends them: `e.identifier` gives each a
 * bucket of its own, a constant puts them all in one.
 */
export function concurrent<U extends UnitOfWork>(processor: EventProcessor<U>, buckets: Sequence): EventProcessor<U> {
  sameBuckets(processor, processor.sequence, buckets, "concurrent")
  return { ...processor, concurrent: buckets }
}

/**
 * Park a failing event in `queue`, under its bucket, and everything behind it
 * in that bucket, so the batch commits and the token moves past it. Other
 * buckets go on.
 *
 * With {@link concurrent} the two must bucket events THE SAME WAY: a bucket
 * that runs side by side with another cannot be parked behind it.
 */
export function deadLettering<U extends UnitOfWork>(
  processor: EventProcessor<U>,
  queue: SequencedDeadLetterQueue<U>,
  buckets: Sequence,
): EventProcessor<U> {
  sameBuckets(processor, processor.concurrent, buckets, "deadLettering")
  return { ...processor, deadLetterQueue: queue, sequence: buckets }
}

function sameBuckets(processor: EventProcessor<any>, existing: Sequence | undefined, buckets: Sequence, wrapper: string) {
  if (existing !== undefined && existing !== buckets) {
    throw new Error(
      `${wrapper}("${processor.name}"): this processor already buckets its events with another function. ` +
        `concurrent and deadLettering must be given the same one — a bucket that runs side by side with ` +
        `another cannot be parked behind it.`,
    )
  }
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
  /** Current effective position in the event stream. */
  readonly position: bigint
  /** Whether the processor is currently replaying events. */
  readonly replaying: boolean
  start(): Promise<void>
  stop(): void
  /**
   * Point-in-time progress snapshot (running / error / position / caughtUp /
   * replaying) — the surface an admin UI reads to show processor health.
   */
  status(): EventProcessorStatus
  /**
   * Reset the processor to replay events from a starting position. The
   * processor must be stopped before calling this.
   */
  resetTokens(startPosition?: bigint): Promise<void>
  /**
   * Replay parked dead letters back through the handlers (the oldest matching
   * lane). No-op returning false when no dead-letter queue is configured. Safe
   * to call whether or not the processor is running.
   */
  reprocessDeadLetters(filter?: (sequenceId: string) => boolean): Promise<boolean>
}
