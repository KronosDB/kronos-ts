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
   * Which ordered lane each event belongs to. Absent means GLOBAL STREAM ORDER
   * — one lane, which is what a projection wants.
   */
  readonly sequence?: Sequence
  /** Where poison pills park. Absent means propagate and retry. Keyed on `U`. */
  readonly deadLetterQueue?: SequencedDeadLetterQueue<U>
  /** Events per batch (one unit of work). Default: 1. */
  readonly batchSize?: number
}

/**
 * Describe one event delivery.
 *
 * ```ts
 * const balances = eventProcessor({
 *   name: "balances",
 *   eventStore, tokenStore,
 *   unitOfWork: drizzleUnitOfWork(unitOfWork, db),
 *   sequence: sequentialPerTag("accountId"),
 *   deadLetterQueue: drizzleDeadLetterQueue(db),
 * })
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
 * A DEAD-LETTER QUEUE WITHOUT A SEQUENCE DOES NOT COMPILE. Parking is a LANE
 * operation — the queue holds a failed event and everything behind it in the
 * same lane — so "which lane" is not optional once there is a queue. The honest
 * global-order answer for a projection is no queue at all (propagate and
 * retry); the honest per-entity answer is `sequentialPerTag(key)`. See
 * {@link SequenceDemand}.
 */
/**
 * WHERE THIS DELIVERY READS, KEEPS ITS PLACE, AND COMMITS — the part of the
 * config that is not about lanes.
 *
 * `U` IS INFERRED FROM HERE, and it has to be: `unitOfWork: () => U` is the one
 * COVARIANT mention of the task in the whole config, so it is the one the
 * checker can read an answer off. `tokenStore: TokenStore<U>` then CHECKS
 * against that answer rather than contributing to it, which is exactly the
 * direction the demand wants — the factory says what the tasks are, and the
 * stores say whether they can live with that.
 */
export type EventProcessorSite<U extends UnitOfWork = UnitOfWork> = {
  name: string
  eventStore: EventStore
  tokenStore: TokenStore<U>
  unitOfWork: () => U
  batchSize?: number
}

/**
 * THE LANE HALF of the config — the two fields the demand below relates.
 *
 * Split out because it is the part {@link eventProcessor} infers a SECOND type
 * parameter from. Keeping `U` out of the inference for that parameter is what
 * lets the two demands coexist: the family check reads the task off the site,
 * the lane check reads the queue off the literal, and neither steals the
 * other's inference.
 */
export type EventProcessorLane<U extends UnitOfWork = UnitOfWork> = {
  sequence?: Sequence
  deadLetterQueue?: SequencedDeadLetterQueue<U>
}

/** The whole config, for anybody who wants to name it. */
export type EventProcessorConfig<U extends UnitOfWork = UnitOfWork> =
  EventProcessorSite<U> & EventProcessorLane<U>

/**
 * THE DEMAND: a queue implies a lane.
 *
 * Branch on the config the caller actually wrote. A `deadLetterQueue` present
 * and a `sequence` absent adds a REQUIRED property the literal does not have,
 * so the compiler reports a missing member whose keys ARE the message — the
 * same trick the correlation demand uses, and for the same reason: give the object
 * type a name and TypeScript prints the NAME, which tells a reader nothing.
 * Left anonymous, it has no shorthand to reach for and prints the structure.
 *
 * The wording is GENERAL, because core is certain about the rule and about
 * nothing else — `sequentialPerTag` is core's own export, so naming it is not
 * core guessing at somebody's stack.
 *
 * This replaces a `throw` at construction time. The old one was honest but
 * late: it fired when the composition root ran, which in a deployed process is
 * boot and in a test suite is whenever that file is first imported.
 */
export type SequenceDemand<C> = C extends { deadLetterQueue: SequencedDeadLetterQueue<any> }
  ? C extends { sequence: Sequence }
    ? unknown
    : {
        readonly sequence: {
          readonly ERROR: "this processor has a deadLetterQueue but no sequence, and parking is a lane operation"
          readonly FIX: "add `sequence: sequentialPerTag(\"<tagKey>\")`, or drop the queue and let failures propagate and retry"
        }
      }
  : unknown

export function eventProcessor<
  U extends UnitOfWork = UnitOfWork,
  L extends EventProcessorLane<U> = EventProcessorLane<U>,
>(config: EventProcessorSite<U> & L & SequenceDemand<L>): EventProcessor<U> {
  // THE DEFENSIVE ASSERT. A caller with a compiler cannot reach it — the
  // combination does not typecheck — so this is the JavaScript path, kept for
  // the same reason `repository.ts` keeps its one line: a named mistake beats a
  // processor that quietly parks nothing.
  if (config.deadLetterQueue && !config.sequence) {
    throw new Error(
      `eventProcessor("${config.name}"): a deadLetterQueue was given without a sequence. ` +
        `Parking is a lane operation — the queue parks a failed event AND everything behind ` +
        `it in the same lane — so there is no lane-free way to park. Add ` +
        `\`sequence: sequentialPerTag("<tagKey>")\`, or drop the queue and let failures ` +
        `propagate and retry.`,
    )
  }
  return {
    name: config.name,
    eventStore: config.eventStore,
    tokenStore: config.tokenStore,
    unitOfWork: config.unitOfWork,
    ...(config.sequence !== undefined ? { sequence: config.sequence } : {}),
    ...(config.deadLetterQueue !== undefined ? { deadLetterQueue: config.deadLetterQueue } : {}),
    ...(config.batchSize !== undefined ? { batchSize: config.batchSize } : {}),
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
  resetTokens(startPosition?: bigint, resetContext?: unknown): Promise<void>
  /**
   * Replay parked dead letters back through the handlers (the oldest matching
   * lane). No-op returning false when no dead-letter queue is configured. Safe
   * to call whether or not the processor is running.
   */
  reprocessDeadLetters(filter?: (sequenceId: string) => boolean): Promise<boolean>
}
