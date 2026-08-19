import type { EventStore } from "../stores/event-store.js"
import type { TokenStore } from "../stores/token-store.js"
import type { SequencedDeadLetterQueue } from "../stores/dead-letter-queue.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { Sequence } from "./sequence.js"

/**
 * A point-in-time snapshot of a processor's progress — the kronos analog of
 * AF5's `EventTrackerStatus`. kronos processors are single-segment, so this is
 * one snapshot per processor rather than a per-segment map.
 */
export interface EventProcessorStatus {
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
 */
export interface EventProcessor {
  /** Durable identity — the key this processor's token is stored under. */
  readonly name: string
  /** The log this processor reads. Also names the state manager `ctx.load` uses. */
  readonly eventStore: EventStore
  /** Where the position is kept. Never defaulted — see {@link eventProcessor}. */
  readonly tokenStore: TokenStore
  /** What each batch commits in. Never defaulted — see {@link eventProcessor}. */
  readonly unitOfWork: () => UnitOfWork
  /**
   * Which ordered lane each event belongs to. Absent means GLOBAL STREAM ORDER
   * — one lane, which is what a projection wants.
   */
  readonly sequence?: Sequence
  /** Where poison pills park. Absent means propagate and retry. */
  readonly deadLetterQueue?: SequencedDeadLetterQueue
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
 *   unitOfWork: drizzleUnitOfWork(db, unitOfWork),
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
 * @throws when a dead-letter queue is given without a sequence. Parking is a
 * LANE operation — the queue holds a failed event and everything behind it in
 * the same lane — so "which lane" is not optional once there is a queue. The
 * honest global-order answer for a projection is no queue at all (propagate
 * and retry); the honest per-entity answer is `sequentialPerTag(key)`.
 */
export function eventProcessor(config: {
  name: string
  eventStore: EventStore
  tokenStore: TokenStore
  unitOfWork: () => UnitOfWork
  sequence?: Sequence
  deadLetterQueue?: SequencedDeadLetterQueue
  batchSize?: number
}): EventProcessor {
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
export interface RunningProcessor {
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
