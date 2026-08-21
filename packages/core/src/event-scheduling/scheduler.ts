/**
 * SCHEDULING — NOT A SEAM. A CAPABILITY TIER ON THE LOG.
 *
 * ── WHY IT MOVED ───────────────────────────────────────────────────────────
 *
 * An event that has not happened yet is still an event, and where it lands
 * when its time comes is the LOG. Every implementation of the old
 * `EventScheduler` seam proved it: the in-memory one had to be handed an event
 * sink, the postgres one had to be handed an `eventStore` in its config, and
 * the KronosDB one only existed because the server appends the event itself.
 * Three implementations, three ways of spelling "and this is the log I fire
 * into" — which is the shape of a capability that belongs ON the log, not a
 * seam that sits beside it.
 *
 * So there is no `eventScheduler` field on an entry, nothing a host can wire
 * half of, and no "no scheduler configured" to discover at runtime. A log that
 * can schedule is a log that was WRAPPED:
 *
 * ```ts
 * const eventStore = postgresSchedulingEventStore(
 *   postgresEventStore(pg, { tagResolver }),
 *   pg,
 *   { unitOfWork: uow, tagResolver },
 * )
 * ```
 *
 * And the compiler makes you: `ctx.schedule` is structurally ABSENT from a
 * handling whose entry's log was never wrapped. See `IfScheduleCapable` in
 * `schedule.ts` — the mirror of `IfSnapshotCapable` in `event-sourcing/load.ts`.
 *
 * ── SEMANTICS, UNCHANGED ───────────────────────────────────────────────────
 *
 * - `schedule(event, at, uow)` is called from inside a unit of work (that is
 *   what `ctx.schedule` does). Where the family CAN be transactional it is: the
 *   record participates in the active task's adapter transaction, so a rolled
 *   back handling schedules nothing. Where it cannot — a server that owns the
 *   schedule the moment it is told — the wrapper says so in its own doc.
 *
 * - Once committed, the event WILL be appended at or after `at` unless
 *   {@link ScheduleCapability.cancelSchedule} wins the race. "At or after",
 *   because workers poll: a fire time is a floor, not a deadline.
 *
 * - `cancelSchedule(token)` answers a {@link CancelResult} rather than throwing,
 *   because the three outcomes are three different pieces of news and a caller
 *   compensating for a fired deadline needs to tell them apart.
 *
 * ── NOT WHAT THIS IS ───────────────────────────────────────────────────────
 *
 * - NOT a command scheduler. Schedule an event and let an automation turn it
 *   into a command on arrival.
 * - NOT cron. Each `schedule()` is one one-shot fire.
 */

import type { EventMessage } from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Opaque handle returned by {@link ScheduleCapability.schedule}. Hand it back
 * to cancel. Tokens are family-specific (postgres uses the row PK, the
 * in-memory tier a UUID, KronosDB the server's own token) but always carry a
 * stable `id`.
 *
 * PERSIST IT IF YOU MEAN TO CANCEL IT — typically by appending an event that
 * carries it. That is the deadline / process-manager pattern, and it is the
 * reason this is a value the caller keeps rather than something the framework
 * remembers on their behalf.
 */
export type ScheduleToken = {
  readonly id: string
}

/**
 * Outcome of {@link ScheduleCapability.cancelSchedule}.
 *
 * - `cancelled`        — the schedule was pending and is now cancelled. The
 *                        event will NOT be appended.
 * - `already-appended` — a worker fired it first; the event is in the log.
 *                        The caller decides whether to compensate.
 * - `not-found`        — nothing matches the token: already cancelled, never
 *                        existed, or from another deployment. Usually a no-op.
 */
export type CancelResult =
  | { readonly kind: "cancelled" }
  | { readonly kind: "already-appended" }
  | { readonly kind: "not-found" }

/**
 * WHAT A SCHEDULING WRAPPER ADDS, named on its own — so a wrapper can be
 * ADDITIVE rather than collapsing.
 *
 * The exact mirror of `SnapshotCapability`, and for the exact same reason. A
 * wrapper typed `(next: EventStore) => ScheduleCapableEventStore` would LAUNDER
 * every other capability its input carried: the runtime object still delegates
 * them, but the type says only "a schedulable event store", and a snapshotting
 * tier underneath is gone from the caller's view. So every family wrapper is
 * spelled `<E extends EventStore>(next: E, …) => E & ScheduleCapability`.
 *
 * TWO MEMBERS, and both had to be named — unlike snapshotting, where the read
 * was already in `EventStore`'s shape and only the write needed a name. A
 * schedule is not a read of the log, so neither half was there.
 */
export type ScheduleCapability = {
  /**
   * Schedule `event` for append at `at`.
   *
   * `at` is the wall-clock fire time. A past instant is legal and fires as soon
   * as the family notices — that is the intended deadline semantic.
   *
   * The unit of work is a TRAILING parameter, matching `storeSnapshot`,
   * `TokenStore` and `SequencedDeadLetterQueue`: `ctx.schedule` passes the
   * handling's, and a transactional family reads its adapter transaction off it
   * so the schedule commits with the rest of the task.
   */
  schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken>

  /**
   * Attempt to cancel a pending schedule. See {@link CancelResult} for the
   * three outcomes.
   *
   * Named `cancelSchedule` rather than `cancel`, because this rides on the LOG
   * now and a bare `cancel` on an event store says nothing about what is being
   * cancelled. The `ctx` verb carries the same name for the same reason.
   *
   * Safe inside a unit of work (joins the active transaction where the family
   * has one) or outside it (commits standalone) — ops and admin paths cancel
   * from no task at all.
   */
  cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult>
}

/**
 * A log that ALSO holds events that have not happened yet — the capability
 * tier, and the only place scheduling exists.
 *
 * IT IS AN INTERSECTION, spelled by hand rather than derived from one wrapper
 * with `ReturnType`, for the same reason `SnapshotCapableEventStore` is: this
 * capability has THREE composers in three packages
 * (`inMemorySchedulingEventStore`, `postgresSchedulingEventStore`,
 * `kronosDbSchedulingEventStore`), and deriving the contract from any one of
 * them would make two packages downstream of a third's implementation detail.
 * So the CONTRACT is written here and each wrapper's return type is annotated
 * with it; the type probe asserts they all still satisfy it.
 */
export type ScheduleCapableEventStore = EventStore & ScheduleCapability
