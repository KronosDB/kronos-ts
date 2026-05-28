/**
 * EventScheduler — schedule an event to be appended to the event store at
 * a future time, with the option to cancel before the fire-time.
 *
 * # Semantics
 *
 * - `schedule(event, at)` MUST be called from inside a UnitOfWork (i.e.,
 *   from a command/event/query handler, or any code that opened a UoW
 *   via `runInNewUoW`). The scheduled record participates in the active
 *   UoW transaction: if the UoW rolls back, the schedule is not persisted;
 *   if the UoW commits, the schedule is durably stored.
 *
 * - Once the schedule is committed, the implementation guarantees that the
 *   event WILL be appended to the event store at or after `at`, unless
 *   {@link EventScheduler.cancel} is called and succeeds before the
 *   fire-time. "At or after" because workers poll on an interval — fire
 *   times are not real-time deadlines.
 *
 * - `cancel(token)` returns a {@link CancelResult} discriminated union so
 *   callers can branch on three distinct outcomes: the schedule was
 *   cancelled before firing, the event had already been appended (too
 *   late), or no such schedule exists (already-cancelled, never-existed,
 *   or token from a different deployment).
 *
 * - Cancel is also UoW-aware: when called inside a UoW, it participates
 *   in the active tx so a handler that cancels and then throws does NOT
 *   leave the schedule cancelled. When called outside any UoW (rare —
 *   typically an ops/admin path), it commits standalone.
 *
 * # Calling from handlers
 *
 * Implementations resolve themselves from the active UoW's resources
 * (similar to {@link send} and {@link emitUpdate}), so handler code uses
 * the scheduler the framework configured for it. The interface itself
 * is transport-agnostic — postgres and in-memory implementations live
 * in their respective packages.
 *
 * # NOT what this is
 *
 * - This is NOT a command scheduler. AF5 schedules events, not commands;
 *   if you want a command to run later, schedule an event and run an
 *   automation processor that turns the event into a command on receipt.
 * - This is NOT a cron / recurring scheduler. Each `schedule()` produces
 *   a single one-shot fire.
 */

import type { EventMessage } from "./message.js"

/**
 * Opaque handle returned by {@link EventScheduler.schedule}. Pass back to
 * {@link EventScheduler.cancel} to attempt cancellation. Tokens are
 * implementation-specific (postgres uses the row PK; in-memory uses a
 * UUID) but always carry a stable `id`.
 */
export interface ScheduleToken {
  readonly id: string
}

/**
 * Outcome of {@link EventScheduler.cancel}.
 *
 * - `cancelled`        — the schedule existed in `pending` state and was
 *                        successfully marked `cancelled`. Event will NOT
 *                        be appended.
 * - `already-appended` — a worker already fired this schedule; the event
 *                        is in the event store. Caller decides whether
 *                        compensation is needed.
 * - `not-found`        — no row matches the token. Could mean: already
 *                        cancelled, never existed, or wrong store. Caller
 *                        usually treats this as a no-op.
 */
export type CancelResult =
  | { readonly kind: "cancelled" }
  | { readonly kind: "already-appended" }
  | { readonly kind: "not-found" }

export interface EventScheduler {
  /**
   * Schedule {@link event} for append at {@link at}. Must be called inside
   * a UoW; throws otherwise. The schedule participates in the active UoW
   * tx and is only durable once the UoW commits.
   *
   * `at` is the wall-clock fire-time. Past dates are valid — they cause
   * the worker to fire the schedule on its next poll.
   */
  schedule(event: EventMessage, at: Date): Promise<ScheduleToken>

  /**
   * Attempt to cancel a pending schedule. See {@link CancelResult} for
   * the three possible outcomes.
   *
   * Safe to call from inside a UoW (joins the active tx) or outside
   * (commits standalone).
   */
  cancel(token: ScheduleToken): Promise<CancelResult>
}
