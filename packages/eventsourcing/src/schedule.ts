import { generateIdentifier, type Metadata, resourceKey, type ResourceKey } from "@kronos-ts/common"
import { requireInvocationPhase } from "@kronos-ts/messaging/processing-state"
import type { z } from "zod"
import type {
  EventDescriptor,
  EventMessage,
  EventScheduler,
  ScheduleToken,
  CancelResult,
} from "@kronos-ts/messaging"

/**
 * Resource key for the event scheduler component.
 * Written by handling modules + processors at handler-invocation entry,
 * exactly like {@link STATE_MANAGER_KEY} — so the {@link schedule} helper
 * resolves the framework-configured scheduler from the active UnitOfWork.
 */
export const EVENT_SCHEDULER_KEY: ResourceKey<EventScheduler> = resourceKey("eventScheduler")

/** Schedule an event for future append from inside a handler. */
export interface ScheduleFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, at: Date): Promise<ScheduleToken>
  <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    at: Date,
    metadata: Metadata,
  ): Promise<ScheduleToken>
}

/**
 * Schedule {@link event} to be appended at {@link at}.
 *
 * The same one-call ergonomics as {@link append}/{@link send}: pass the event
 * descriptor + payload and the helper builds the {@link EventMessage} (tags,
 * metadata, identifier) and hands it to the configured {@link EventScheduler}.
 * No need to fetch the scheduler or construct a message by hand.
 *
 * Must be called from inside a UnitOfWork (throws otherwise). The schedule
 * participates in the active transaction — if the handler's UoW rolls back,
 * nothing is scheduled; if it commits, the event is durably scheduled and will
 * fire at or after `at` unless cancelled. Metadata defaults to the UoW
 * metadata, so correlation/causation lineage carries onto the fired event.
 *
 * Returns a {@link ScheduleToken} — persist it (e.g. in state via an event) to
 * {@link cancelSchedule} later. That is the deadline/process-manager pattern.
 */
export const schedule: ScheduleFunction = (async <P extends z.ZodType>(
  event: EventDescriptor<P>,
  payload: z.infer<P>,
  at: Date,
  metadata?: Metadata,
): Promise<ScheduleToken> => {
  const state = requireInvocationPhase() // D-43 mutator guard
  const scheduler = state.resources.get(EVENT_SCHEDULER_KEY.symbol) as EventScheduler | undefined
  if (!scheduler) throw new Error("No event scheduler configured")

  // Reject malformed fire times. A past-but-valid `at` is allowed — it fires
  // ASAP, which is the intended deadline semantic — but an Invalid Date is a
  // caller bug that otherwise behaves inconsistently across schedulers (the
  // in-memory one fires immediately; the postgres one throws on toISOString at
  // insert time). Fail fast and uniformly here instead.
  if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
    throw new Error(`schedule: \`at\` must be a valid Date, received ${String(at)}`)
  }

  const eventMessage: EventMessage = {
    identifier: generateIdentifier(),
    name: event.name,
    version: event.version,
    payload,
    metadata: metadata ?? state.metadata,
    timestamp: Date.now(),
    tags: event.tags ? event.tags(payload) : [],
  }
  return scheduler.schedule(eventMessage, at)
}) as ScheduleFunction

/** Schedule an event a fixed delay from now, from inside a handler. */
export interface ScheduleAfterFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>, delayMs: number): Promise<ScheduleToken>
  <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    delayMs: number,
    metadata: Metadata,
  ): Promise<ScheduleToken>
}

/**
 * Convenience wrapper over {@link schedule}: fire `delayMs` milliseconds from
 * now instead of at an absolute {@link Date}.
 */
export const scheduleAfter: ScheduleAfterFunction = (async (
  event: EventDescriptor<any>,
  payload: unknown,
  delayMs: number,
  metadata?: Metadata,
) => {
  // A non-finite delay (NaN/Infinity) would produce an Invalid Date; reject it
  // here so the error names the actual offending argument. A negative delay is
  // allowed — it resolves to a past time and fires ASAP.
  if (!Number.isFinite(delayMs)) {
    throw new Error(`scheduleAfter: \`delayMs\` must be a finite number, received ${String(delayMs)}`)
  }
  return schedule(event, payload, new Date(Date.now() + delayMs), metadata as Metadata)
}) as ScheduleAfterFunction

/**
 * Attempt to cancel a previously {@link schedule}d event from inside a handler.
 * UoW-aware: joins the active transaction, so cancelling then throwing does not
 * leave the schedule cancelled. See {@link CancelResult} for outcomes.
 */
export const cancelSchedule = async (token: ScheduleToken): Promise<CancelResult> => {
  const state = requireInvocationPhase() // D-43 mutator guard
  const scheduler = state.resources.get(EVENT_SCHEDULER_KEY.symbol) as EventScheduler | undefined
  if (!scheduler) throw new Error("No event scheduler configured")
  return scheduler.cancel(token)
}
