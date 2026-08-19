import { emptyMetadata, mergeMetadata, type Metadata } from "../primitives/metadata.js"
import { generateIdentifier } from "../primitives/identifier.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { z } from "zod"
import type { EventDescriptor } from "../messages/descriptor.js"
import type { EventMessage, Message } from "../messages/message.js"
import type { EventScheduler, ScheduleToken, CancelResult } from "../processor/event-scheduler.js"

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

/** The three scheduling capabilities, built together because they share deps. */
export interface ScheduleFunctions {
  readonly schedule: ScheduleFunction
  readonly scheduleAfter: ScheduleAfterFunction
  readonly cancelSchedule: (token: ScheduleToken) => Promise<CancelResult>
}

/**
 * Build the scheduling capabilities for ONE invocation, closed over that
 * invocation's unit of work and the configured {@link EventScheduler}.
 *
 * Internal — exported only via the "./schedule" subpath for the
 * HandlerContext. Handlers reach the results as `ctx.schedule`,
 * `ctx.scheduleAfter`, `ctx.cancelSchedule`.
 *
 * The same one-call ergonomics as `ctx.append`: pass the event descriptor and
 * payload and the helper builds the {@link EventMessage} (tags, metadata,
 * identifier) and hands it to the scheduler along with the unit of work.
 *
 * The schedule participates in the unit of work's transaction — if it rolls
 * back, nothing is scheduled; if it commits, the event is durably scheduled
 * and will fire at or after `at` unless cancelled. Metadata defaults to that of
 * the message this invocation is handling, so correlation/causation lineage
 * carries onto the fired event.
 *
 * Returns a {@link ScheduleToken} — persist it (e.g. in state via an event) to
 * cancel later. That is the deadline/process-manager pattern.
 */
export function scheduleFunctions(deps: {
  uow: UnitOfWork
  message?: Message
  eventScheduler?: EventScheduler
}): ScheduleFunctions {
  const schedule = (async <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    at: Date,
    metadata?: Metadata,
  ): Promise<ScheduleToken> => {
    const uow = requireInvocation(deps.uow)
    const scheduler = deps.eventScheduler
    if (!scheduler) throw new Error("No event scheduler configured")

    // Reject malformed fire times. A past-but-valid `at` is allowed — it fires
    // ASAP, which is the intended deadline semantic — but an Invalid Date is a
    // caller bug that otherwise behaves inconsistently across schedulers (the
    // in-memory one fires immediately; the postgres one throws on toISOString at
    // insert time). Fail fast and uniformly here instead.
    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new Error(`schedule: \`at\` must be a valid Date, received ${String(at)}`)
    }

    // Capture the unit of work's correlation data onto the scheduled event at
    // schedule-time — mirroring append(). There is no originating unit of work
    // to read from when the event later fires (timer / worker tick), so
    // schedule-time is the only point where this lineage is available. The
    // causationId becomes the message that scheduled the event, which is
    // exactly "what caused" the fired event.
    const baseMetadata = metadata ?? deps.message?.metadata ?? emptyMetadata()
    const correlationData = uow.correlationData()
    const eventMetadata =
      Object.keys(correlationData).length > 0
        ? mergeMetadata(baseMetadata, correlationData)
        : baseMetadata

    const eventMessage: EventMessage = {
      kind: "event",
      identifier: generateIdentifier(),
      name: event.name,
      version: event.version,
      payload,
      metadata: eventMetadata,
      timestamp: uow.now(),
      tags: event.tags ? event.tags(payload) : [],
    }
    return scheduler.schedule(eventMessage, at, uow)
  }) as ScheduleFunction

  const scheduleAfter = (async (
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
    // The delay is measured from the TASK's instant, not from wall time, so a
    // fixture that froze the clock gets a fire-time it can predict exactly.
    return schedule(event, payload, new Date(deps.uow.now() + delayMs), metadata as Metadata)
  }) as ScheduleAfterFunction

  /**
   * Attempt to cancel a previously scheduled event. UoW-aware: joins the unit
   * of work's transaction, so cancelling then throwing does not leave the
   * schedule cancelled.
   */
  const cancelSchedule = async (token: ScheduleToken): Promise<CancelResult> => {
    const uow = requireInvocation(deps.uow)
    const scheduler = deps.eventScheduler
    if (!scheduler) throw new Error("No event scheduler configured")
    return scheduler.cancel(token, uow)
  }

  return { schedule, scheduleAfter, cancelSchedule }
}
