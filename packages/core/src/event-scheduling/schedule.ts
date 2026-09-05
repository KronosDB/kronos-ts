import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  emptyMetadata,
  type Metadata,
  type EventDescriptor,
  type EventMessage,
} from "../messaging/messages.js"
import { generateIdentifier } from "../messaging/identifier.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type {
  CancelResult,
  ScheduleStoreCapability,
  ScheduleCapableEventStore,
  ScheduleToken,
} from "./scheduler.js"

// ---------------------------------------------------------------------------
// THE DEMAND — one alias, and every scheduling surface derives from it.
//
// This is the store tier that REACHES A CONTEXT: a handler has three new
// members to call, so there is a capability (`ScheduleCapability`) and a
// supply-side conditional that decides whether the entry's log provides it.
// Snapshotting, the other store tier, adds nothing a handler calls and so has
// no context capability at all — see `event-sourcing/load.ts`.
// ---------------------------------------------------------------------------

/**
 * THE DEMAND. Branch on whether `E` — the log an entry actually wired — carries
 * the scheduling capability.
 *
 * THIS IS THE ONE PLACE THE QUESTION IS ASKED, and {@link SuppliedScheduleCapability} is its
 * single face: what a CAPABLE store ADDS to a context. Against a bare store
 * those three verbs are structurally ABSENT — not present-and-throwing — so
 * `ctx.schedule(...)` is a "property does not exist" error at the call site
 * rather than a runtime discovery in production at 3am.
 *
 * Snapshotting needed two faces because a state can DECLARE that it caches, so
 * there was something to refuse as well as something to offer. Nothing declares
 * that it schedules — a handler either calls the verb or does not — so this
 * side of the mirror has one face, and the shape stays the same.
 *
 * NOTHING RUNS. This is erased entirely; the only runtime trace is one
 * defensive assert in {@link scheduleFunctions} for callers who have no
 * compiler at all.
 */
export type IfScheduleCapable<E extends EventStore, Capable, Bare> =
  E extends ScheduleCapableEventStore ? Capable : Bare

/** Schedule an event for future append from inside a handler. */
export type ScheduleFunction = {
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>, at: Date): Promise<ScheduleToken>
  <P extends StandardSchemaV1>(
    event: EventDescriptor<P>,
    payload: InferOutput<P>,
    at: Date,
    metadata: Metadata,
  ): Promise<ScheduleToken>
}

/** Schedule an event a fixed delay from now, from inside a handler. */
export type ScheduleAfterFunction = {
  <P extends StandardSchemaV1>(event: EventDescriptor<P>, payload: InferOutput<P>, delayMs: number): Promise<ScheduleToken>
  <P extends StandardSchemaV1>(
    event: EventDescriptor<P>,
    payload: InferOutput<P>,
    delayMs: number,
    metadata: Metadata,
  ): Promise<ScheduleToken>
}

/** Cancel a schedule this handling (or an earlier one) armed. */
export type CancelScheduleFunction = (token: ScheduleToken) => Promise<CancelResult>

/**
 * The three scheduling capabilities, built together because they share deps.
 *
 * INTERNAL to the builder. What a CONTEXT gets is {@link SuppliedScheduleCapability}, which
 * is this record or nothing at all depending on the entry's log.
 */
export type ScheduleCapability = {
  readonly schedule: ScheduleFunction
  readonly scheduleAfter: ScheduleAfterFunction
  readonly cancelSchedule: CancelScheduleFunction
}

/**
 * THE SUPPLY SIDE of {@link ScheduleCapability}: the capability when the
 * entry's log can schedule, `unknown` when it cannot.
 *
 * A context is ASSEMBLED BY INTERSECTION — `EventHandlerContext<E, Q, U>` is the
 * base shape & this & the subscription tier's — so against a bare log the
 * members do not exist, and `unknown` disappears from the intersection without
 * a trace. A handler never writes this type; it intersects `ScheduleCapability`
 * and the entry's `E` decides whether the supplied context satisfies it.
 */
export type SuppliedScheduleCapability<E extends EventStore> = IfScheduleCapable<
  E,
  ScheduleCapability,
  unknown
>

/**
 * The log a schedule is armed on, or an error naming what was missing.
 *
 * THE ONLY RUNTIME TRACE OF THE WHOLE DEMAND. A caller with a compiler cannot
 * reach this: the verbs are absent from a context whose `E` is bare, and the
 * entry that supplied `E` had to carry a wrapped log to typecheck. What is left
 * is JavaScript callers and `as any` — for whom a named mistake beats
 * `undefined is not a function`.
 */
function requireScheduling(eventStore: EventStore | undefined, call: string): ScheduleStoreCapability {
  if (eventStore && typeof (eventStore as Partial<ScheduleStoreCapability>).schedule === "function") {
    return eventStore as unknown as ScheduleStoreCapability
  }
  // GENERAL, BECAUSE CORE CANNOT KNOW THE FAMILY. Which wrapper this host
  // should reach for is a fact about the persistence package it chose, and core
  // is downstream of none of them. Name the capability and the pattern; the
  // package that owns a family is the one entitled to name its own function.
  throw new Error(
    `${call} needs a log that can hold events that have not happened yet, but this entry's ` +
      "`eventStore` was never wrapped. Wrap it at the composition root in the scheduling " +
      "wrapper for its persistence family — `<family>SchedulingEventStore(store, …)`.",
  )
}

/**
 * Build the scheduling capabilities for ONE invocation, closed over that
 * invocation's unit of work and the SITE of the entry being invoked — its log,
 * which is ALSO its schedule book when the host wrapped one.
 *
 * Internal — handlers reach the results as `ctx.schedule`, `ctx.scheduleAfter`
 * and `ctx.cancelSchedule`.
 *
 * The same one-call ergonomics as `ctx.append`: pass the descriptor and the
 * payload and this builds the {@link EventMessage} — tags, metadata, identifier
 * — and hands it to the log along with the unit of work.
 *
 * The scheduled event's metadata is EXACTLY what the caller passed, the same
 * rule as `ctx.append` and `ctx.send`. A host that wants the triggering
 * message's correlation to reach the event when it eventually fires composes
 * `correlatingHandler(next, from)`, which overlays the task's map through this
 * verb's `metadata` parameter at SCHEDULE time. That is the only point where it
 * could happen: when the schedule fires there is no originating task left to
 * ask.
 */
export function scheduleFunctions(deps: {
  uow: UnitOfWork
  eventStore?: EventStore
}): ScheduleCapability {
  const schedule = (async <P extends StandardSchemaV1>(
    event: EventDescriptor<P>,
    payload: InferOutput<P>,
    at: Date,
    metadata?: Metadata,
  ): Promise<ScheduleToken> => {
    const uow = requireInvocation(deps.uow)
    const log = requireScheduling(deps.eventStore, "ctx.schedule(…)")

    // Reject malformed fire times. A past-but-valid `at` is allowed — it fires
    // ASAP, which is the intended deadline semantic — but an Invalid Date is a
    // caller bug that otherwise behaves inconsistently across families (the
    // in-memory tier fires immediately; the postgres one throws on toISOString
    // at insert time). Fail fast and uniformly here instead.
    if (!(at instanceof Date) || Number.isNaN(at.getTime())) {
      throw new Error(`schedule: \`at\` must be a valid Date, received ${String(at)}`)
    }

    const eventMetadata = metadata ?? emptyMetadata()

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
    return log.schedule(eventMessage, at, uow)
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
   * Attempt to cancel a previously scheduled event. Unit-of-work aware: joins
   * the task's transaction where the family has one, so cancelling and then
   * throwing does not leave the schedule cancelled.
   */
  const cancelSchedule = async (token: ScheduleToken): Promise<CancelResult> => {
    const uow = requireInvocation(deps.uow)
    const log = requireScheduling(deps.eventStore, "ctx.cancelSchedule(…)")
    return log.cancelSchedule(token, uow)
  }

  return { schedule, scheduleAfter, cancelSchedule }
}
