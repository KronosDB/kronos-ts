// ---------------------------------------------------------------------------
// IN-MEMORY SCHEDULING — the capability tier's null implementation, and what
// every fixture's production sibling runs on.
//
// A `Map` and a `setTimeout`. Nothing about it is clever, which is the point:
// if the capability needed cleverness to be implementable in memory it would be
// the wrong capability.
//
// WHAT CHANGED WHEN IT BECAME A TIER: it used to be handed an `eventSink`, and
// a host had to remember that the sink and the entry's `eventStore` were meant
// to be the same log. THE WRAPPED STORE IS THE SINK NOW. That is not a
// convenience — it is the reason the tier exists. A schedule fires INTO a log,
// and the only log it can honestly fire into is the one the handling that armed
// it reads and writes.
// ---------------------------------------------------------------------------

import type { EventMessage } from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { CancelResult, ScheduleCapability, ScheduleToken } from "./scheduler.js"
import { NoActiveUnitOfWork, requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { generateIdentifier } from "../messaging/identifier.js"

type RecordStatus = "pending" | "appended" | "cancelled"

type ScheduleRecord = {
  status: RecordStatus
  event: EventMessage
  fireAt: number
  timer?: ReturnType<typeof setTimeout>
}

export type InMemorySchedulingOptions = {
  /**
   * Where "now" comes from when a fire time is turned into a delay. Absent means
   * system time — `Date.now` IS one, so passing nothing and passing it say the
   * same thing.
   */
  readonly clock?: () => number
}

/**
 * WHAT THE IN-MEMORY TIER ADDS BEYOND THE CAPABILITY: a way to disarm.
 *
 * Not part of {@link ScheduleCapability}, because "cancel every timer you are
 * holding" is a property of a tier that HOLDS timers, and the postgres tier
 * holds a poller instead. Tests call it in `afterEach` so a schedule armed by
 * one test cannot fire into the next.
 */
export type InMemorySchedulingControl = {
  stopScheduling(): Promise<void>
}

/**
 * Add the scheduling capability to any event store, served from memory.
 *
 * THING FIRST, capability second — the same order every wrapper here uses. What
 * comes back is the store you passed in, PLUS the two scheduling members, so
 * nothing the inner store carried is laundered on the way through: wrap a
 * snapshotting store and the result still caches folds, wrap this in an
 * upcasting store and the result can still schedule. Every order typechecks,
 * and the orders are pinned by the type probe.
 *
 * ```ts
 * const eventStore = inMemorySchedulingEventStore(
 *   inMemorySnapshottingEventStore(inMemoryEventStore()),
 * )
 * ```
 *
 * NO READ PATH CHANGES AT ALL, which is what makes this tier different from the
 * snapshotting one: a snapshot narrows what `source()` hands back, a schedule
 * adds a WRITE that happens later. Everything on the inner store delegates
 * untouched, and when a timer fires the event goes in through `append` like any
 * other fact — because that is what it is by then.
 *
 * ── UNIT-OF-WORK SEMANTICS (best-effort, test-grade) ───────────────────────
 *
 * - `schedule()` must be handed a unit of work in its INVOCATION phase. The
 *   record is staged immediately so a `cancelSchedule()` in the SAME task sees
 *   it, but the `setTimeout` arming is deferred to AFTER_COMMIT — so a rolled
 *   back task schedules nothing. `onError` drops the staged record, so a
 *   post-rollback cancel answers `not-found` rather than `cancelled`.
 *
 * - `cancelSchedule()` may be called inside or outside a task. The state change
 *   is applied immediately for caller visibility, which means a task that
 *   cancels and then rolls back does NOT restore the schedule's timer. That
 *   differs from the postgres tier (genuinely transactional) and is acceptable
 *   for an in-memory remit. Write tests that depend on cancel-rollback
 *   semantics against postgres.
 *
 * ── NOT PRODUCTION ─────────────────────────────────────────────────────────
 *
 * No persistence, no recovery on restart, no at-least-once. A real-enough
 * surface to exercise framework wiring, and a process restart forgets every
 * deadline it was holding.
 */
export function inMemorySchedulingEventStore<E extends EventStore>(
  next: E,
  options: InMemorySchedulingOptions = {},
): E & ScheduleCapability & InMemorySchedulingControl {
  const now = options.clock ?? Date.now
  const records = new Map<string, ScheduleRecord>()

  function armTimer(id: string, record: ScheduleRecord): void {
    const delay = Math.max(0, record.fireAt - now())
    record.timer = setTimeout(() => {
      const rec = records.get(id)
      if (!rec || rec.status !== "pending") return
      rec.status = "appended"
      rec.timer = undefined
      // THE WRAPPED STORE IS THE SINK. No second resource, nothing to keep in
      // agreement with anything, and the event lands in exactly the log the
      // handling that armed the schedule was reading.
      next.append([rec.event]).catch((err) => {
        // Test-grade: surface but do not crash the process. A real
        // implementation needs at-least-once retry; not modelled here.
        console.warn("inMemorySchedulingEventStore: append failed:", err)
      })
    }, delay)
    record.timer.unref?.()
  }

  return {
    ...next,

    async schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken> {
      if (uow === undefined) {
        throw new NoActiveUnitOfWork(
          "inMemorySchedulingEventStore.schedule requires a UnitOfWork — call it as ctx.schedule from inside a handler",
        )
      }
      requireInvocation(uow)

      const id = generateIdentifier()
      const record: ScheduleRecord = {
        status: "pending",
        event,
        fireAt: at.getTime(),
      }
      records.set(id, record)

      uow.onAfterCommit(() => {
        const rec = records.get(id)
        if (!rec || rec.status !== "pending") return
        armTimer(id, rec)
      })

      uow.onError(() => {
        // Roll the staged record back so a post-rollback cancel sees
        // `not-found` rather than `cancelled`.
        const rec = records.get(id)
        if (rec && rec.status === "pending") records.delete(id)
      })

      return { id }
    },

    async cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult> {
      const rec = records.get(token.id)
      if (!rec) return { kind: "not-found" }
      if (rec.status === "appended") return { kind: "already-appended" }
      if (rec.status === "cancelled") return { kind: "not-found" }

      rec.status = "cancelled"
      if (rec.timer !== undefined) {
        clearTimeout(rec.timer)
        rec.timer = undefined
      }

      // Best-effort task participation: inside a task that later errors, revert
      // the cancel so the schedule's pending state re-materialises. The original
      // timer (if armed) has already been cleared and the AFTER_COMMIT re-arm
      // cycle is not re-driven, so a cancel + rollback in a post-commit window
      // would not re-fire. Acceptable for an in-memory remit.
      if (uow !== undefined && !uow.closed) {
        uow.onError(() => {
          const r = records.get(token.id)
          if (r && r.status === "cancelled") r.status = "pending"
        })
      }

      return { kind: "cancelled" }
    },

    async stopScheduling(): Promise<void> {
      for (const rec of records.values()) {
        if (rec.timer !== undefined) clearTimeout(rec.timer)
      }
      records.clear()
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred. The probe is what makes the
    // assertion honest: it pins that the result still satisfies BOTH the
    // capability and whatever `E` was.
  } as E & ScheduleCapability & InMemorySchedulingControl
}
