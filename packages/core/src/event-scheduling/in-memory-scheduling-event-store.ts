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

import type { TaggedEvent } from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { CancelResult, ScheduleStoreCapability, ScheduleToken } from "./scheduler.js"
import { NoActiveUnitOfWork, Phase, requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { generateIdentifier } from "../messaging/identifier.js"

type RecordStatus = "pending" | "appended" | "cancelled"

type ScheduleRecord = {
  status: RecordStatus
  event: TaggedEvent
  fireAt: number
  /** Whether the task that scheduled it has committed. Only then does it have a timer. */
  committed: boolean
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
 * Not part of {@link ScheduleStoreCapability}, because "cancel every timer you are
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
 *   back task schedules nothing. `onError` drops the staged record, cancelled
 *   in that task or not, so a post-rollback cancel answers `not-found`.
 *
 * - `cancelSchedule()` may be called inside or outside a task. The state change
 *   is applied immediately for caller visibility. Handed a unit of work that
 *   later fails, the cancel is undone: the schedule is pending again and its
 *   timer is armed again for its fire time, or for now when that has passed.
 *   Called without a unit of work, the cancel is final at once.
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
): E & ScheduleStoreCapability & InMemorySchedulingControl {
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

    async schedule(event: TaggedEvent, at: Date, uow?: UnitOfWork): Promise<ScheduleToken> {
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
        committed: false,
      }
      records.set(id, record)

      uow.onAfterCommit(() => {
        const rec = records.get(id)
        if (!rec) return
        rec.committed = true
        if (rec.status === "pending") armTimer(id, rec)
      })

      uow.onError(() => {
        // Roll the staged record back, cancelled in this task or not, so a
        // post-rollback cancel sees `not-found`.
        const rec = records.get(id)
        if (rec && !rec.committed) records.delete(id)
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

      // Task participation: when the task later fails, the cancel is undone.
      // The schedule is pending again, and a schedule whose own task had
      // committed gets its timer back. One still staged by this same task has
      // no timer to restore; its own `onError` drops the record. A task that
      // fails after its commit has committed, and the cancel stands.
      if (uow !== undefined && !uow.closed) {
        uow.onError((_error, failedPhase) => {
          if (failedPhase === Phase.AFTER_COMMIT) return
          const r = records.get(token.id)
          if (!r || r.status !== "cancelled") return
          r.status = "pending"
          if (r.committed) armTimer(token.id, r)
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
  } as E & ScheduleStoreCapability & InMemorySchedulingControl
}
