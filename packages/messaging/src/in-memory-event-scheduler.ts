/**
 * In-memory {@link EventScheduler} — intended for tests only.
 *
 * Backed by a `Map<scheduleId, record>` and `setTimeout`. Publishes fired
 * events through a supplied {@link EventSink} (typically the in-memory
 * event bus or a test spy).
 *
 * # UoW semantics (best-effort, test-grade)
 *
 * - `schedule()` must be called inside a UoW (INVOCATION phase). The
 *   record is staged immediately so cancel() inside the SAME UoW sees it,
 *   but the `setTimeout` arming is deferred to AFTER_COMMIT — so if the
 *   UoW rolls back the schedule never fires. `onError` cleans the staged
 *   record so callers see `not-found` on the rolled-back token.
 *
 * - `cancel()` may be called inside or outside a UoW. State change is
 *   applied immediately for caller-visibility; this means a UoW that
 *   cancels and then rolls back does NOT restore the schedule. This
 *   differs from the postgres implementation (which is true
 *   transactional) and is acceptable for the in-memory's test-only
 *   remit. Document this when writing tests that depend on cancel
 *   rollback semantics — use the postgres scheduler for that.
 *
 * # NOT production
 *
 * No persistence, no recovery on restart, no at-least-once. A test-only
 * spy with a real-enough surface to exercise framework wiring.
 */

import type { EventMessage } from "./message.js"
import type { EventSink } from "./event-sink.js"
import type { EventScheduler, ScheduleToken, CancelResult } from "./event-scheduler.js"
import {
  requireInvocationPhase,
  onAfterCommit,
  onError,
  processingStateStorage,
} from "./processing-state.js"
import { generateIdentifier } from "@kronos-ts/common"

type RecordStatus = "pending" | "appended" | "cancelled"

interface ScheduleRecord {
  status: RecordStatus
  event: EventMessage
  fireAt: number
  timer?: ReturnType<typeof setTimeout>
}

export interface InMemoryEventSchedulerOptions {
  readonly eventSink: EventSink
  /** Override `Date.now` for deterministic tests. Defaults to `Date.now`. */
  readonly now?: () => number
}

export interface InMemoryEventScheduler extends EventScheduler {
  /**
   * Cancel any armed timers and drop all internal state. Tests call this
   * in `afterEach` to ensure schedulers from one test do not fire into
   * another. Not part of the public {@link EventScheduler} contract.
   */
  stop(): Promise<void>
}

export function inMemoryEventScheduler(
  options: InMemoryEventSchedulerOptions,
): InMemoryEventScheduler {
  const { eventSink } = options
  const now = options.now ?? Date.now
  const records = new Map<string, ScheduleRecord>()

  function armTimer(id: string, record: ScheduleRecord): void {
    const delay = Math.max(0, record.fireAt - now())
    record.timer = setTimeout(() => {
      const rec = records.get(id)
      if (!rec || rec.status !== "pending") return
      rec.status = "appended"
      rec.timer = undefined
      eventSink.publish([rec.event]).catch((err) => {
        // Test-only: surface but do not crash the process. Real
        // implementations need an at-least-once retry; not modelled here.
        console.warn("inMemoryEventScheduler: publish failed:", err)
      })
    }, delay)
  }

  return {
    async schedule(event: EventMessage, at: Date): Promise<ScheduleToken> {
      requireInvocationPhase()

      const id = generateIdentifier()
      const record: ScheduleRecord = {
        status: "pending",
        event,
        fireAt: at.getTime(),
      }
      records.set(id, record)

      onAfterCommit(() => {
        const rec = records.get(id)
        if (!rec || rec.status !== "pending") return
        armTimer(id, rec)
      })

      onError(() => {
        // Roll back the staged record so post-rollback cancel() sees
        // `not-found` rather than `cancelled`.
        const rec = records.get(id)
        if (rec && rec.status === "pending") records.delete(id)
      })

      return { id }
    },

    async cancel(token: ScheduleToken): Promise<CancelResult> {
      const rec = records.get(token.id)
      if (!rec) return { kind: "not-found" }
      if (rec.status === "appended") return { kind: "already-appended" }
      if (rec.status === "cancelled") return { kind: "not-found" }

      rec.status = "cancelled"
      if (rec.timer !== undefined) {
        clearTimeout(rec.timer)
        rec.timer = undefined
      }

      // Best-effort UoW participation: if we're inside a UoW that later
      // errors, revert the cancel so the schedule's pending state
      // re-materialises. The original timer (if it was armed) has already
      // been cleared — the AFTER_COMMIT re-arm cycle is not re-driven
      // here, which means a cancel + rollback inside a post-commit window
      // would not re-fire. Acceptable for the in-memory's test-only remit.
      if (processingStateStorage.getStore() !== undefined) {
        onError(() => {
          const r = records.get(token.id)
          if (r && r.status === "cancelled") r.status = "pending"
        })
      }

      return { kind: "cancelled" }
    },

    async stop(): Promise<void> {
      for (const rec of records.values()) {
        if (rec.timer !== undefined) clearTimeout(rec.timer)
      }
      records.clear()
    },
  }
}
