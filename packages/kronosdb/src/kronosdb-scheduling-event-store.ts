/**
 * kronosDbSchedulingEventStore — the SCHEDULING CAPABILITY TIER for the
 * KronosDB family.
 *
 * SCHEDULES ALREADY RIDE THE KRONOSDB LOG, SERVER-SIDE, and that is worth
 * saying out loud because it is the clearest evidence the capability belongs on
 * the store rather than beside it. This wrapper holds no table, no timer and no
 * poller: it hands the event to the same server the log lives in and the server
 * appends it when due. If the server is up, due events land; if it is down,
 * nothing else could have appended either, and overdue schedules fire on
 * recovery. Firing is exactly-once across leader failover and the schedule is
 * durable the moment `schedule()` resolves — quorum-acknowledged like any
 * append.
 *
 * ADDITIVE, like every capability adder: `E` in, `E & ScheduleStoreCapability` out,
 * so a store already wrapped for snapshots still serves them.
 *
 * ```ts
 * const eventStore = kronosDbSchedulingEventStore(
 *   kronosDbSnapshottingEventStore(kronosDbEventStore(kdb, context), kdb, context),
 *   kdb,
 *   { serializer },
 * )
 * ```
 *
 * ── THE ONE PLACE IT DIFFERS FROM THE OTHER TIERS ──────────────────────────
 *
 * IT DOES NOT JOIN THE CALLER'S TRANSACTION. The trailing unit of work is
 * accepted and ignored, because the server owns the schedule the instant it is
 * told and there is no client-side handle to roll back. A handling that arms a
 * schedule and then throws HAS armed it. That was true of the standalone
 * KronosDB scheduler this absorbs, and moving it onto the store changes nothing
 * about it — but it is now visible on the same object as the log, so a reader
 * comparing the three tiers can see which one is the odd family out.
 */
import {
  qualifiedNameToString,
  type Serializer,
} from "@kronos-ts/core"
import type {
  CancelResult,
  EventStore,
  ScheduleStoreCapability,
  ScheduleToken,
  UnitOfWork,
} from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import type { KronosDbConnection } from "./connection.js"
import { KronosDbRequestError } from "./errors.js"
import { writeRecord } from "./native-events.js"
import { contextRequest, unexpected } from "./native-requests.js"
import {
  FrameKind,
  decodeScheduled,
  decodeSchedules,
  reader,
  writeMillis,
  writeStr16,
  writeU16,
} from "./protocol/kronosdb-protocol.js"

/** A schedule that has neither fired nor been cancelled. */
export type PendingSchedule = {
  /** Token identifying the schedule for cancellation. */
  token: string
  /** When the event will be appended, in milliseconds since epoch. */
  dueAt: number
  /** Type name of the event that will be appended. */
  eventName: string
}

export type KronosDbSchedulingOptions = {
  /** How a scheduled event's payload is encoded on the way to the server. */
  readonly serializer: Serializer
}

/** The token is already in use — usually a retried `schedule()` call. */
export class ScheduleAlreadyExistsError extends Error {
  constructor(readonly token: string) {
    super(`A schedule with token '${token}' already exists`)
    this.name = "ScheduleAlreadyExistsError"
  }
}

// `ScheduleAlreadyResolvedError` and `ScheduleNotFoundError` are GONE. They
// were the two cancel outcomes spelled as throws, from a time when this was a
// standalone scheduler with its own vocabulary. `ScheduleStoreCapability.cancelSchedule`
// answers a `CancelResult` — `already-appended`, `not-found`, `cancelled` —
// because those are three pieces of NEWS a caller branches on, not three
// failures, and every family answers in the same three words so a compensating
// handler is written once. The protocol's statuses are translated below.

/**
 * WHAT THE KRONOSDB TIER ADDS BEYOND THE CAPABILITY: the operator's view.
 *
 * `listSchedules` is a read of what the SERVER is holding, which no other
 * family can answer as cheaply — postgres would have to query its table and the
 * in-memory tier has nothing durable to report. It stays out of
 * {@link ScheduleStoreCapability} for exactly that reason: a capability is what every
 * member of the tier can honestly promise.
 */
export type KronosDbSchedulingControl = {
  /** Schedules that have neither fired nor been cancelled, soonest first. */
  listSchedules(): Promise<PendingSchedule[]>
  /**
   * Schedule under a token YOU chose, so a retried call is refused instead of
   * armed twice: the second call throws {@link ScheduleAlreadyExistsError}
   * carrying the token. `schedule` lets the server mint the token because the
   * shared capability cannot promise idempotency; this is the KronosDB tier's
   * own, for the host that needs it. Persist the token to cancel with it.
   */
  scheduleIdempotent(event: EventMessage, at: Date, token: string): Promise<ScheduleToken>
}

/**
 * Add the scheduling capability to a KronosDB event store, served by the same
 * server the log lives in.
 *
 * Scheduling and cancelling must reach the leader node; in a cluster, followers
 * refuse with `unavailable` (a transient error the caller retries) and the
 * connection's failover handles the re-routing. Listing works on any node.
 */
export function kronosDbSchedulingEventStore<E extends EventStore>(
  next: E,
  connection: KronosDbConnection,
  options: KronosDbSchedulingOptions,
  context: string = connection.config.context,
): E & ScheduleStoreCapability & KronosDbSchedulingControl {
  const { serializer } = options
  const link = connection.native

  async function arm(event: EventMessage, at: Date, requested: string): Promise<ScheduleToken> {
    const name = qualifiedNameToString(event.name)
    const data = serializer.serialize(event.payload, name, event.version).data
    const dueMs = at.getTime()

    try {
      const token = await contextRequest(
        link,
        context,
        FrameKind.ScheduleAppend,
        (w, id) => {
          writeU16(w, id)
          writeMillis(w, dueMs)
          // An empty token asks the server to mint one. `schedule` always does:
          // `ScheduleStoreCapability.schedule` has no idempotency key, because
          // two of the three families have no way to honour it.
          writeStr16(w, requested, "schedule token")
          // The event's timestamp is advisory: the server stamps the actual
          // append time when the schedule fires.
          writeRecord(w, event, data)
        },
        (kind, buf, start, end) => {
          if (kind !== FrameKind.Scheduled) throw unexpected(kind, "Scheduled")
          return decodeScheduled(reader(buf, start, end)).token
        },
      )
      return { id: token }
    } catch (error) {
      if (isStatus(error, "already_exists")) throw new ScheduleAlreadyExistsError(requested)
      throw error
    }
  }

  return {
    ...next,

    async schedule(event: EventMessage, at: Date, _uow?: UnitOfWork): Promise<ScheduleToken> {
      return arm(event, at, "")
    },

    scheduleIdempotent(event: EventMessage, at: Date, token: string): Promise<ScheduleToken> {
      return arm(event, at, token)
    },

    async cancelSchedule(token: ScheduleToken, _uow?: UnitOfWork): Promise<CancelResult> {
      // THE THREE OUTCOMES, TRANSLATED. The protocol answers with statuses and
      // the capability answers with news, so the mapping is stated here rather
      // than left to each caller. `rejected` means the schedule has already
      // resolved — fired or cancelled — and the server does not say which;
      // `already-appended` is the safe reading, because it is the one that makes
      // a caller check whether it needs to compensate.
      try {
        await contextRequest(
          link,
          context,
          FrameKind.CancelSchedule,
          (w, id) => {
            writeU16(w, id)
            writeStr16(w, token.id, "schedule token")
          },
          (kind) => {
            if (kind !== FrameKind.ScheduleCancelled) throw unexpected(kind, "ScheduleCancelled")
          },
        )
        return { kind: "cancelled" }
      } catch (error) {
        if (isStatus(error, "rejected")) return { kind: "already-appended" }
        if (isStatus(error, "not_found")) return { kind: "not-found" }
        throw error
      }
    },

    async listSchedules() {
      return contextRequest(
        link,
        context,
        FrameKind.ListSchedules,
        (w, id) => writeU16(w, id),
        (kind, buf, start, end) => {
          if (kind !== FrameKind.Schedules) throw unexpected(kind, "Schedules")
          return decodeSchedules(reader(buf, start, end)).schedules.map((schedule) => ({
            token: schedule.token,
            dueAt: schedule.due,
            eventName: schedule.eventName,
          }))
        },
      )
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred; the probe keeps it honest.
  } as E & ScheduleStoreCapability & KronosDbSchedulingControl
}

function isStatus(error: unknown, status: string): boolean {
  return error instanceof KronosDbRequestError && error.status === status
}
