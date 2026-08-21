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
 * ADDITIVE, like every capability adder: `E` in, `E & ScheduleCapability` out,
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
  ScheduleCapability,
  ScheduleToken,
  UnitOfWork,
} from "@kronos-ts/core"
import type { EventMessage } from "@kronos-ts/core"
import { Status } from "nice-grpc"
import type { KronosDbConnection } from "./connection.js"
import { kronosMetadata } from "./connection.js"
import { metadataToStringMap } from "./metadata-conversion.js"

const textEncoder = new TextEncoder()

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
// standalone scheduler with its own vocabulary. `ScheduleCapability.cancelSchedule`
// answers a `CancelResult` — `already-appended`, `not-found`, `cancelled` —
// because those are three pieces of NEWS a caller branches on, not three
// failures, and every family answers in the same three words so a compensating
// handler is written once. The gRPC statuses are translated below.

/**
 * WHAT THE KRONOSDB TIER ADDS BEYOND THE CAPABILITY: the operator's view.
 *
 * `listSchedules` is a read of what the SERVER is holding, which no other
 * family can answer as cheaply — postgres would have to query its table and the
 * in-memory tier has nothing durable to report. It stays out of
 * {@link ScheduleCapability} for exactly that reason: a capability is what every
 * member of the tier can honestly promise.
 */
export type KronosDbSchedulingControl = {
  /** Schedules that have neither fired nor been cancelled, soonest first. */
  listSchedules(): Promise<PendingSchedule[]>
}

/**
 * Add the scheduling capability to a KronosDB event store, served by the same
 * server the log lives in.
 *
 * Scheduling and cancelling must reach the leader node; in a cluster, followers
 * refuse with FAILED_PRECONDITION and the connection's failover handles the
 * re-routing. Listing works on any node.
 */
export function kronosDbSchedulingEventStore<E extends EventStore>(
  next: E,
  connection: KronosDbConnection,
  options: KronosDbSchedulingOptions,
): E & ScheduleCapability & KronosDbSchedulingControl {
  const { serializer } = options

  function getMetadata() {
    return kronosMetadata(connection.config)
  }

  return {
    ...next,

    async schedule(event: EventMessage, at: Date, _uow?: UnitOfWork): Promise<ScheduleToken> {
      const name = qualifiedNameToString(event.name)
      const serialized = serializer.serialize(event.payload, name, event.version)
      const dueMs = at.getTime()

      try {
        const response = await connection.scheduler.scheduleAppend(
          {
            dueMs: BigInt(dueMs),
            // THE SERVER MINTS THE TOKEN. The standalone scheduler let a caller
            // supply one as an idempotency key; `ScheduleCapability.schedule`
            // has no such parameter, because two of the three families have no
            // way to honour it and a capability is what all of them can promise.
            // A host that needs KronosDB's idempotent retry reaches the
            // connection's scheduler service directly.
            token: "",
            event: {
              event: {
                identifier: event.identifier,
                // Advisory: the server stamps the actual append time when
                // the schedule fires.
                timestamp: BigInt(event.timestamp),
                name,
                version: event.version,
                payload: serialized.data,
                metadata: metadataToStringMap(event.metadata),
              },
              tags: event.tags.map((tag) => ({
                key: textEncoder.encode(tag.key),
                value: textEncoder.encode(tag.value),
              })),
            },
          },
          { metadata: getMetadata() },
        )
        return { id: response.token }
      } catch (error) {
        if (isGrpcStatus(error, Status.ALREADY_EXISTS)) {
          throw new ScheduleAlreadyExistsError("")
        }
        throw error
      }
    },

    async cancelSchedule(token: ScheduleToken, _uow?: UnitOfWork): Promise<CancelResult> {
      // THE THREE OUTCOMES, TRANSLATED. The service answers with gRPC statuses
      // and the capability answers with news, so the mapping is stated here
      // rather than left to each caller. FAILED_PRECONDITION means the schedule
      // has already resolved — fired or cancelled — and the server does not say
      // which; `already-appended` is the safe reading, because it is the one
      // that makes a caller check whether it needs to compensate.
      try {
        await connection.scheduler.cancelSchedule({ token: token.id }, { metadata: getMetadata() })
        return { kind: "cancelled" }
      } catch (error) {
        if (isGrpcStatus(error, Status.FAILED_PRECONDITION)) return { kind: "already-appended" }
        if (isGrpcStatus(error, Status.NOT_FOUND)) return { kind: "not-found" }
        throw error
      }
    },

    async listSchedules() {
      const response = await connection.scheduler.listSchedules({}, { metadata: getMetadata() })
      return response.schedules.map((schedule) => ({
        token: schedule.token,
        dueAt: Number(schedule.dueMs),
        eventName: schedule.eventName,
      }))
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred; the probe keeps it honest.
  } as E & ScheduleCapability & KronosDbSchedulingControl
}

function isGrpcStatus(error: unknown, status: Status): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === status
}
