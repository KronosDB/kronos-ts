import { generateIdentifier, requireInvocation, NoActiveUnitOfWork } from "@kronos-ts/core"
import type {
  AppendCondition,
  AppendTransaction,
  CancelResult,
  CommandBus,
  CommandMessage,
  ConsistencyMarker,
  EventMessage,
  EventStore,
  QueryBus,
  QueryMessage,
  ScheduleCapability,
  ScheduleToken,
  SubscriptionFilter,
  SubscriptionQueryResult,
  UnitOfWork,
} from "@kronos-ts/core"

// ---------------------------------------------------------------------------
// Recorders: thing-first decorators.
//
// Each takes the thing it records and returns THE SAME SHAPE plus a readable
// log. That is what makes them composable in either direction and usable
// outside the fixture: `recordingEventStore(postgresEventStore(pg))` records a
// real store, and `recordingCommandBus(rabbitMqCommandBus(local, rabbit))`
// records what actually left. Nothing here decides what the recorded thing IS.
//
// `reset()` is on each of them because a timeline is longer than one act: a
// fixture marks the boundary between "history" and "what this act did" by
// clearing the log, and the alternative — handing every caller an index to
// slice from — makes every caller responsible for arithmetic that has exactly
// one right answer.
// ---------------------------------------------------------------------------

/**
 * An event store that remembers, in order, every event COMMITTED through it.
 *
 * Both write paths are recorded, and both at commit: the auto-committing
 * `append` (what a command handler's unit of work flushes through) and the
 * two-phase `appendEvents`, recorded when its transaction commits and never
 * when it rolls back.
 */
export type RecordingEventStore = EventStore & EventRecording

/** WHAT RECORDING ADDS, named on its own so the wrapper can be ADDITIVE. */
export type EventRecording = {
  /** Every event committed through this store since the last reset, oldest first. */
  readonly appended: ReadonlyArray<EventMessage>
  /** Forget the log. The STORE keeps its events; only the recording is cleared. */
  reset(): void
}

/**
 * Record what is committed through `store`.
 *
 * ADDITIVE: `E & EventRecording`, never a collapse to `RecordingEventStore`.
 * The fixture composes `recordingEventStore(inMemorySnapshottingEventStore(log))`
 * — recorder OUTERMOST, so `appended` is what left the fixture — and a
 * signature that returned the bare intersection would have thrown the
 * snapshotting capability away one layer above the store that has it. Every
 * scope loading a snapshotting state would then fail to compile against a
 * fixture that serves it perfectly well.
 */
export function recordingEventStore<E extends EventStore>(store: E): E & EventRecording {
  const log: EventMessage[] = []

  return {
    ...store,

    get appended(): ReadonlyArray<EventMessage> {
      return log
    },

    reset(): void {
      log.length = 0
    },

    async append(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
      uow?: UnitOfWork,
    ): Promise<ConsistencyMarker> {
      const marker = await store.append(events, condition, uow)
      log.push(...events)
      return marker
    },

    async appendEvents(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
      uow?: UnitOfWork,
    ): Promise<AppendTransaction> {
      const transaction = await store.appendEvents(events, condition, uow)
      return {
        ...transaction,
        async commit(): Promise<void> {
          await transaction.commit()
          log.push(...events)
        },
      }
    },
  } as E & EventRecording
}

/**
 * A command bus that remembers every message dispatched through it, in order.
 *
 * Recorded at ENTRY, before the handler runs — so a command and the commands its
 * handler dispatches appear in causal order, which is the order a reader expects
 * and the order an automation test asserts.
 *
 * The recorded messages may carry no `timestamp`, because that is what a bus is
 * handed: the edge verb builds the message and the bus behind this one fills
 * the instant in from the task it mints. A recorder that pretended otherwise
 * would be reporting a field it never saw.
 */
export type RecordingCommandBus<U extends UnitOfWork = UnitOfWork> = CommandBus<U> &
  CommandRecording

/** WHAT RECORDING ADDS, named on its own so the wrapper can be ADDITIVE. */
export type CommandRecording = {
  readonly dispatched: ReadonlyArray<CommandMessage>
  reset(): void
}

/**
 * Record what is dispatched through `bus`. Whatever `bus` mints, this mints —
 * and whatever `bus` could do, this can, because the wrapper is `B & CommandRecording`
 * over a spread rather than a collapse to the base seam.
 */
export function recordingCommandBus<B extends CommandBus<any>>(
  bus: B,
): B & CommandRecording {
  const log: Array<CommandMessage> = []

  return {
    ...bus,

    get dispatched(): ReadonlyArray<CommandMessage> {
      return log
    },

    reset(): void {
      log.length = 0
    },

    async dispatch(message: CommandMessage): Promise<unknown> {
      log.push(message)
      return bus.dispatch(message)
    },

    subscribe(commandName: string, handler: unknown): void {
      bus.subscribe(commandName, handler as never)
    },
  } as B & CommandRecording
}

/** A query bus that remembers every message asked through it, in order. */
export type RecordingQueryBus<U extends UnitOfWork = UnitOfWork> = QueryBus<U> & QueryRecording

/** WHAT RECORDING ADDS, named on its own so the wrapper can be ADDITIVE. */
export type QueryRecording = {
  readonly queried: ReadonlyArray<QueryMessage>
  reset(): void
}

/** Record what is asked through `bus`. Whatever `bus` mints, this mints. */
export function recordingQueryBus<B extends QueryBus<any>>(
  bus: B,
): B & QueryRecording {
  const log: Array<QueryMessage> = []

  return {
    ...bus,

    get queried(): ReadonlyArray<QueryMessage> {
      return log
    },

    reset(): void {
      log.length = 0
    },

    async query(message: QueryMessage, uow?: UnitOfWork): Promise<unknown> {
      log.push(message)
      return bus.query(message, uow)
    },

    subscribe(queryName: string, handler: unknown): void {
      bus.subscribe(queryName, handler as never)
    },

    subscriptionQuery(
      message: QueryMessage,
      bufferSize?: number,
    ): SubscriptionQueryResult {
      log.push(message)
      return bus.subscriptionQuery(message, bufferSize)
    },

    subscribeToUpdates(
      message: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      log.push(message)
      return bus.subscribeToUpdates(message, bufferSize)
    },

    emitUpdate(
      queryName: string,
      filter: SubscriptionFilter,
      update: unknown,
      uow?: UnitOfWork,
    ): Promise<void> {
      return bus.emitUpdate(queryName, filter, update, uow)
    },

    completeSubscription(
      queryName: string,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return bus.completeSubscription(queryName, filter, uow)
    },

    completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: SubscriptionFilter,
      uow?: UnitOfWork,
    ): Promise<void> {
      return bus.completeSubscriptionExceptionally(queryName, error, filter, uow)
    },
  } as B & QueryRecording
}

// ── the controllable SCHEDULING TIER ───────────────────────────────────────

/** One armed, fired or cancelled schedule, as a reader sees it. */
export type ScheduleRecord = {
  readonly token: ScheduleToken
  /** The event this schedule will append when it fires. */
  readonly event: EventMessage
  /** The clock instant the handler armed it at. */
  readonly armedAt: number
  /** The clock instant it fires at. `fireAt - armedAt` is the delay asked for. */
  readonly fireAt: number
  readonly status: "pending" | "fired" | "cancelled"
}

/**
 * WHAT THE CONTROLLABLE TIER ADDS BEYOND THE CAPABILITY, named on its own so
 * the wrapper can be ADDITIVE.
 *
 * `resetSchedules` rather than `reset`, because this tier composes UNDER
 * `recordingEventStore` and that one already owns a `reset`. Two different
 * resets under one name on one object was a collision waiting to be found by
 * somebody debugging at midnight; the recorder's `reset` clears the event log
 * and this clears the schedule book, and now you can say which you meant.
 */
export type ScheduleRecording = {
  /** Every schedule this store has seen since the last reset, oldest first. */
  readonly schedules: ReadonlyArray<ScheduleRecord>
  /**
   * The events whose fire-time has arrived at the clock's current instant, in
   * FIRE-TIME order, marked fired. Calling it twice does not fire anything twice.
   */
  due(): ReadonlyArray<EventMessage>
  /** Forget every schedule. The LOG keeps its events; only the book is cleared. */
  resetSchedules(): void
}

/**
 * A scheduling capability tier with NO TIMER: nothing fires until somebody
 * moves the clock and asks what is due.
 *
 * That is the whole difference from `inMemorySchedulingEventStore`, and it is
 * the difference between a deadline test that takes thirty seconds and one that
 * takes a millisecond. It does not append the fired events either: {@link
 * ScheduleRecording.due} HANDS THEM BACK, and whoever drives the clock decides
 * where they go — in a fixture, in through the OUTERMOST store, so the recorder
 * above this one sees them exactly as it sees a handler's append.
 *
 * ADDITIVE: `E & ScheduleCapability & ScheduleRecording`, never a collapse. The
 * fixture composes it over the snapshotting tier and under the recorder, and a
 * signature that returned a bare intersection would throw one of the other two
 * capabilities away on the way through.
 *
 * Unit-of-work semantics match the in-memory tier: a schedule is staged during
 * INVOCATION so a `cancelSchedule` in the same task can see it, becomes
 * DUE-able only at AFTER_COMMIT, and is dropped if the task fails.
 *
 * One thing it does NOT copy: a fired event is stamped with the instant it
 * FIRES, not the instant somebody arranged it. The arrangement is not the event —
 * a reader who jumped the clock thirty seconds and got back an event claiming to
 * have happened thirty seconds ago would be reading a lie, and everything the
 * fired event then causes is stamped from the fire instant anyway.
 */
export function controllableSchedulingEventStore<E extends EventStore>(
  next: E,
  clock: () => number,
): E & ScheduleCapability & ScheduleRecording {
  type Entry = {
    record: ScheduleRecord
    armed: boolean
  }
  const entries: Entry[] = []

  function find(id: string): Entry | undefined {
    return entries.find((e) => e.record.token.id === id)
  }

  return {
    ...next,

    get schedules(): ReadonlyArray<ScheduleRecord> {
      return entries.map((e) => e.record)
    },

    resetSchedules(): void {
      entries.length = 0
    },

    due(): ReadonlyArray<EventMessage> {
      const now = clock()
      // Fire-time order, not arming order: two deadlines arranged in one order
      // and due in another happen in the order they are DUE, which is the only
      // order a reader can predict from the scenario.
      const ready = entries
        .filter((e) => e.armed && e.record.status === "pending" && e.record.fireAt <= now)
        .sort((a, b) => a.record.fireAt - b.record.fireAt)
      const fired: EventMessage[] = []
      for (const entry of ready) {
        entry.record = { ...entry.record, status: "fired" }
        fired.push({ ...entry.record.event, timestamp: now })
      }
      return fired
    },

    async schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken> {
      if (uow === undefined) {
        throw new NoActiveUnitOfWork(
          "controllableSchedulingEventStore.schedule requires a UnitOfWork — call it as ctx.schedule from inside a handler",
        )
      }
      requireInvocation(uow)

      const token: ScheduleToken = { id: generateIdentifier() }
      const entry: Entry = {
        armed: false,
        record: {
          token,
          event,
          armedAt: clock(),
          fireAt: at.getTime(),
          status: "pending",
        },
      }
      entries.push(entry)

      // Arm at AFTER_COMMIT and drop on failure — a schedule a rolled-back
      // handler asked for was never asked for.
      uow.onAfterCommit(() => {
        entry.armed = true
      })
      uow.onError(() => {
        const index = entries.indexOf(entry)
        if (index >= 0 && entry.record.status === "pending") entries.splice(index, 1)
      })

      return token
    },

    async cancelSchedule(token: ScheduleToken, uow?: UnitOfWork): Promise<CancelResult> {
      const entry = find(token.id)
      if (!entry) return { kind: "not-found" }
      if (entry.record.status === "fired") return { kind: "already-appended" }
      if (entry.record.status === "cancelled") return { kind: "not-found" }

      entry.record = { ...entry.record, status: "cancelled" }
      if (uow !== undefined && !uow.closed) {
        uow.onError(() => {
          if (entry.record.status === "cancelled") {
            entry.record = { ...entry.record, status: "pending" }
          }
        })
      }
      return { kind: "cancelled" }
    },
    // The spread of a generic is opaque to the checker, so the shape it
    // produces is asserted rather than inferred; the probe keeps it honest.
  } as E & ScheduleCapability & ScheduleRecording
}
