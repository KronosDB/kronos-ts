import { generateIdentifier, requireInvocation, NoActiveUnitOfWork } from "@kronos-ts/core"
import type { AdvanceableClock, Duration } from "./clock.js"
import type {
  SubscriptionBusCapability,
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
  ScheduleStoreCapability,
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

/** One armed, fired or cancelled deadline, as a reader sees it. */
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
 * WHAT A CLOCK ADDS to the recorder: the deadlines it holds, and the verb that
 * moves time over them. Named on its own so the wrapper can be ADDITIVE, and so
 * a fixture can tell from the TYPE whether the app's log can be advanced.
 */
export type ScheduleRecording = {
  /** The clock the deadlines are held against — the one given to the recorder. */
  readonly clock: AdvanceableClock
  /** Every deadline armed through this store, oldest first, with what became of it. */
  readonly schedules: ReadonlyArray<ScheduleRecord>
  /**
   * Move the clock forward, then append through this store every held deadline
   * the clock has reached — in FIRE-TIME order, each stamped with the instant it
   * fired — so the recording sees them exactly as it sees a handler's append.
   */
  advance(duration: Duration): Promise<void>
  /** Forget every held deadline, pending or not. The LOG keeps its events; only the book is cleared. */
  resetSchedules(): void
}

/**
 * Record what is committed through `store`.
 *
 * ADDITIVE: `E & EventRecording`, never a collapse to `RecordingEventStore`.
 * `recordingEventStore(inMemorySnapshottingEventStore(log))` still serves a
 * cached state, and a signature that returned the bare intersection would have
 * thrown that capability away one layer above the store that has it.
 *
 * WITH A CLOCK it also holds the deadlines: `schedule` and `cancelSchedule` keep
 * them in memory, nothing fires on a timer, and {@link ScheduleRecording.advance}
 * moves the clock and appends what is due through `store`. The LOG stays the
 * real one — in memory, Postgres or KronosDB — and only the deadline book is
 * held here, in place of any scheduling tier `store` has of its own:
 *
 * ```ts
 * const clock = advanceableClock()
 * const eventStore = recordingEventStore(postgresEventStore(pg), { clock })
 * ```
 *
 * A deadline is staged while its handling runs (so a `cancelSchedule` in the
 * same task finds it), becomes due-able when the task commits, and is dropped
 * if the task fails. A fired event is stamped with the instant it FIRES, not
 * the instant it was arranged: the arrangement is not the event, and
 * everything the fired event causes is stamped from the fire instant anyway.
 */
export function recordingEventStore<E extends EventStore>(store: E): E & EventRecording
export function recordingEventStore<E extends EventStore>(
  store: E,
  options: { readonly clock: AdvanceableClock },
): E & EventRecording & ScheduleStoreCapability & ScheduleRecording
export function recordingEventStore<E extends EventStore>(
  store: E,
  options?: { readonly clock: AdvanceableClock },
): E & EventRecording & Partial<ScheduleStoreCapability & ScheduleRecording> {
  const log: EventMessage[] = []
  const scheduling = store as E & Partial<ScheduleStoreCapability>

  const recorded = {
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
  }

  // The scheduling tier: held in memory against the clock when there is one,
  // passed through to `store` when it has a tier of its own, absent otherwise.
  const tier: Partial<ScheduleStoreCapability & ScheduleRecording> = options
    ? heldDeadlines(options.clock, (events) => recorded.append(events).then(() => undefined))
    : typeof scheduling.schedule === "function"
      ? { schedule: scheduling.schedule, cancelSchedule: scheduling.cancelSchedule! }
      : {}

  // DESCRIPTORS, not a spread: a spread reads every getter once and freezes the
  // value, and a tier under this one may expose a live view that has to stay one.
  const own = {
    get appended(): ReadonlyArray<EventMessage> {
      return log
    },

    reset(): void {
      log.length = 0
    },

    ...recorded,

    ...(tier.schedule !== undefined ? { schedule: tier.schedule, cancelSchedule: tier.cancelSchedule! } : {}),
  }
  return Object.defineProperties(
    {},
    {
      ...Object.getOwnPropertyDescriptors(store),
      ...Object.getOwnPropertyDescriptors(own),
      ...(tier.advance !== undefined ? Object.getOwnPropertyDescriptors(pick(tier, ["clock", "schedules", "advance", "resetSchedules"])) : {}),
    },
  ) as E & EventRecording & Partial<ScheduleStoreCapability & ScheduleRecording>
}

/** Copy the named properties as DESCRIPTORS, so a getter stays a getter. */
function pick<T extends object>(source: T, keys: ReadonlyArray<keyof T>): Partial<T> {
  const target = {}
  for (const key of keys) {
    const descriptor = Object.getOwnPropertyDescriptor(source, key)
    if (descriptor) Object.defineProperty(target, key, descriptor)
  }
  return target as Partial<T>
}

/**
 * The deadline book a clocked recorder holds: no timer, nothing fires until the
 * clock is moved through {@link ScheduleRecording.advance}, and what fires goes
 * to `fire` — the recorder's own append, so it is recorded like any other.
 */
function heldDeadlines(
  clock: AdvanceableClock,
  fire: (events: ReadonlyArray<EventMessage>) => Promise<void>,
): ScheduleStoreCapability & ScheduleRecording {
  type Entry = { record: ScheduleRecord; armed: boolean }
  const entries: Entry[] = []

  return {
    clock,

    get schedules(): ReadonlyArray<ScheduleRecord> {
      return entries.map((e) => e.record)
    },

    resetSchedules(): void {
      entries.length = 0
    },

    async advance(duration: Duration): Promise<void> {
      clock.advance(duration)
      const now = clock()
      // Fire-time order, not arming order: two deadlines arranged in one order
      // and due in another happen in the order they are DUE, which is the only
      // order a reader can predict from the scenario.
      const ready = entries
        .filter((e) => e.armed && e.record.status === "pending" && e.record.fireAt <= now)
        .sort((a, b) => a.record.fireAt - b.record.fireAt)
      if (ready.length === 0) return
      for (const entry of ready) entry.record = { ...entry.record, status: "fired" }
      await fire(ready.map((e) => ({ ...e.record.event, timestamp: now })))
    },

    async schedule(event: EventMessage, at: Date, uow?: UnitOfWork): Promise<ScheduleToken> {
      if (uow === undefined) {
        throw new NoActiveUnitOfWork(
          "recordingEventStore.schedule requires a UnitOfWork — call it as ctx.schedule from inside a handler",
        )
      }
      requireInvocation(uow)

      const token: ScheduleToken = { id: generateIdentifier() }
      const entry: Entry = {
        armed: false,
        record: { token, event, armedAt: clock(), fireAt: at.getTime(), status: "pending" },
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
      const entry = entries.find((e) => e.record.token.id === token.id)
      if (!entry) return { kind: "not-found" }
      if (entry.record.status === "fired") return { kind: "already-appended" }
      if (entry.record.status === "cancelled") return { kind: "not-found" }

      entry.record = { ...entry.record, status: "cancelled" }
      if (uow !== undefined && !uow.closed) {
        uow.onError(() => {
          if (entry.record.status === "cancelled") entry.record = { ...entry.record, status: "pending" }
        })
      }
      return { kind: "cancelled" }
    },
  }
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
  const capable = bus as B & Partial<SubscriptionBusCapability>
  const log: Array<QueryMessage> = []

  return {
    ...bus,

    get queried(): ReadonlyArray<QueryMessage> {
      return log
    },

    reset(): void {
      log.length = 0
    },

    async query(message: QueryMessage): Promise<unknown> {
      log.push(message)
      return bus.query(message)
    },

    subscribe(queryName: string, handler: unknown): void {
      bus.subscribe(queryName, handler as never)
    },

    // The subscription tier is recorded only where the wrapped bus claims it;
    // the emit/complete half carries no query message and rides the spread.
    ...(typeof capable.subscriptionQuery === "function"
      ? {
          subscriptionQuery: (message: QueryMessage, bufferSize?: number): SubscriptionQueryResult => {
            log.push(message)
            return capable.subscriptionQuery!(message, bufferSize)
          },
        }
      : {}),
    ...(typeof capable.subscribeToUpdates === "function"
      ? {
          subscribeToUpdates: (
            message: QueryMessage,
            bufferSize?: number,
          ): AsyncIterable<unknown> & { close(): void } => {
            log.push(message)
            return capable.subscribeToUpdates!(message, bufferSize)
          },
        }
      : {}),



  } as B & QueryRecording
}
