import type { Components } from "@kronos-ts/app"
import type { AppendCondition, ConsistencyMarker, EventStore } from "@kronos-ts/eventsourcing"
import type { CommandBus, CommandMessage, EventMessage } from "@kronos-ts/messaging"

/**
 * Recorded traffic from a test run.
 *
 * Recording is a wrapper, not a registration: you build the components you
 * want, wrap the two that carry traffic, and pass the result to `createApp`.
 * There is no enhancer slot, no priority, no extension — the wrapper is
 * innermost because you put it innermost.
 */
export interface Recordings {
  /** Events appended since the last reset. */
  events(): ReadonlyArray<EventMessage>
  /** Commands dispatched since the last reset. */
  commands(): ReadonlyArray<CommandMessage>
  /** Clear all recordings. The fixture calls this between Given and When. */
  reset(): void
}

/** Internal writer surface — the wrappers below hold it, callers never see it. */
interface RecordingsInternal extends Recordings {
  readonly _push: {
    event: (e: EventMessage) => void
    command: (c: CommandMessage) => void
  }
}

/** Create a fresh recordings handle to hand to the wrappers below. */
export function createRecordings(): Recordings {
  const recordedEvents: EventMessage[] = []
  const recordedCommands: CommandMessage[] = []
  const internal: RecordingsInternal = {
    events: () => [...recordedEvents],
    commands: () => [...recordedCommands],
    reset: () => {
      recordedEvents.length = 0
      recordedCommands.length = 0
    },
    _push: {
      event: (e) => {
        recordedEvents.push(e)
      },
      command: (c) => {
        recordedCommands.push(c)
      },
    },
  }
  return internal
}

function writersOf(recordings: Recordings): RecordingsInternal["_push"] {
  const push = (recordings as RecordingsInternal)._push
  if (!push) {
    throw new Error(
      "[recording] Recordings handle missing internal writers — pass an instance from createRecordings().",
    )
  }
  return push
}

/**
 * An event store that records what is appended through it, after a successful
 * append. Only the auto-committing `append` is wrapped — that is the path the
 * command-handling module writes through — so two-phase `appendEvents` callers
 * are not recorded.
 */
export function recordingEventStore(inner: EventStore, recordings: Recordings): EventStore {
  const push = writersOf(recordings)
  return {
    ...inner,
    async append(
      events: ReadonlyArray<EventMessage>,
      condition?: AppendCondition,
    ): Promise<ConsistencyMarker> {
      const marker = await inner.append(events, condition)
      for (const e of events) push.event(e)
      return marker
    },
  }
}

/**
 * A command bus that records every dispatched command BEFORE it reaches its
 * handler — the raw dispatched message, not the outcome.
 */
export function recordingCommandBus(inner: CommandBus, recordings: Recordings): CommandBus {
  const push = writersOf(recordings)
  return {
    ...inner,
    async dispatch(message: CommandMessage): Promise<unknown> {
      push.command(message)
      return inner.dispatch(message)
    },
  }
}

/**
 * Wrap the two traffic-carrying components of a component record. Everything
 * else passes through untouched.
 *
 * ```ts
 * const recordings = createRecordings()
 * const app = createApp({
 *   components: recordingComponents(inMemoryComponents(), recordings),
 *   modules: [module("courses", ...courses)],
 * })
 * ```
 */
export function recordingComponents(components: Components, recordings: Recordings): Components {
  return {
    ...components,
    eventStore: recordingEventStore(components.eventStore, recordings),
    commandBus: recordingCommandBus(components.commandBus, recordings),
  }
}

/**
 * Same, for a module's partial overrides: wrap whichever of the two it brings
 * of its own, leave the rest to inherit (already-wrapped) app components.
 */
export function recordingOverrides(
  overrides: Partial<Components>,
  recordings: Recordings,
): Partial<Components> {
  const wrapped: Partial<Components> = { ...overrides }
  if (overrides.eventStore) wrapped.eventStore = recordingEventStore(overrides.eventStore, recordings)
  if (overrides.commandBus) wrapped.commandBus = recordingCommandBus(overrides.commandBus, recordings)
  return wrapped
}
