import type { Extension, App } from "@kronos-ts/app"
import type { CommandBus, CommandMessage, EventMessage } from "@kronos-ts/messaging"

/**
 * Recorded state from the test fixture.
 * Events and commands are captured by decorators installed at the
 * INNERMOST position (after all interceptors have run).
 */
export interface Recordings {
  /** Events recorded since the last reset. */
  events(): ReadonlyArray<EventMessage>
  /** Commands dispatched since the last reset. */
  commands(): ReadonlyArray<CommandMessage>
  /** Clear all recordings. Called between Given and When phases. */
  reset(): void
}

/**
 * Internal-shape extension of {@link Recordings} carrying the writer pair
 * used by the decorators created in {@link testRecordingExtension}. Kept
 * `private` to the module — callers see only the {@link Recordings} surface.
 */
interface RecordingsInternal extends Recordings {
  readonly _push: {
    event: (e: EventMessage) => void
    command: (c: CommandMessage) => void
  }
}

/**
 * Create a fresh Recordings handle. Pass it into {@link testRecordingExtension}
 * so the fixture and the decorators share the same backing arrays.
 *
 * Replaces the legacy "register testRecordings as a component, retrieve via
 * configuration.getComponent" pattern (removed in Phase 8).
 */
export function createRecordings(): Recordings {
  const recordedEvents: EventMessage[] = []
  const recordedCommands: CommandMessage[] = []
  const internal: RecordingsInternal = {
    events() {
      return [...recordedEvents]
    },
    commands() {
      return [...recordedCommands]
    },
    reset() {
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

/**
 * Native Extension that decorates the eventStore and commandBus with
 * recording wrappers.
 *
 * **Decoration order** (Phase 6 D-62): user decorators registered AFTER this
 * extension's `app.use(...)` wrap OUTSIDE the recording decorators. To land
 * the recording decorators at the INNERMOST position (capturing messages
 * AFTER all interceptors have enriched them), call
 * `app.use(testRecordingExtension(recordings))` BEFORE applying any user
 * decorators / `configureFn(app)`.
 *
 * The legacy enhancer used `Number.MIN_SAFE_INTEGER` numeric priority for the
 * same effect; Phase 6 dropped numeric priorities — innermost = first
 * registered.
 */
export function testRecordingExtension(recordings: Recordings): Extension {
  const push = (recordings as RecordingsInternal)._push
  if (!push) {
    throw new Error(
      "[testRecordingExtension] Recordings handle missing internal writers — pass an instance from createRecordings().",
    )
  }
  return (app: App) => {
    // EventStore append wrapper — records events after a successful append.
    app.decorate("eventStore", (inner) => {
      const originalAppend = inner.append.bind(inner)
      return {
        ...inner,
        async append(events: ReadonlyArray<EventMessage>, condition?: any) {
          const result = await originalAppend(events, condition)
          for (const e of events) push.event(e)
          return result
        },
      }
    })

    // CommandBus dispatch wrapper — records commands BEFORE dispatch (legacy
    // semantics: legacy fixture inspected the raw dispatched command, not
    // the post-handler outcome).
    app.decorate("commandBus", (inner) => {
      const originalDispatch = inner.dispatch.bind(inner)
      return {
        ...inner,
        async dispatch(message: CommandMessage) {
          push.command(message)
          return originalDispatch(message)
        },
      } as CommandBus
    })
  }
}
