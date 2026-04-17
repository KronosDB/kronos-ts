import {
  ComponentKeys,
  type ComponentRegistry,
  type ConfigurationEnhancer,
} from "@kronos-ts/common"
import type { EventMessage, CommandBus, CommandMessage } from "@kronos-ts/messaging"
import type { ProcessingContext } from "@kronos-ts/messaging"

/**
 * Recorded state from the test fixture.
 * Events and commands are captured by decorators installed
 * at the innermost position (after all interceptors have run).
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
 * Creates a ConfigurationEnhancer that installs recording decorators
 * on the CommandBus and EventStore.
 *
 * The decorators sit at the INNERMOST position in the decorator chain
 * (decoration order = very low number), so they capture messages
 * AFTER all dispatch interceptors have enriched them.
 *
 * This is the TypeScript equivalent of AF5's MessagesRecordingConfigurationEnhancer.
 */
export function createRecordingEnhancer(): ConfigurationEnhancer {
  const recordedEvents: EventMessage[] = []
  const recordedCommands: CommandMessage[] = []

  const recordings: Recordings = {
    events() { return [...recordedEvents] },
    commands() { return [...recordedCommands] },
    reset() {
      recordedEvents.length = 0
      recordedCommands.length = 0
    },
  }

  return {
    // Run LAST among enhancers so all other components are set up first
    order: Number.MAX_SAFE_INTEGER,

    enhance(registry: ComponentRegistry) {
      // Register the recordings as a component so the fixture can retrieve them
      registry.register("testRecordings", () => recordings)

      // Wrap the EventStore at the innermost position.
      // Decoration order = very low = innermost = after all interceptors.
      registry.registerDecorator(
        ComponentKeys.EVENT_STORE,
        Number.MIN_SAFE_INTEGER,
        (_config, _name, delegate: any) => {
          const original = delegate.append.bind(delegate)
          return {
            ...delegate,
            async append(events: ReadonlyArray<EventMessage>, condition?: any) {
              const result = await original(events, condition)
              recordedEvents.push(...events)
              return result
            },
          }
        },
      )

      // Wrap the CommandBus at a position just outside the innermost
      // (after distributed bus decorator if any, but before user decorators).
      registry.registerDecorator(
        ComponentKeys.COMMAND_BUS,
        Number.MIN_SAFE_INTEGER + 1,
        (_config, _name, delegate: CommandBus) => {
          const originalDispatch = delegate.dispatch.bind(delegate)
          return {
            ...delegate,
            async dispatch(message: CommandMessage) {
              recordedCommands.push(message)
              return originalDispatch(message)
            },
          } as CommandBus
        },
      )
    },
  }
}
