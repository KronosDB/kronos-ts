import {
  ComponentKeys,
  qualifiedNameToString,
  type Configuration,
  type Module,
} from "@kronos-ts/common"
import type { CommandHandlerDefinition } from "./command-handler.js"
import type { CommandBus } from "./command-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { CORRELATION_DATA_KEY } from "./correlation-data.js"
import { getResource, setResource, onPrepareCommit } from "./processing-state.js"
import type { CommandMessage, EventMessage } from "./message.js"
import {
  BUFFERED_EVENTS_KEY,
  SOURCING_INFOS_KEY,
  STATE_MANAGER_KEY,
} from "@kronos-ts/eventsourcing"

// ---------------------------------------------------------------------------
// Command invocation — builds handler context from ProcessingContext
// ---------------------------------------------------------------------------

/**
 * Creates a command handler invocation function that uses the ProcessingContext
 * for state caching, event buffering, and lifecycle-aware event flushing.
 */
function createCommandInvocation(
  handler: CommandHandlerDefinition<any, any>,
  config: Configuration,
) {
  return async (message: CommandMessage): Promise<unknown> => {
    // D-44 wiring: write state manager into ALS at invocation entry so that
    // the module-level load + append helpers can access it.
    if (config.hasComponent(ComponentKeys.STATE_MANAGER)) {
      setResource(STATE_MANAGER_KEY, config.getComponent<any>(ComponentKeys.STATE_MANAGER))
    }

    // Register event flush in PREPARE_COMMIT phase
    onPrepareCommit(async () => {
      const buffered = getResource(BUFFERED_EVENTS_KEY)
      if (!buffered || buffered.length === 0) return
      if (!config.hasComponent(ComponentKeys.EVENT_STORE)) return

      const eventStore = config.getComponent<{ append: (events: ReadonlyArray<EventMessage>, condition?: any) => Promise<unknown> }>(ComponentKeys.EVENT_STORE)

      // Enrich events with correlation data from ProcessingContext
      // (set by the CorrelationDataHandlerInterceptor during handler execution)
      const correlationData = getResource(CORRELATION_DATA_KEY)
      const enrichedEvents = correlationData
        ? buffered.map(event => ({
            ...event,
            metadata: { ...event.metadata, ...correlationData },
          }))
        : buffered

      // Resolve tags via TagResolver (if configured)
      const tagResolver = config.getOptionalComponent<{ resolve: (event: EventMessage) => Array<{ key: string; value: string }> }>(ComponentKeys.TAG_RESOLVER)
      const resolvedEvents = tagResolver
        ? enrichedEvents.map(event => ({
            ...event,
            tags: [...event.tags, ...tagResolver.resolve(event)],
          }))
        : enrichedEvents
      const sourcingInfos = getResource(SOURCING_INFOS_KEY) ?? []

      let appendCondition: any = undefined
      if (sourcingInfos.length > 0) {
        const combinedCriteria = sourcingInfos.length === 1
          ? sourcingInfos[0]!.criteria
          : { kind: "either" as const, criteria: sourcingInfos.map((s) => s.criteria) }

        const maxMarker = sourcingInfos.reduce(
          (max, s) => s.markerPosition > max ? s.markerPosition : max,
          -1n,
        )

        const finalCriteria = handler.appendCondition
          ? handler.appendCondition(message.payload, combinedCriteria)
          : combinedCriteria

        appendCondition = {
          criteria: finalCriteria,
          marker: { position: maxMarker },
        }
      }

      await eventStore.append(resolvedEvents, appendCondition)
    })

    return handler.handler(message.payload, message.metadata)
  }
}

/**
 * A module that registers command handlers with the command bus.
 *
 * ```
 * commandHandlingModule("course-commands", [createCourse, changeCourseCapacity])
 * ```
 */
export function commandHandlingModule(
  moduleName: string,
  handlers: ReadonlyArray<CommandHandlerDefinition<any, any>>,
): Module {
  return {
    name: moduleName,

    initialize(config: Configuration) {
      const bus = config.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS)
      const enhancer = config.getOptionalComponent<HandlerEnhancerDefinition>(
        ComponentKeys.HANDLER_ENHANCER_DEFINITIONS,
      )

      for (const handler of handlers) {
        const commandName = qualifiedNameToString(handler.descriptor.name)
        let invocation = createCommandInvocation(handler, config)

        if (enhancer) {
          invocation = enhancer.wrapHandler(invocation, {
            messageType: "command",
            messageName: commandName,
            handlerGroup: moduleName,
          })
        }

        bus.subscribe(commandName, invocation)
      }
    },
  }
}
