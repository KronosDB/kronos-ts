import {
  ComponentKeys,
  qualifiedNameToString,
  type Configuration,
} from "@kronos-ts/common"
import type { CommandHandlerDefinition } from "./command-handler.js"
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { CORRELATION_DATA_KEY } from "./correlation-data.js"
import { getResource, setResource, onPrepareCommit } from "./processing-state.js"
import { COMMAND_BUS_KEY } from "./send.js"
import { QUERY_BUS_KEY } from "./emit-update.js"
import type { CommandMessage, EventMessage } from "./message.js"
import {
  BUFFERED_EVENTS_KEY,
  SOURCING_INFOS_KEY,
  STATE_MANAGER_KEY,
} from "@kronos-ts/eventsourcing"

// ---------------------------------------------------------------------------
// Command invocation — D-82: byte-identical ALS resource setup at invocation entry.
// Plan 08-03a expanded the ALS three-key set: STATE_MANAGER + COMMAND_BUS + QUERY_BUS.
// ---------------------------------------------------------------------------

/**
 * Creates a command handler invocation function that uses the ProcessingContext
 * for state caching, event buffering, and lifecycle-aware event flushing.
 *
 * D-82: ALS resource setup is preserved BYTE-IDENTICAL across the configurer
 * deletion. The invocation entry seeds the three-key ALS set so module-level
 * helpers (load/append/send/emitUpdate) and the onPrepareCommit closure all
 * resolve their dependencies from the active UoW state.
 */
export function createCommandInvocation(
  handler: CommandHandlerDefinition<any, any>,
  config: Configuration,
) {
  return async (message: CommandMessage): Promise<unknown> => {
    // D-82 — full ALS resource setup at command invocation entry.
    // STATE_MANAGER: read by load() helper. COMMAND_BUS: read by send() helper.
    // QUERY_BUS: read by emitUpdate() helper.
    if (config.hasComponent(ComponentKeys.STATE_MANAGER)) {
      setResource(STATE_MANAGER_KEY, config.getComponent<any>(ComponentKeys.STATE_MANAGER))
    }
    if (config.hasComponent(ComponentKeys.COMMAND_BUS)) {
      setResource(COMMAND_BUS_KEY, config.getComponent<CommandBus>(ComponentKeys.COMMAND_BUS))
    }
    if (config.hasComponent(ComponentKeys.QUERY_BUS)) {
      setResource(QUERY_BUS_KEY, config.getComponent<QueryBus>(ComponentKeys.QUERY_BUS))
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
 * Plan 08-03a (D-82 reshape): function-style helper called by AppImpl.start()
 * to subscribe command handlers natively, without the configurer's Module shape.
 *
 * Subscribes each handler onto the commandBus with the createCommandInvocation
 * wrapper that does Phase 4 D-44 ALS resource setup at invocation entry.
 *
 * @param handlers Array of handler definitions to register
 * @param deps Resolved dependencies — commandBus to subscribe onto, plus a
 *             Configuration shim (built natively in AppImpl.start()) that
 *             createCommandInvocation reads inside the dispatch hot path.
 *             Optional handlerEnhancer mirrors the legacy module's enhancer
 *             wrap (kept for parity; default-decorator pipeline already
 *             handles framework-default interceptors).
 * @param moduleName Logical name passed to the handler enhancer for
 *                   discriminating command handler groups (default: "commands").
 */
export function registerCommandHandlersNatively(
  handlers: ReadonlyArray<CommandHandlerDefinition<any, any>>,
  deps: {
    commandBus: CommandBus
    config: Configuration
    handlerEnhancer?: HandlerEnhancerDefinition
    moduleName?: string
  },
): void {
  const moduleName = deps.moduleName ?? "commands"
  for (const handler of handlers) {
    const commandName = qualifiedNameToString(handler.descriptor.name)
    let invocation = createCommandInvocation(handler, deps.config)

    if (deps.handlerEnhancer) {
      invocation = deps.handlerEnhancer.wrapHandler(invocation, {
        messageType: "command",
        messageName: commandName,
        handlerGroup: moduleName,
      })
    }

    deps.commandBus.subscribe(commandName, invocation)
  }
}
