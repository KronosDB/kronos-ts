import { qualifiedNameToString, resourceKey } from "@kronos-ts/common"
import type { CommandHandlerDefinition } from "./command-handler.js"

/**
 * Minimal Configuration shape consumed by createCommandInvocation /
 * registerCommandHandlersNatively. Phase 8 D-82 reshape: messaging no longer
 * depends on the full @kronos-ts/common Configuration interface (deleted
 * with the configurer trio in Plan 08-04). Callers (kronos() AppImpl.start())
 * pass a shim that satisfies just these methods.
 *
 * The component-key strings used by createCommandInvocation are inlined
 * below (COMMAND_INVOCATION_KEYS) so this file owns its own key set.
 */
export interface MinimalConfiguration {
  hasComponent(type: string, name?: string): boolean
  getComponent<T>(type: string, name?: string): T
  getOptionalComponent<T>(type: string, name?: string): T | undefined
}

/**
 * Component-key strings consumed by createCommandInvocation. Inlined here
 * so this file has no shared-constant dependency. The kronos() AppImpl
 * populates its config-shim with the same string keys.
 */
const COMMAND_INVOCATION_KEYS = {
  STATE_MANAGER: "stateManager",
  COMMAND_BUS: "commandBus",
  QUERY_BUS: "queryBus",
  EVENT_STORE: "eventStore",
  TAG_RESOLVER: "tagResolver",
} as const

const EVENT_FLUSH_REGISTERED_KEY = resourceKey<boolean>("commandInvocationEventFlushRegistered")
import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { CORRELATION_DATA_KEY } from "./correlation-data.js"
import { getResource, setResource, onPrepareCommit, hasResource } from "./processing-state.js"
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
  config: MinimalConfiguration,
) {
  return async (message: CommandMessage): Promise<unknown> => {
    // D-82 — full ALS resource setup at command invocation entry.
    // STATE_MANAGER: read by load() helper. COMMAND_BUS: read by send() helper.
    // QUERY_BUS: read by emitUpdate() helper.
    if (config.hasComponent(COMMAND_INVOCATION_KEYS.STATE_MANAGER)) {
      setResource(STATE_MANAGER_KEY, config.getComponent<any>(COMMAND_INVOCATION_KEYS.STATE_MANAGER))
    }
    if (config.hasComponent(COMMAND_INVOCATION_KEYS.COMMAND_BUS)) {
      setResource(COMMAND_BUS_KEY, config.getComponent<CommandBus>(COMMAND_INVOCATION_KEYS.COMMAND_BUS))
    }
    if (config.hasComponent(COMMAND_INVOCATION_KEYS.QUERY_BUS)) {
      setResource(QUERY_BUS_KEY, config.getComponent<QueryBus>(COMMAND_INVOCATION_KEYS.QUERY_BUS))
    }

    // Register event flush in PREPARE_COMMIT phase once per UnitOfWork.
    // Nested context-aware dispatch re-enters createCommandInvocation inside
    // the same ALS state; without this guard every nested command registers an
    // additional flush and the same buffered events are appended repeatedly.
    if (!hasResource(EVENT_FLUSH_REGISTERED_KEY)) {
      setResource(EVENT_FLUSH_REGISTERED_KEY, true)
      onPrepareCommit(async () => {
        const buffered = getResource(BUFFERED_EVENTS_KEY)
        if (!buffered || buffered.length === 0) return
        if (!config.hasComponent(COMMAND_INVOCATION_KEYS.EVENT_STORE)) return

        const eventStore = config.getComponent<{ append: (events: ReadonlyArray<EventMessage>, condition?: any) => Promise<unknown> }>(COMMAND_INVOCATION_KEYS.EVENT_STORE)

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
        const tagResolver = config.getOptionalComponent<{ resolve: (event: EventMessage) => Array<{ key: string; value: string }> }>(COMMAND_INVOCATION_KEYS.TAG_RESOLVER)
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
            ? handler.appendCondition(message, combinedCriteria)
            : combinedCriteria

          appendCondition = {
            criteria: finalCriteria,
            marker: { position: maxMarker },
          }
        }

        await eventStore.append(resolvedEvents, appendCondition)
      })
    }

    return handler.handler(message)
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
    config: MinimalConfiguration
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
