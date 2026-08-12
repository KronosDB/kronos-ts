import { qualifiedNameToString } from "@kronos-ts/common"
import type { CommandHandlerDefinition } from "./command-handler.js"
import { HANDLER_CONTEXT } from "./handler-context.js"

/**
 * Minimal Configuration shape consumed by commandInvocation /
 * registerCommandHandlersNatively. Phase 8 D-82 reshape: messaging no longer
 * depends on the full @kronos-ts/common Configuration interface (deleted
 * with the configurer trio in Plan 08-04). Callers (kronos() AppImpl.start())
 * pass a shim that satisfies just these methods.
 *
 * The component-key strings used by commandInvocation are inlined
 * below (COMMAND_INVOCATION_KEYS) so this file owns its own key set.
 */
export interface MinimalConfiguration {
  hasComponent(type: string, name?: string): boolean
  getComponent<T>(type: string, name?: string): T
  getOptionalComponent<T>(type: string, name?: string): T | undefined
}

/**
 * Component-key strings consumed by commandInvocation. Inlined here
 * so this file has no shared-constant dependency. The kronos() AppImpl
 * populates its config-shim with the same string keys.
 */
const COMMAND_INVOCATION_KEYS = {
  STATE_MANAGER: "stateManager",
  COMMAND_BUS: "commandBus",
  QUERY_BUS: "queryBus",
  EVENT_SCHEDULER: "eventScheduler",
  EVENT_STORE: "eventStore",
  TAG_RESOLVER: "tagResolver",
} as const

import type { CommandBus } from "./command-bus.js"
import type { QueryBus } from "./query-bus.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { setResource } from "./processing-state.js"
import { registerEventFlush } from "./event-flush.js"
import type { EventCriteria } from "./event-criteria.js"
import { COMMAND_BUS_KEY } from "./send.js"
import { QUERY_BUS_KEY } from "./emit-update.js"
import type { CommandMessage, EventMessage } from "./message.js"
import {
  BUFFERED_EVENTS_KEY,
  SOURCING_INFOS_KEY,
  STATE_MANAGER_KEY,
  EVENT_SCHEDULER_KEY,
} from "@kronos-ts/eventsourcing"
import type { EventScheduler } from "./event-scheduler.js"

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
export function commandInvocation(
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
    if (config.hasComponent(COMMAND_INVOCATION_KEYS.EVENT_SCHEDULER)) {
      setResource(EVENT_SCHEDULER_KEY, config.getComponent<EventScheduler>(COMMAND_INVOCATION_KEYS.EVENT_SCHEDULER))
    }

    // One flush per UnitOfWork (nested dispatch re-enters this wrapper).
    if (config.hasComponent(COMMAND_INVOCATION_KEYS.EVENT_STORE)) {
      registerEventFlush({
        eventStore: config.getComponent(COMMAND_INVOCATION_KEYS.EVENT_STORE),
        tagResolver: config.getOptionalComponent(COMMAND_INVOCATION_KEYS.TAG_RESOLVER),
        ...(handler.appendCondition
          ? { appendCondition: (criteria: EventCriteria) => handler.appendCondition!(message, criteria) }
          : {}),
      })
    }

    return handler.handler(message, HANDLER_CONTEXT)
  }
}

/**
 * Plan 08-03a (D-82 reshape): function-style helper called by AppImpl.start()
 * to subscribe command handlers natively, without the configurer's Module shape.
 *
 * Subscribes each handler onto the commandBus with the commandInvocation
 * wrapper that does Phase 4 D-44 ALS resource setup at invocation entry.
 *
 * @param handlers Array of handler definitions to register
 * @param deps Resolved dependencies — commandBus to subscribe onto, plus a
 *             Configuration shim (built natively in AppImpl.start()) that
 *             commandInvocation reads inside the dispatch hot path.
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
    let invocation = commandInvocation(handler, deps.config)

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
