import { qualifiedNameToString } from "@kronos-ts/common"
import type { QueryHandlerDefinition } from "./query-handler.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"
import { QUERY_HANDLER_CONTEXT } from "./handler-context.js"
import { setResource } from "./processing-state.js"
import { STATE_MANAGER_KEY } from "@kronos-ts/eventsourcing"
import type { MinimalConfiguration } from "./command-handling-module.js"

/**
 * Function-style helper called by AppImpl.start() to subscribe query handlers
 * natively, without the configurer's Module shape.
 *
 * Accepts a flat ReadonlyArray<QueryHandlerDefinition> (singular handler shape).
 *
 * Symmetric to registerCommandHandlersNatively — accepts an optional
 * handlerEnhancer that wraps each query invocation so query handlers receive
 * the same tracing / timing / cross-cutting treatment as command and event
 * handlers. moduleName defaults to "queries" for HandlerMetadata.handlerGroup.
 *
 * Queries run inside a UnitOfWork (see `simpleQueryBus`), so when a
 * Configuration shim is supplied the state manager is seeded onto the active
 * ALS state exactly as the command path does — that is what backs
 * `ctx.load`. The shim is optional: without it the query context still
 * carries `transaction` (which needs only the active UoW), and `ctx.load`
 * throws the usual "no state manager configured" error.
 */
export function registerQueryHandlersNatively(
  handlers: ReadonlyArray<QueryHandlerDefinition>,
  deps: {
    queryBus: QueryBus
    handlerEnhancer?: HandlerEnhancerDefinition
    moduleName?: string
    config?: MinimalConfiguration
  },
): void {
  const moduleName = deps.moduleName ?? "queries"
  for (const reg of handlers) {
    const queryName = qualifiedNameToString(reg.descriptor.name)
    let invocation = async (message: QueryMessage) => {
      const config = deps.config
      if (config?.hasComponent("stateManager")) {
        setResource(STATE_MANAGER_KEY, config.getComponent<never>("stateManager"))
      }
      return reg.handler(message, QUERY_HANDLER_CONTEXT)
    }
    if (deps.handlerEnhancer) {
      invocation = deps.handlerEnhancer.wrapHandler(invocation, {
        messageType: "query",
        messageName: queryName,
        handlerGroup: moduleName,
      })
    }
    deps.queryBus.subscribe(queryName, invocation)
  }
}
