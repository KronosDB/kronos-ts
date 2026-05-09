import { qualifiedNameToString } from "@kronos-ts/common"
import type { QueryHandlersDefinition } from "./query-handler.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"
import type { HandlerEnhancerDefinition } from "./handler-enhancer.js"

/**
 * Plan 08-03a (D-82 reshape): function-style helper called by AppImpl.start()
 * to subscribe query handlers natively, without the configurer's Module shape.
 *
 * Plan 09-01 (RESEARCH Open Question #4): symmetric to
 * registerCommandHandlersNatively — accepts an optional handlerEnhancer that
 * wraps each query invocation, so query handlers receive the same tracing /
 * timing / cross-cutting treatment as command and event handlers. moduleName
 * defaults to `"queries"` for HandlerMetadata.handlerGroup.
 *
 * Query handlers do NOT need a Configuration shim — queries don't append
 * events, so no per-invocation ALS resource setup is required at query
 * subscription level.
 */
export function registerQueryHandlersNatively(
  groups: ReadonlyArray<QueryHandlersDefinition>,
  deps: {
    queryBus: QueryBus
    handlerEnhancer?: HandlerEnhancerDefinition
    moduleName?: string
  },
): void {
  const moduleName = deps.moduleName ?? "queries"
  for (const group of groups) {
    for (const reg of group.handlers) {
      const queryName = qualifiedNameToString(reg.descriptor.name)
      let invocation = async (message: QueryMessage) =>
        reg.handler(message.payload, message.metadata)
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
}
