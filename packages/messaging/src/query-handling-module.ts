import { qualifiedNameToString } from "@kronos-ts/common"
import type { QueryHandlersDefinition } from "./query-handler.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"

/**
 * Plan 08-03a (D-82 reshape): function-style helper called by AppImpl.start()
 * to subscribe query handlers natively, without the configurer's Module shape.
 *
 * Query handlers do NOT need a Configuration shim — queries don't append
 * events, so no per-invocation ALS resource setup is required at query
 * subscription level.
 */
export function registerQueryHandlersNatively(
  groups: ReadonlyArray<QueryHandlersDefinition>,
  deps: { queryBus: QueryBus },
): void {
  for (const group of groups) {
    for (const reg of group.handlers) {
      const queryName = qualifiedNameToString(reg.descriptor.name)
      deps.queryBus.subscribe(queryName, async (message: QueryMessage) => {
        return reg.handler(message.payload, message.metadata)
      })
    }
  }
}
