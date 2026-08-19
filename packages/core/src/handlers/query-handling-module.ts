import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { StateManagerLike } from "../state/load.js"
import { queryHandlerContext } from "./handler-context.js"
import type { QueryMessage } from "../messages/message.js"
import type { QueryBus } from "../buses/query-bus.js"
import type { QueryHandlerDefinition } from "./query-handler.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Subscribes query handlers onto the query bus, each with a per-invocation
 * context closed over the unit of work the bus handed in — freshly opened for
 * a primary query, or the CALLER'S when a handler consulted this read via
 * `ctx.query`, so the nested read shares its transaction.
 *
 * `stateManager` is optional: without it the context still carries
 * `transaction` and `query`, and `ctx.load` throws the usual "no state manager
 * configured" error.
 */
export function subscribeQueryHandlers(
  handlers: ReadonlyArray<QueryHandlerDefinition>,
  deps: {
    queryBus: QueryBus
    stateManager?: StateManagerLike
  },
): void {
  for (const reg of handlers) {
    const queryName = qualifiedNameToString(reg.descriptor.name)
    const invocation = async (message: QueryMessage, uow: UnitOfWork) => {
      return reg.handler(
        message,
        queryHandlerContext({
          uow,
          message,
          ...(deps.stateManager ? { stateManager: deps.stateManager } : {}),
          // ctx.query from a query handler: reads may compose reads.
          queryBus: deps.queryBus,
        }),
      )
    }
    deps.queryBus.subscribe(queryName, invocation)
  }
}
