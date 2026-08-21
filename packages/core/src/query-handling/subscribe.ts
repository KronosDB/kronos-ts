import { qualifiedNameToString, type QueryMessage } from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { queryHandlerContext, type QueryHandlerContext } from "./context.js"
import type { QueryBus } from "./bus.js"
import type { QueryHandler } from "./handler.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * Subscribes query handlers onto the query bus, each with a per-invocation
 * context closed over the unit of work the bus handed in — freshly opened for
 * a primary query, or the CALLER'S when a handler consulted this read via
 * `ctx.query`, so the nested read shares its transaction.
 *
 * `eventStore` is optional, and a query handler is the one kind that
 * legitimately has none — a read model served from a projection table needs no
 * log to answer. Without it the context still carries `query` and its unit of
 * work, and `ctx.load` throws naming the state it could not source.
 */
export function subscribeQueryHandlers<U extends UnitOfWork, E extends EventStore = EventStore>(
  handlers: ReadonlyArray<QueryHandler<any, any, QueryHandlerContext<U, E>>>,
  deps: {
    queryBus: QueryBus<U>
    /** The log `ctx.load` sources from, and what it is allowed to load. */
    eventStore?: E
  },
): void {
  for (const reg of handlers) {
    const queryName = qualifiedNameToString(reg.descriptor.name)
    const invocation = async (message: QueryMessage, uow: U) => {
      return reg.handler(
        message,
        queryHandlerContext<U, E>({
          uow,
          ...(deps.eventStore ? { eventStore: deps.eventStore } : {}),
          // ctx.query from a query handler: reads may compose reads.
          queryBus: deps.queryBus,
        }),
      )
    }
    deps.queryBus.subscribe(queryName, invocation)
  }
}
