import {
  ComponentKeys,
  qualifiedNameToString,
  type Configuration,
  type Module,
} from "@kronos-ts/common"
import type { QueryHandlersDefinition } from "./query-handler.js"
import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"

/**
 * A module that registers query handlers with the query bus.
 *
 * ```
 * queryHandlingModule("course-queries", [courseQueries])
 * ```
 */
export function queryHandlingModule(
  moduleName: string,
  handlerGroups: ReadonlyArray<QueryHandlersDefinition>,
): Module {
  return {
    name: moduleName,

    initialize(config: Configuration) {
      const bus = config.getComponent<QueryBus>(ComponentKeys.QUERY_BUS)
      for (const group of handlerGroups) {
        for (const reg of group.handlers) {
          const queryName = qualifiedNameToString(reg.descriptor.name)
          bus.subscribe(queryName, async (message: QueryMessage) => {
            return reg.handler(message.payload, { metadata: message.metadata })
          })
        }
      }
    },
  }
}
