import { resourceKey, generateIdentifier, type ResourceKey } from "@kronos-ts/common"
import { requireInvocationPhase } from "./processing-state.js"
import type { CommandBus } from "./command-bus.js"
import type { SendFunction } from "./handler.js"

/**
 * Resource key for the command bus component.
 * Written by handling modules + processors at handler-invocation entry (D-44).
 */
export const COMMAND_BUS_KEY: ResourceKey<CommandBus> = resourceKey("commandBus")

/**
 * Plan 04-01 (HDL-02 / D-42): module-level send.
 *
 * Throws NoActiveUnitOfWork outside a UoW; throws WrongUoWPhase outside
 * INVOCATION phase (D-43 mutator guard). Dispatches a command message through
 * the active command bus, using the active UoW metadata.
 */
export const send: SendFunction = async (descriptor, payload) => {
  const state = requireInvocationPhase() // D-43 mutator guard
  const bus = state.resources.get(COMMAND_BUS_KEY.symbol) as CommandBus | undefined
  if (!bus) throw new Error("No command bus configured")
  return bus.dispatch({
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: state.metadata,
    timestamp: Date.now(),
  })
}
