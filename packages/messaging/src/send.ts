import { resourceKey, generateIdentifier, type ResourceKey } from "@kronos-ts/common"
import { requireInvocationPhase } from "./processing-state.js"
import type { CommandBus } from "./command-bus.js"
import type { CommandDescriptor } from "./descriptor.js"
import type { z } from "zod"

type CommandDispatchFunction = <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  descriptor: CommandDescriptor<P, R>,
  payload: z.infer<P>,
) => Promise<unknown>

/**
 * Resource key for the command bus component.
 * Written by handling modules + processors at handler-invocation entry (D-44).
 */
export const COMMAND_BUS_KEY: ResourceKey<CommandBus> = resourceKey("commandBus")

/**
 * Send a command from inside a handler.
 *
 * AF5-aligned semantics: every command is handled in its own fresh
 * UnitOfWork (`commandBus.dispatch` always starts a new one — see
 * `createSimpleCommandBus`). The command handler is therefore its own
 * atomic boundary: it loads state, decides, appends events, and commits
 * once — independent of the caller's UnitOfWork.
 *
 * The caller's `metadata` IS carried onto the outgoing command, so
 * correlation/causation lineage propagates the AF5 way — through message
 * metadata, applied by the correlation-data dispatch interceptor — across
 * any transport, local or distributed. No processing-context object is
 * threaded through the command API or over the wire.
 */
export const send: CommandDispatchFunction = async (descriptor, payload) => {
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
