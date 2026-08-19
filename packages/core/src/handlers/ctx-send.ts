import { emptyMetadata, mergeMetadata } from "../primitives/metadata.js"
import { generateIdentifier } from "../primitives/identifier.js"
import type { CommandBus } from "../buses/command-bus.js"
import type { CommandDescriptor } from "../messages/descriptor.js"
import type { Message } from "../messages/message.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"
import type { z } from "zod"

/** `ctx.send` — dispatch a command from inside a handler. */
export type CommandDispatchFunction = <P extends z.ZodType, R extends z.ZodType | undefined = undefined>(
  descriptor: CommandDescriptor<P, R>,
  payload: z.infer<P>,
) => Promise<unknown>

/**
 * Build the `send` capability for ONE invocation, closed over that
 * invocation's unit of work and command bus.
 *
 * Internal — not exported from the package barrel. Handlers reach the result
 * as `ctx.send`.
 *
 * AF5-aligned semantics: every command is handled in its own fresh UnitOfWork
 * (`commandBus.dispatch` always starts a new one — see `simpleCommandBus`).
 * The command handler is therefore its own atomic boundary: it loads state,
 * decides, appends events, and commits once — independent of the caller's
 * UnitOfWork.
 *
 * Lineage is stamped onto the outgoing command HERE, so correlation/causation
 * propagates the AF5 way — through message metadata — across any transport,
 * local or distributed. Nothing is threaded over the wire and nothing is read
 * from an ambient store on the far side.
 *
 * The BASE metadata is the causing message's own — the message this invocation
 * is handling, closed over here — so a host key like `actor` rides forward
 * without any provider configured. The unit of work's correlation data is
 * merged OVER it, which is where `correlationId`/`causationId` come from (the
 * default provider derives `causationId` from that same message's identifier).
 */
export function sendFunction(deps: {
  uow: UnitOfWork
  message?: Message
  commandBus?: CommandBus
}): CommandDispatchFunction {
  return async (descriptor, payload) => {
    const uow = requireInvocation(deps.uow)
    const bus = deps.commandBus
    if (!bus) throw new Error("No command bus configured")
    return bus.dispatch({
      kind: "command",
      identifier: generateIdentifier(),
      name: descriptor.name,
      payload,
      metadata: mergeMetadata(deps.message?.metadata ?? emptyMetadata(), uow.correlationData()),
      timestamp: uow.now(),
    })
  }
}
