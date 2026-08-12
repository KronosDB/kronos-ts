import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import { runInNewUoW, type UoWRunner } from "./unit-of-work.js"
import { qualifiedNameToString } from "@kronos-ts/common"

/**
 * Simple in-process command bus.
 *
 * Maintains a local handler map and dispatches commands directly,
 * wrapping each dispatch in a fresh UnitOfWork via `runInNewUoW`.
 *
 * AF5 parity: like `SimpleCommandBus`, every command — primary OR nested
 * (dispatched from inside another handler via `send()`) — is handled in
 * its own independent UnitOfWork with its own commit boundary. A command
 * handler is the atomic unit; commands compose by independent commit, not
 * by sharing a transaction. DCB read-set / append-condition merging
 * happens only WITHIN a single handler's UnitOfWork.
 *
 * The handler runs through `unitOfWorkRunner` — by default `runInNewUoW`, but
 * the configurer injects the resolved `unitOfWorkFactory` slot (e.g. a
 * transactional runner from a storage extension) so the per-command UoW
 * carries whatever transaction that backend provides. This mirrors the
 * distributed command buses (kronosdb / axon-server), which already run
 * handlers through the configured runner. The runner is always built on
 * `runInNewUoW`, so a command — primary OR nested via `send()` — still gets
 * its own fresh UoW (and its own independent transaction); composition does
 * not change AF5 isolation, only whether that fresh UoW has a transaction.
 *
 * Interceptor support is provided by wrapping with
 * {@link interceptingCommandBus}.
 */
export function simpleCommandBus(unitOfWorkRunner: UoWRunner = runInNewUoW): CommandBus {
  const handlers = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      // AF5 parity: every command gets its own fresh UnitOfWork (the runner is
      // built on runInNewUoW), even when dispatched from inside another
      // handler. Dispatch interceptors have already run in the caller's context
      // (the intercepting bus wraps this one), so correlation data is carried
      // on `message.metadata` before we cross into the new UoW.
      return unitOfWorkRunner(message.metadata, () => handler(message))
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage) => Promise<unknown>,
    ) {
      const existing = handlers.get(commandName)
      if (existing && existing !== handler) {
        throw new Error(
          `A different handler is already registered for command "${commandName}". ` +
          `Duplicate command handler subscriptions are not allowed.`,
        )
      }
      handlers.set(commandName, handler)
    },
  }
}
