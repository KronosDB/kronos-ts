import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import { runInNewUoW } from "./unit-of-work.js"
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
 * Transactional wiring composes at the runner level via
 * `transactionalUnitOfWorkFactory(runInNewUoW, txManager)` and is consumed
 * by extensions / processors directly, not by the bus.
 *
 * Interceptor support is provided by wrapping with
 * {@link createInterceptingCommandBus}.
 */
export function createSimpleCommandBus(): CommandBus {
  const handlers = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      // AF5 parity: every command gets its own fresh UnitOfWork, even when
      // dispatched from inside another handler. Dispatch interceptors have
      // already run in the caller's context (the intercepting bus wraps
      // this one), so correlation data is carried on `message.metadata`
      // before we cross into the new UoW.
      return runInNewUoW(message.metadata, () => handler(message))
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
