import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import { runInUoW } from "./unit-of-work.js"
import { qualifiedNameToString } from "@kronos-ts/common"

/**
 * Simple in-process command bus.
 *
 * Maintains a local handler map and dispatches commands directly,
 * wrapping each dispatch in a UnitOfWork via `runInUoW`.
 *
 * Plan 03-04 (CTX-04 / D-34): the explicit `unitOfWorkFactory`
 * parameter and branch are gone. `runInUoW` is the only codepath —
 * transactional wiring composes at the runner level via
 * `transactionalUnitOfWorkFactory(runInUoW, txManager)` and is consumed
 * by extensions / processors directly, not by the bus.
 *
 * Interceptor support is provided by wrapping with
 * {@link createInterceptingCommandBus}, following Java's pattern of
 * separating concerns (SimpleCommandBus vs InterceptingCommandBus).
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

      // Plan 03-01 (D-32) / Plan 03-04 (CTX-04): nested dispatch detects
      // the active UoW via ALS and reuses it; primary dispatch creates a
      // new one. UoW detection is purely ALS-based.
      return runInUoW(message.metadata, () => handler(message))
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
