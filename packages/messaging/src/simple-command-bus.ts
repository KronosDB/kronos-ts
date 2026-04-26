import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { UnitOfWorkFactory } from "./unit-of-work.js"
import { runInUoW } from "./unit-of-work.js"
import { qualifiedNameToString } from "@kronos-ts/common"

/**
 * Simple in-process command bus.
 *
 * Maintains a local handler map and dispatches commands directly,
 * wrapping each dispatch in a UnitOfWork that drives the ProcessingContext
 * lifecycle.
 *
 * Interceptor support is provided by wrapping with
 * {@link createInterceptingCommandBus}, following Java's pattern of
 * separating concerns (SimpleCommandBus vs InterceptingCommandBus).
 */
export function createSimpleCommandBus(
  unitOfWorkFactory?: UnitOfWorkFactory,
): CommandBus {
  const handlers = new Map<string, (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>>()

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      // Plan 03-01 (D-32) / Plan 03-03 (CTX-01): default codepath routes
      // through runInUoW so that nested dispatch (handler-internal bus calls)
      // detects the active UoW via ALS and reuses it. Primary dispatch (no
      // active UoW) creates a new one. The `context` parameter is gone — UoW
      // detection is purely ALS-based.
      if (unitOfWorkFactory !== undefined) {
        // Caller provided a custom factory (e.g., transactionalUnitOfWorkFactory) —
        // preserve the explicit-factory codepath so transactional wiring keeps
        // working. Plan 04 (D-34) rewrites transactionalUnitOfWorkFactory as a
        // composable runner wrapper and this branch disappears.
        const uow = unitOfWorkFactory(message.metadata)
        return uow.executeWithResult(async (ctx) => handler(message, ctx))
      }
      return runInUoW(message.metadata, (ctx) => handler(message, ctx))
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>,
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
