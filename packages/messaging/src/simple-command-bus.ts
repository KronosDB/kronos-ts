import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { UnitOfWorkFactory } from "./unit-of-work.js"
import { defaultUnitOfWorkFactory } from "./unit-of-work.js"
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
  const factory = unitOfWorkFactory ?? defaultUnitOfWorkFactory()
  const handlers = new Map<string, (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>>()

  return {
    async dispatch(message: CommandMessage, context?: ProcessingContext): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      const uow = factory(message.metadata)
      return uow.executeWithResult(async (ctx) => {
        return handler(message, ctx)
      })
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
