import type { CommandBus } from "./bus.js"
import {
  withInstant,
  type CommandMessage,
  qualifiedNameToString,
} from "../messaging/messages.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
/**
 * Simple in-process command bus.
 *
 * Maintains a local handler map and dispatches commands directly,
 * wrapping each dispatch in a fresh UnitOfWork.
 *
 * AF5 parity: like `LocalCommandBus`, every command — primary OR nested
 * (dispatched from inside another handler via `send()`) — is handled in
 * its own independent UnitOfWork with its own commit boundary. A command
 * handler is the atomic unit; commands compose by independent commit, not
 * by sharing a transaction. DCB read-set / append-condition merging
 * happens only WITHIN a single handler's UnitOfWork.
 *
 * Hand it `drizzleUnitOfWork(unitOfWork, db)` (or any unit-of-work factory) and every
 * per-command unit of work carries a transaction — begun before the handler, committed at
 * COMMIT, rolled back on error. That is the ONLY axis that varies: the bus
 * always opens a fresh unit of work, so a command — primary OR nested via
 * `send()` — still gets its own (and its own independent transaction).
 * Supplying a transactional factory does not change AF5 isolation, only whether that fresh
 * unit of work has a transaction.
 *
 * Interceptor support is provided by wrapping with
 * {@link interceptingCommandBus}.
 */
export function localCommandBus<U extends UnitOfWork = UnitOfWork>(
  unitOfWork: () => U,
): CommandBus<U> {
  const handlers = new Map<string, (message: CommandMessage, uow: U) => Promise<unknown>>()

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      // AF5 parity: every command gets its own fresh UnitOfWork, even when
      // dispatched from inside another handler. Whatever the caller's handler
      // wanted the new command to carry is already on `message.metadata`, so it
      // crosses into the new UoW on the message.
      //
      // This is also where the command's INSTANT is settled. The `send` verb
      // builds the message without one because the instant belongs to the task,
      // and the task is the unit of work minted right here — so the birth stamp
      // and the handling clock are the same clock, by construction.
      //
      // The MINTED handle is what the handler receives, not `execute`'s
      // parameter: the factory's return type is `U`, so closing over it is what
      // carries a composed capability (correlating, an adapter's) through to
      // the handler's `ctx.unitOfWork` instead of laundering it back to the
      // bare handle the lifecycle protocol is declared against.
      const uow = unitOfWork()
      return uow.execute(() => handler(withInstant(message, () => uow.now()), uow))
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, uow: U) => Promise<unknown>,
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
