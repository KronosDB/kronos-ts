import type { CommandBus } from "./command-bus.js"
import { stamped, type CommandMessage, type Unstamped } from "../messages/message.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"
import { qualifiedNameToString } from "../primitives/qualified-name.js"

/**
 * Simple in-process command bus.
 *
 * Maintains a local handler map and dispatches commands directly,
 * wrapping each dispatch in a fresh UnitOfWork.
 *
 * AF5 parity: like `SimpleCommandBus`, every command — primary OR nested
 * (dispatched from inside another handler via `send()`) — is handled in
 * its own independent UnitOfWork with its own commit boundary. A command
 * handler is the atomic unit; commands compose by independent commit, not
 * by sharing a transaction. DCB read-set / append-condition merging
 * happens only WITHIN a single handler's UnitOfWork.
 *
 * Hand it `drizzleUnitOfWork(db, unitOfWork)` (or any unit-of-work factory) and every
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
export function simpleCommandBus(unitOfWork: () => UnitOfWork): CommandBus {
  const handlers = new Map<string, (message: CommandMessage, uow: UnitOfWork) => Promise<unknown>>()

  return {
    async dispatch(message: Unstamped<CommandMessage>): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for command "${key}"`)
      }

      // AF5 parity: every command gets its own fresh UnitOfWork, even when
      // dispatched from inside another
      // handler. `ctx.send` already stamped the caller's lineage onto
      // `message.metadata`, so it crosses into the new UoW on the message.
      //
      // This is also where the command's INSTANT is settled. The `send` verb
      // builds the message without one because the instant belongs to the task,
      // and the task is the unit of work minted right here — so the birth stamp
      // and the handling clock are the same clock, by construction.
      return unitOfWork().execute((uow) => handler(stamped(message, () => uow.now()), uow))
    },

    subscribe(
      commandName: string,
      handler: (message: CommandMessage, uow: UnitOfWork) => Promise<unknown>,
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
