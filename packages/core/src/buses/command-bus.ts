import type { CommandMessage, Unstamped } from "../messages/message.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * The command bus — low-level infrastructure for dispatching command messages
 * to handlers. The bus is swappable: SimpleCommandBus for local, Axon Server
 * connector for distributed.
 *
 * A host does not build messages by hand — the `send` verb does that and
 * hands the result here.
 */
export interface CommandBus {
  /**
   * Dispatch a command message to its handler.
   *
   * Every command — primary or dispatched from inside another handler via
   * `ctx.send` — is handled in its OWN fresh UnitOfWork. There is deliberately
   * no unit-of-work parameter here: a command never joins its caller's unit of
   * work, so there is nothing to hand in. Lineage crosses the boundary on
   * `message.metadata`.
   *
   * The message may arrive {@link Unstamped}: the `send` verb cannot know the
   * task's instant, so the bus that mints the unit of work stamps `timestamp`
   * from `uow.now()`. A message that already carries one passes through
   * untouched.
   */
  dispatch(message: Unstamped<CommandMessage>): Promise<unknown>

  /**
   * Subscribe a handler for the given command name. The bus opens the unit of
   * work and hands it to the handler, which builds the `ctx` its handler
   * function receives.
   */
  subscribe(
    commandName: string,
    handler: (message: CommandMessage, uow: UnitOfWork) => Promise<unknown>,
  ): void
}
