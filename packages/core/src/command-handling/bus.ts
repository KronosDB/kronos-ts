import type { CommandMessage } from "../messaging/messages.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * The command bus — low-level infrastructure for dispatching command messages
 * to handlers. The bus is swappable: LocalCommandBus for local, Axon Server
 * connector for distributed.
 *
 * A host does not build messages by hand — the `send` verb does that and
 * hands the result here.
 *
 * `U` is the unit of work this bus MINTS — whatever its factory produces. It
 * defaults to the bare {@link UnitOfWork}, so uncorrelated, unadapted usage
 * reads exactly as it always did. It is threaded because a handler can DEMAND
 * more than the bare handle: `CommandBus<CorrelatingUnitOfWork>` is the type of
 * a bus whose tasks carry a correlation map, and `CommandBus` (bare) is not
 * assignable to it. That is the conditional compile error — the demand exists
 * only for the hosts that composed one.
 */
export type CommandBus<U extends UnitOfWork = UnitOfWork> = {
  /**
   * Dispatch a command message to its handler.
   *
   * Every command — primary or dispatched from inside another handler via
   * `ctx.send` — is handled in its OWN fresh UnitOfWork. There is deliberately
   * no unit-of-work parameter here: a command never joins its caller's unit of
   * work, so there is nothing to hand in. Correlation crosses the boundary on
   * `message.metadata`.
   *
   * The message may arrive with NO `timestamp` — that field is optional on
   * `Message` precisely because of this moment: the `send` verb cannot know the
   * task's instant, because the task is the unit of work this bus is about to
   * mint. So the bus fills it from `uow.now()`, and a message that already
   * carries one passes through untouched.
   */
  dispatch(message: CommandMessage): Promise<unknown>

  /**
   * Subscribe a handler for the given command name. The bus opens the unit of
   * work and hands it to the handler, which builds the `ctx` its handler
   * function receives.
   */
  subscribe(
    commandName: string,
    handler: (message: CommandMessage, uow: U) => Promise<unknown>,
  ): void
}
