import type { CommandMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"

/**
 * The command bus — low-level infrastructure for dispatching command messages
 * to handlers. The bus is swappable: SimpleCommandBus for local, Axon Server connector
 * for distributed.
 *
 * Users don't interact with the bus directly — they use the CommandGateway.
 */
export interface CommandBus {
  /**
   * Dispatch a fully-formed command message to its handler.
   * Creates a UnitOfWork internally and drives the full lifecycle.
   */
  dispatch(message: CommandMessage, context?: ProcessingContext): Promise<unknown>

  /**
   * Subscribe a handler for the given command name.
   * The handler receives the command message and a ProcessingContext
   * created by the bus's UnitOfWork.
   */
  subscribe(
    commandName: string,
    handler: (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>,
  ): void
}
