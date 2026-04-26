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
   *
   * The bus auto-nests via ALS (CTX-02): when called outside a UnitOfWork,
   * `runInUoW` creates one; when called inside an active UoW (handler-internal
   * re-dispatch), the active UoW is reused. No explicit context parameter is
   * threaded.
   */
  dispatch(message: CommandMessage): Promise<unknown>

  /**
   * Subscribe a handler for the given command name.
   * The handler receives the command message and a ProcessingContext
   * created by the bus's UnitOfWork.
   *
   * The handler signature retains the `ctx` parameter until Plan 04 deletes
   * `ProcessingContext` itself.
   */
  subscribe(
    commandName: string,
    handler: (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>,
  ): void
}
