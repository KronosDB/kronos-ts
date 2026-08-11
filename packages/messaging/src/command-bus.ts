import type { CommandMessage } from "./message.js"

/**
 * The command bus — low-level infrastructure for dispatching command messages
 * to handlers. The bus is swappable: SimpleCommandBus for local, Axon Server connector
 * for distributed.
 *
 * Users don't interact with the bus directly — they use the CommandGateway.
 */
/**
 * Optional per-subscription routing hints. `group` names the CONSUMER GROUP a
 * handler joins — distributed transports derive queue names from it so two
 * replicas of the same group compete, and different groups do not. Absent, the
 * transport falls back to the app identity (today's behaviour).
 */
export interface SubscribeOptions {
  readonly group?: string
}

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
   * Subscribe a handler for the given command name. The handler is invoked
   * inside the active UnitOfWork — module-level accessors
   * (`getResource`, `setResource`, `on`, `onError`, …) read/write that UoW's
   * ALS-backed state.
   *
   * Plan 03-04 (CTX-04 / D-34): handler signature dropped its `ctx`
   * parameter. The ProcessingContext type is gone.
   */
  subscribe(
    commandName: string,
    handler: (message: CommandMessage) => Promise<unknown>,
    options?: SubscribeOptions,
  ): void
}
