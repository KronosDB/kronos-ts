import {
  qualifiedNameToString,
  type CommandMessage,
  type EventMessage,
} from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import type { CommandBus } from "./bus.js"
import type { CommandHandler } from "./handler.js"
import type { EventQuery } from "../event-sourcing/dcb-query.js"
import { registerEventFlush } from "../unit-of-work/event-flush.js"
import { commandHandlerContext, type CommandHandlerContext } from "./context.js"
import type { QueryBus, SubscriptionCapableQueryBus } from "../query-handling/bus.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// Command invocation — builds a FRESH handler context per invocation.
//
// The context is a closure over the unit of work the bus opened, the buses the
// caller already holds, and the stores of the handler being invoked. There is
// no configuration lookup in the dispatch hot path and no ambient state: what
// the handler can reach is exactly what was passed here.
// ---------------------------------------------------------------------------

/**
 * Everything `commandInvocation` needs, resolved once at subscribe time.
 * Replaces the `MinimalConfiguration` shim, which existed only so the
 * invocation could seed ambient async storage from string-keyed components.
 */
export type CommandInvocationDeps<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = {
  /** Backs `ctx.send`. */
  readonly commandBus?: CommandBus<U>
  /** Backs `ctx.query` and `ctx.emitUpdate`. */
  readonly queryBus?: QueryBus<U>
  /**
   * The entry's log — both halves of the atomic boundary. `ctx.load` sources
   * from it, and `ctx.append`'s buffer is flushed to it at PREPARE_COMMIT. Its
   * TYPE is what decides whether this handling may load a snapshotting state,
   * and whether it has the scheduling verbs at all.
   */
  readonly eventStore?: E
  /** Tags derived at flush time. */
  readonly tagResolver?: (event: EventMessage) => Array<{ key: string; value: string }>
}

/**
 * Creates a command handler invocation function: given the incoming command
 * and the unit of work the bus opened for it, seed correlation, register the
 * event flush, build the context and call the handler.
 */
export function commandInvocation<U extends UnitOfWork, E extends EventStore = EventStore>(
  handler: CommandHandler<any, any, CommandHandlerContext<E, SubscriptionCapableQueryBus<U>, U>>,
  deps: CommandInvocationDeps<U, E>,
) {
  return async (message: CommandMessage, uow: U): Promise<unknown> => {
    // Nothing is carried here. What an outgoing message inherits from the
    // command being handled is a HOST policy, expressed by wrapping the handler
    // in `correlatingHandler(next, from)` — this layer neither knows the policy
    // nor supplies one.

    // One flush per UnitOfWork.
    if (deps.eventStore) {
      registerEventFlush(uow, {
        eventStore: deps.eventStore,
        ...(deps.tagResolver ? { tagResolver: deps.tagResolver } : {}),
        ...(handler.appendCondition
          ? { appendCondition: (query: EventQuery) => handler.appendCondition!(message, query) }
          : {}),
      })
    }

    // The context closes over the unit of work — the task — and nothing else.
    // The MESSAGE reaches the handler as its first argument, which is where a
    // wrapper reads it from too.
    return handler.handler(message, commandHandlerContext({ uow, ...deps }))
  }
}

/**
 * Subscribes each handler onto the command bus with the {@link commandInvocation}
 * wrapper that builds its per-invocation context.
 */
export function subscribeCommandHandlers<U extends UnitOfWork, E extends EventStore = EventStore>(
  handlers: ReadonlyArray<CommandHandler<any, any, CommandHandlerContext<E, SubscriptionCapableQueryBus<U>, U>>>,
  deps: { commandBus: CommandBus<U> } & CommandInvocationDeps<U, E>,
): void {
  for (const handler of handlers) {
    const commandName = qualifiedNameToString(handler.descriptor.name)
    // The handler's handler is already whatever the host composed onto it
    // (an observability package wraps handlers over the same public shapes) —
    // there is nothing to enhance here.
    deps.commandBus.subscribe(commandName, commandInvocation(handler, deps))
  }
}
