import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { StateManagerLike } from "../state/load.js"
import type { CommandBus } from "../buses/command-bus.js"
import type { CommandHandlerDefinition } from "./command-handler.js"
import type { EventQuery } from "../query/event-query.js"
import { registerEventFlush, type EventFlushStore } from "../unit-of-work/event-flush.js"
import type { EventScheduler } from "../processor/event-scheduler.js"
import { handlerContext } from "./handler-context.js"
import type { CommandMessage, EventMessage } from "../messages/message.js"
import type { QueryBus } from "../buses/query-bus.js"
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
export interface CommandInvocationDeps {
  /** Backs `ctx.load`. Omit for handlers with no event-sourced state. */
  readonly stateManager?: StateManagerLike
  /** Backs `ctx.send`. */
  readonly commandBus?: CommandBus
  /** Backs `ctx.query` and `ctx.emitUpdate`. */
  readonly queryBus?: QueryBus
  /** Backs `ctx.schedule` / `ctx.scheduleAfter` / `ctx.cancelSchedule`. */
  readonly eventScheduler?: EventScheduler
  /** Where `ctx.append`'s buffer is flushed at PREPARE_COMMIT. */
  readonly eventStore?: EventFlushStore
  /** Tags derived at flush time. */
  readonly tagResolver?: (event: EventMessage) => Array<{ key: string; value: string }>
}

/**
 * Creates a command handler invocation function: given the incoming command
 * and the unit of work the bus opened for it, seed lineage, register the
 * event flush, build the context and call the handler.
 */
export function commandInvocation(
  handler: CommandHandlerDefinition<any, any>,
  deps: CommandInvocationDeps,
) {
  return async (message: CommandMessage, uow: UnitOfWork): Promise<unknown> => {
    // Nothing seeds lineage here. The context carries the HANDLED MESSAGE'S
    // metadata outward on `ctx.send` / `ctx.query` / `ctx.append`, so whatever
    // is on the incoming command — including the correlationId/causationId
    // `lineage` stamped as it crossed the bus — propagates without this layer
    // knowing the rule. That is the same mechanism the event leg uses.

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

    // The context closes over BOTH the unit of work and the command being
    // handled: the unit of work is the task, the message is the cause that
    // `ctx.send`/`ctx.append`/`ctx.query` stamp onto what they emit.
    return handler.handler(message, handlerContext({ uow, ...deps, message }))
  }
}

/**
 * Subscribes each handler onto the command bus with the {@link commandInvocation}
 * wrapper that builds its per-invocation context.
 */
export function subscribeCommandHandlers(
  handlers: ReadonlyArray<CommandHandlerDefinition<any, any>>,
  deps: { commandBus: CommandBus } & CommandInvocationDeps,
): void {
  for (const handler of handlers) {
    const commandName = qualifiedNameToString(handler.descriptor.name)
    // The handler's handler is already whatever the host composed onto it
    // (an observability package wraps handlers over the same public shapes) —
    // there is nothing to enhance here.
    deps.commandBus.subscribe(commandName, commandInvocation(handler, deps))
  }
}
