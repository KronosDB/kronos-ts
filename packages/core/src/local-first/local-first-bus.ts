import type { CommandBus } from "../command-handling/bus.js"
import type { QueryBus } from "../query-handling/bus.js"
import { qualifiedNameToString, type CommandMessage, type QueryMessage } from "../messaging/messages.js"

/**
 * Hand a command whose handler was subscribed through this bus to `local`
 * instead of to `next`.
 *
 * `next` is usually a transport over `local` — `rabbitMqCommandBus(local,
 * rabbit)` — so a handler subscribed here is subscribed on `local` too, and a
 * command for it never needs to leave the process:
 *
 * ```ts
 * const local = localCommandBus(unitOfWork)
 * const commandBus = interceptingCommandBus(
 *   localFirstCommandBus(rabbitMqCommandBus(local, rabbit), local),
 *   correlation,
 * )
 * ```
 *
 * `local` must be the bus `next` subscribes handlers on; a different one has no
 * handler to run. Interception goes OUTSIDE, so both branches are intercepted.
 *
 * A command handled locally is local work: a transport's shutdown does not wait
 * for it, and its request limits and timeouts do not apply to it.
 */
export function localFirstCommandBus<B extends CommandBus<any>>(next: B, local: CommandBus<any>): B {
  const subscribed = new Set<string>()
  return {
    ...next,

    async dispatch(message: CommandMessage) {
      return subscribed.has(qualifiedNameToString(message.name)) ? local.dispatch(message) : next.dispatch(message)
    },
    subscribe(commandName: string, handler: unknown) {
      subscribed.add(commandName)
      next.subscribe(commandName, handler as never)
    },
    // CAPABILITY-PRESERVING: `B` in, `B` out, like every bus wrapper.
  } as B
}

/**
 * Query-side counterpart of {@link localFirstCommandBus}: a query whose handler
 * was subscribed through this bus is asked of `local`. Subscription queries go
 * to `next` as before, because their updates are emitted through it.
 *
 * ```ts
 * const local = localQueryBus(unitOfWork)
 * const queryBus = localFirstQueryBus(kronosDbQueryBus(local, kdb), local)
 * ```
 */
export function localFirstQueryBus<B extends QueryBus<any>>(next: B, local: QueryBus<any>): B {
  const subscribed = new Set<string>()
  return {
    ...next,

    async query(message: QueryMessage): Promise<unknown> {
      return subscribed.has(qualifiedNameToString(message.name)) ? local.query(message) : next.query(message)
    },
    subscribe(queryName: string, handler: unknown) {
      subscribed.add(queryName)
      next.subscribe(queryName, handler as never)
    },
  } as B
}
