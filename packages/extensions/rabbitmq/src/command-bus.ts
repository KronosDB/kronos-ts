import type { CommandBus, CommandMessage } from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import {
  correlationDataDispatchInterceptor,
  interceptingCommandBus,
  runInNewUoW,
} from "@kronos-ts/messaging"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

export interface RabbitMqCommandEnvelope {
  readonly kind: "command"
  readonly requestId: string
  readonly message: CommandMessage
  readonly expectsReply: boolean
  readonly timeoutMs: number
}

export interface RabbitMqCommandReplyEnvelope {
  readonly requestId: string
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
}

export interface RabbitMqCommandTransport {
  dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope>
  subscribe(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): void | Promise<void>
}

export interface RabbitMqCommandBusOptions {
  readonly localSegment: CommandBus
  readonly transport: RabbitMqCommandTransport
  readonly config: RabbitMqResolvedConfig
}

/**
 * A RabbitMQ-routed command bus.
 *
 * ## Correlation lineage and the interceptor layer
 *
 * The returned bus is wrapped in {@link interceptingCommandBus} carrying
 * {@link correlationDataDispatchInterceptor}, so the interceptor chain sits
 * OUTSIDE the local-vs-remote routing decision below.
 *
 * That ordering is AxonFramework's, not an invention here. In AF4,
 * `DistributedCommandBus.dispatch` reads
 * `CommandMessage<? extends C> interceptedCommand = intercept(command);`
 * and only then calls `commandRouter.findDestination(...)`; `AxonServerCommandBus.dispatch`
 * is literally `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)`.
 * In AF5 the same property is expressed by decorator order —
 * `InterceptingCommandBus.DECORATION_ORDER` vs
 * `DistributedCommandBusConfigurationEnhancer.DISTRIBUTED_COMMAND_BUS_ORDER =
 * DECORATION_ORDER - 50` — which stacks the buses as
 * `InterceptingCommandBus → DistributedCommandBus → SimpleCommandBus`.
 * Interception happens once, at the top, and therefore covers BOTH branches.
 *
 * Wrapping outside is what fixes the lineage defect: `dispatch()` routes either
 * to `localSegment` or to the broker, and only the local branch used to reach an
 * interceptor (the app's default bus is an intercepting one). A command routed
 * over the broker left with no `correlationId` / `causationId` at all.
 *
 * Double application is harmless. When `localSegment` is the app's default bus
 * the interceptor runs a second time inside it, but
 * `correlationDataDispatchInterceptor` reads the SAME correlation data off the
 * SAME active UnitOfWork and merges it with `mergeMetadata`, which is
 * `{ ...base, ...override }` — re-merging identical keys with identical values
 * is a no-op. AF makes the same trade: `AxonServerCommandBus` deliberately does
 * not propagate `registerDispatchInterceptor` to its local segment, but nothing
 * guards a user who registers on both.
 *
 * Note that handler interceptors are NOT part of this wrap — inbound commands
 * from the broker are handled through `localSegment.subscribe`, which keeps
 * whatever handler interception the app installed.
 */
export function rabbitMqCommandBus(options: RabbitMqCommandBusOptions): CommandBus {
  const localHandlers = new Set<string>()
  const { localSegment, transport, config } = options

  const routing: CommandBus = {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const commandName = qualifiedNameToString(message.name)
      const preferLocal = config.commands.preferLocalHandlers && !config.commands.alwaysUseDistributedBus
      if (preferLocal && localHandlers.has(commandName)) {
        return localSegment.dispatch(message)
      }

      const envelope: RabbitMqCommandEnvelope = {
        kind: "command",
        requestId: message.identifier,
        message,
        expectsReply: true,
        timeoutMs: config.commands.defaultTimeoutMs,
      }
      const reply = await transport.dispatch(envelope)
      if (!reply.ok) throw deserializeRemoteError(reply.error)
      return reply.result
    },

    subscribe(commandName: string, handler: (message: CommandMessage) => Promise<unknown>): void {
      localHandlers.add(commandName)
      localSegment.subscribe(commandName, handler)
      void transport.subscribe(commandName, async (envelope) => {
        try {
          // AF5 parity: an inbound distributed command is handled in its
          // own fresh UnitOfWork. Correlation/causation lineage rides on
          // the command message metadata, which crosses the wire intact.
          const result = await runInNewUoW(envelope.message.metadata, () =>
            handler(envelope.message),
          )
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },
  }

  const bus = interceptingCommandBus(routing)
  bus.registerDispatchInterceptor(correlationDataDispatchInterceptor())
  return bus
}

function serializeError(error: unknown): RabbitMqCommandReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}

function deserializeRemoteError(error: RabbitMqCommandReplyEnvelope["error"]): Error {
  const result = new Error(error?.message ?? "Remote command handling failed")
  result.name = error?.name ?? "RemoteCommandHandlingError"
  if (error?.stack) result.stack = error.stack
  return result
}
