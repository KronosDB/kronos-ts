import type { CommandBus, CommandMessage } from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import { runInNewUoW } from "@kronos-ts/messaging"
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

export function createRabbitMqCommandBus(options: RabbitMqCommandBusOptions): CommandBus {
  const localHandlers = new Set<string>()
  const { localSegment, transport, config } = options

  return {
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
