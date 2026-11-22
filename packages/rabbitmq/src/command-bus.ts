import {
  qualifiedNameToString,
  type CommandBus,
  type CommandMessage,
  type SerializedError,
  type UnitOfWork,
} from "@kronos-ts/core"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"
import { deserializeError, serializeError } from "./remote-error.js"

export type RabbitMqCommandEnvelope = {
  readonly kind: "command"
  readonly requestId: string
  readonly message: CommandMessage
  readonly expectsReply: boolean
  readonly timeoutMs: number
}

export type RabbitMqCommandReplyEnvelope = {
  readonly requestId: string
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: SerializedError
}

export type RabbitMqCommandTransport = {
  dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope>
  subscribe(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): void | Promise<void>
}

/**
 * Per-bus knobs. Broker mechanics are the connection's, not knobs here. Every
 * dispatch goes over the broker; `localFirstCommandBus` / `localFirstQueryBus`
 * hand a co-located handler its messages without it.
 */
export type RabbitMqBusOptions = {
  /** How long to wait for a remote handler's reply. Default: 30000. */
  readonly timeoutMs?: number
}

const DEFAULT_TIMEOUT_MS = 30_000

/**
 * What the bus borrows from the connection. Narrower than
 * {@link RabbitMqConnection} on purpose: this names the whole dependency, so a
 * test can drive it with a fake transport and nothing else.
 */
export type RabbitMqCommandBusSource = {
  readonly config: RabbitMqResolvedConfig
  readonly commandTransport: RabbitMqCommandTransport
}

/**
 * A RabbitMQ-backed command bus over YOUR next segment.
 *
 * `dispatch` publishes every command to the broker, including one this
 * instance subscribed. The broker delivers it to one consumer of the command's
 * queue, which may be this process. `subscribe` does BOTH — it registers on
 * `next` and binds the command's queue on the broker, so the broker can route
 * work here.
 *
 * To handle a co-located command without the broker, wrap the bus:
 * `localFirstCommandBus(rabbitMqCommandBus(local, rabbit), local)`.
 *
 * On the broker: competing consumers on durable per-command queues, an
 * identity-named exclusive reply queue, correlation-id matched replies,
 * dead-lettering.
 *
 * ## Where the interceptors go
 *
 * OUTSIDE, always:
 *
 * ```ts
 * interceptingCommandBus(rabbitMqCommandBus(next, rabbit), correlation)
 * ```
 *
 * Interception at the top covers every dispatch. It goes outside
 * `localFirstCommandBus` too, so that it covers the local path and the broker
 * path. An intercept on `next` alone runs only where the command is handled:
 * the command leaves this process with no `correlationId` / `causationId`.
 *
 * ## Inbound
 *
 * A command the broker routes here is dispatched into `next` — not into a
 * privately-held handler reference. That is what makes the unit-of-work policy
 * you chose for `next` (say `postgresUnitOfWork(unitOfWork, pg)`) apply to
 * work the broker delivers exactly as it applies to work dispatched on `next`
 * directly. Correlation rides on the message metadata, which crosses the wire
 * intact.
 */
export function rabbitMqCommandBus<U extends UnitOfWork = UnitOfWork>(
  next: CommandBus<U>,
  rabbit: RabbitMqCommandBusSource,
  options: RabbitMqBusOptions = {},
): CommandBus<U> {
  const transport = rabbit.commandTransport
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS

  return {
    async dispatch(unstamped: CommandMessage): Promise<unknown> {
      const commandName = qualifiedNameToString(unstamped.name)

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed.
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }

      const reply = await transport.dispatch({
        kind: "command",
        requestId: message.identifier,
        message,
        expectsReply: true,
        timeoutMs,
      })
      if (!reply.ok) throw deserializeError(reply.error, { name: "RemoteCommandHandlingError", message: "Remote command handling failed" })
      return reply.result
    },

    subscribe(commandName, handler): void {
      next.subscribe(commandName, handler)

      // A handling failure comes back as `ok: false` on the reply, NOT as a
      // rejection: the transport acks a command it answered and reserves nack
      // (and therefore the dead-letter exchange) for a message it could not
      // process at all.
      void transport.subscribe(commandName, async (envelope) => {
        try {
          const result = await next.dispatch(envelope.message)
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },
  }
}
