import {
  qualifiedNameToString,
  type CommandBus,
  type CommandMessage,
  type UnitOfWork,
} from "@kronos-ts/core"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

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
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
}

export type RabbitMqCommandTransport = {
  dispatch(envelope: RabbitMqCommandEnvelope): Promise<RabbitMqCommandReplyEnvelope>
  subscribe(
    commandName: string,
    handler: (envelope: RabbitMqCommandEnvelope) => Promise<RabbitMqCommandReplyEnvelope>,
  ): void | Promise<void>
}

/** Routing policy. Broker mechanics are the connection's, not knobs here. */
export type RabbitMqBusOptions = {
  /**
   * Handle locally when this instance subscribed a handler for the message.
   * Default: true. Set false to force every dispatch over the broker, which is
   * how you exercise the wire in a single-process test.
   */
  readonly preferLocal?: boolean
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
 * `dispatch` forks: a command this instance subscribed goes to `next`, and
 * anything else goes over the broker. `subscribe` does BOTH — it registers on
 * `next` and announces the name to the broker, so a remote instance can route
 * work here.
 *
 * The fork used to live in core, behind a connector seam, with this package
 * supplying only the wire half. It is one function again because the two halves
 * were never separable in practice: the reply timeout, the identity-named reply
 * queue and the prefer-next decision are one routing policy, and splitting
 * them across a package boundary bought an interface nobody else implemented.
 *
 * Below the fork, unchanged: competing consumers on durable per-command queues,
 * an identity-named exclusive reply queue, correlation-id matched replies,
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
 * Interception at the top covers BOTH branches of the fork. Wrapping on the
 * inside is the classic correlation defect: a command routed over the wire leaves
 * with no `correlationId` / `causationId` at all, because only the next branch
 * reaches the intercepting bus.
 *
 * ## Inbound
 *
 * A command the broker routes here is dispatched into `next` — not into a
 * privately-held handler reference. That is what makes the unit-of-work policy
 * you chose for `next` (say `postgresUnitOfWork(unitOfWork, pg)`) apply to
 * remote-origin work exactly as it applies to next work. Correlation rides on the
 * message metadata, which crosses the wire intact.
 */
export function rabbitMqCommandBus<U extends UnitOfWork = UnitOfWork>(
  next: CommandBus<U>,
  rabbit: RabbitMqCommandBusSource,
  options: RabbitMqBusOptions = {},
): CommandBus<U> {
  const transport = rabbit.commandTransport
  const localHandlers = new Set<string>()
  const preferLocal = options.preferLocal ?? true
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS

  return {
    async dispatch(unstamped: CommandMessage): Promise<unknown> {
      const commandName = qualifiedNameToString(unstamped.name)
      if (preferLocal && localHandlers.has(commandName)) {
        return next.dispatch(unstamped)
      }

      // A transport is not a task: it has no unit of work, so it has no clock.
      // A message that reaches the wire with no instant yet gets one from system
      // time here — the envelope crosses a process boundary and must be fully
      // formed. A locally-shortcut message is handed to `next` untouched
      // instead, so the task that handles it supplies the instant.
      const message = { ...unstamped, timestamp: unstamped.timestamp ?? Date.now() }

      const reply = await transport.dispatch({
        kind: "command",
        requestId: message.identifier,
        message,
        expectsReply: true,
        timeoutMs,
      })
      if (!reply.ok) throw deserializeRemoteError(reply.error)
      return reply.result
    },

    subscribe(commandName, handler): void {
      localHandlers.add(commandName)
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
