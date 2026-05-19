import type { QueryBus, QueryMessage, SubscriptionQueryResult } from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import { runInNewUoW } from "@kronos-ts/messaging"
import type { RabbitMqResolvedConfig } from "./rabbitmq.js"

export interface RabbitMqQueryEnvelope {
  readonly kind: "query"
  readonly requestId: string
  readonly message: QueryMessage
  readonly timeoutMs: number
}

export interface RabbitMqQueryReplyEnvelope {
  readonly requestId: string
  readonly ok: boolean
  readonly result?: unknown
  readonly error?: {
    readonly name?: string
    readonly message: string
    readonly stack?: string
  }
}

export interface RabbitMqQueryTransport {
  dispatch(envelope: RabbitMqQueryEnvelope): Promise<RabbitMqQueryReplyEnvelope>
  subscribe(
    queryName: string,
    handler: (envelope: RabbitMqQueryEnvelope) => Promise<RabbitMqQueryReplyEnvelope>,
  ): void | Promise<void>
}

export interface RabbitMqQueryBusOptions {
  readonly localSegment: QueryBus
  readonly transport: RabbitMqQueryTransport
  readonly config: RabbitMqResolvedConfig
}

/**
 * Distributed query bus decorator.
 *
 * Direct request/reply queries (`query` + `subscribe`) route over RabbitMQ.
 * Subscription queries (`subscriptionQuery`, `subscribeToUpdates`, `emitUpdate`,
 * `completeSubscription*`) remain process-local and delegate to the local
 * segment — distributing subscription-query update streams over RabbitMQ is
 * intentionally out of scope for this version (see rabbitmq-extension-plan.md).
 */
export function createRabbitMqQueryBus(options: RabbitMqQueryBusOptions): QueryBus {
  const localHandlers = new Set<string>()
  const { localSegment, transport, config } = options

  return {
    async query(message: QueryMessage): Promise<unknown> {
      const queryName = qualifiedNameToString(message.name)
      const preferLocal = config.queries.preferLocalHandlers && !config.queries.alwaysUseDistributedBus
      if (preferLocal && localHandlers.has(queryName)) {
        return localSegment.query(message)
      }

      const envelope: RabbitMqQueryEnvelope = {
        kind: "query",
        requestId: message.identifier,
        message,
        timeoutMs: config.queries.defaultTimeoutMs,
      }
      const reply = await transport.dispatch(envelope)
      if (!reply.ok) throw deserializeRemoteError(reply.error)
      return reply.result
    },

    subscribe(queryName: string, handler: (message: QueryMessage) => Promise<unknown>): void {
      localHandlers.add(queryName)
      localSegment.subscribe(queryName, handler)
      void transport.subscribe(queryName, async (envelope) => {
        try {
          // AF5 parity: an inbound distributed query is handled in its own
          // fresh UnitOfWork. Correlation/causation lineage rides on the query
          // message metadata, which crosses the wire intact.
          const result = await runInNewUoW(envelope.message.metadata, () =>
            handler(envelope.message),
          )
          return { requestId: envelope.requestId, ok: true, result }
        } catch (error) {
          return { requestId: envelope.requestId, ok: false, error: serializeError(error) }
        }
      })
    },

    // Subscription queries stay process-local — see the doc comment above.
    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      return localSegment.subscriptionQuery(message, bufferSize)
    },

    subscribeToUpdates(
      message: QueryMessage,
      bufferSize?: number,
    ): AsyncIterable<unknown> & { close(): void } {
      return localSegment.subscribeToUpdates(message, bufferSize)
    },

    emitUpdate(
      queryName: string,
      filter: (queryPayload: unknown) => boolean,
      update: unknown,
    ): Promise<void> {
      return localSegment.emitUpdate(queryName, filter, update)
    },

    completeSubscription(
      queryName: string,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      return localSegment.completeSubscription(queryName, filter)
    },

    completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      return localSegment.completeSubscriptionExceptionally(queryName, error, filter)
    },
  }
}

function serializeError(error: unknown): RabbitMqQueryReplyEnvelope["error"] {
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack }
  }
  return { message: String(error) }
}

function deserializeRemoteError(error: RabbitMqQueryReplyEnvelope["error"]): Error {
  const result = new Error(error?.message ?? "Remote query handling failed")
  result.name = error?.name ?? "RemoteQueryHandlingError"
  if (error?.stack) result.stack = error.stack
  return result
}
