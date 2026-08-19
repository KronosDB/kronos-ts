import type { CommandBus, CommandMessage, QueryBus, QueryMessage } from "@kronos-ts/core"
import { qualifiedNameToString } from "@kronos-ts/core"
import { SpanKind, type OtlpExporter } from "./otlp-exporter.js"
import { messageAttributes } from "./otlp-handler.js"
import { traceparentOf, withTraceparent } from "./traceparent.js"

// ---------------------------------------------------------------------------
// The PRODUCER side.
//
// One span per dispatch, and the outgoing message leaves carrying that span's
// `traceparent`. The handler side (`otlpHandler`) is the single authority for
// handler spans — these wrappers pass `subscribe` through untouched, so a
// command gets exactly one dispatch span and exactly one handle span.
// ---------------------------------------------------------------------------

/**
 * A {@link CommandBus} that traces dispatch.
 *
 * The dispatch span continues the caller's trace when the incoming message
 * already carries one (the edge stamped it, or `ctx.send` carried the
 * handler's metadata outward), and starts a new trace when it does not.
 *
 * ```ts
 * const commandBus = otlpCommandBus(interceptingCommandBus(simpleCommandBus(uow), lineage), exporter)
 * ```
 */
export function otlpCommandBus(bus: CommandBus, exporter: OtlpExporter): CommandBus {
  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const span = exporter.startSpan({
        name: `dispatch(${qualifiedNameToString(message.name)})`,
        kind: SpanKind.PRODUCER,
        parent: traceparentOf(message.metadata),
        attributes: messageAttributes(message),
      })
      const propagated: CommandMessage = {
        ...message,
        metadata: withTraceparent(message.metadata, span),
      }
      try {
        const result = await bus.dispatch(propagated)
        span.end()
        return result
      } catch (error) {
        span.fail(error)
        throw error
      }
    },

    subscribe(commandName, handler): void {
      bus.subscribe(commandName, handler)
    },
  }
}

/**
 * A {@link QueryBus} that traces `query`. Everything else on the bus —
 * subscriptions, subscription queries, updates — delegates unchanged.
 */
export function otlpQueryBus(bus: QueryBus, exporter: OtlpExporter): QueryBus {
  return {
    async query(message: QueryMessage, uow?): Promise<unknown> {
      const span = exporter.startSpan({
        name: `query(${qualifiedNameToString(message.name)})`,
        kind: SpanKind.CLIENT,
        parent: traceparentOf(message.metadata),
        attributes: messageAttributes(message),
      })
      const propagated: QueryMessage = {
        ...message,
        metadata: withTraceparent(message.metadata, span),
      }
      try {
        const result = await bus.query(propagated, uow)
        span.end()
        return result
      } catch (error) {
        span.fail(error)
        throw error
      }
    },

    subscribe(queryName, handler): void {
      bus.subscribe(queryName, handler)
    },

    subscriptionQuery(message, bufferSize) {
      return bus.subscriptionQuery(message, bufferSize)
    },

    subscribeToUpdates(message, bufferSize) {
      return bus.subscribeToUpdates(message, bufferSize)
    },

    emitUpdate(queryName, filter, update, uow) {
      return bus.emitUpdate(queryName, filter, update, uow)
    },

    completeSubscription(queryName, filter, uow) {
      return bus.completeSubscription(queryName, filter, uow)
    },

    completeSubscriptionExceptionally(queryName, error, filter, uow) {
      return bus.completeSubscriptionExceptionally(queryName, error, filter, uow)
    },
  }
}
