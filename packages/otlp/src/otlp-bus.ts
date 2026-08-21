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
 * const commandBus = otlpCommandBus(interceptingCommandBus(localCommandBus(uow), correlation), exporter)
 * ```
 */
export function otlpCommandBus<B extends CommandBus<any>>(next: B, exporter: OtlpExporter): B {
  return {
    ...next,

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
        const result = await next.dispatch(propagated)
        span.end()
        return result
      } catch (error) {
        span.fail(error)
        throw error
      }
    },

    subscribe(commandName, handler): void {
      next.subscribe(commandName, handler)
    },
    // CAPABILITY-PRESERVING. `B` in, `B` out, over a spread of everything the
    // wrapped bus had — so tracing a bus that mints correlating units of work
    // yields a bus that still mints them, and a handler demanding
    // `HandlerContext<CorrelatingUnitOfWork>` still fits behind it. Typed
    // `(CommandBus) => CommandBus` this erased `U` outright, which made
    // tracing silently incompatible with correlation: the runtime worked and
    // the build did not.
  } as B
}

/**
 * A {@link QueryBus} that traces `query`. Everything else on the bus —
 * subscriptions, subscription queries, updates — delegates unchanged.
 */
export function otlpQueryBus<B extends QueryBus<any>>(next: B, exporter: OtlpExporter): B {
  return {
    ...next,

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
        const result = await next.query(propagated, uow)
        span.end()
        return result
      } catch (error) {
        span.fail(error)
        throw error
      }
    },

    subscribe(queryName, handler): void {
      next.subscribe(queryName, handler)
    },

    subscriptionQuery(message, bufferSize) {
      return next.subscriptionQuery(message, bufferSize)
    },

    subscribeToUpdates(message, bufferSize) {
      return next.subscribeToUpdates(message, bufferSize)
    },

    emitUpdate(queryName, filter, update, uow) {
      return next.emitUpdate(queryName, filter, update, uow)
    },

    completeSubscription(queryName, filter, uow) {
      return next.completeSubscription(queryName, filter, uow)
    },

    completeSubscriptionExceptionally(queryName, error, filter, uow) {
      return next.completeSubscriptionExceptionally(queryName, error, filter, uow)
    },
    // CAPABILITY-PRESERVING — see `otlpCommandBus`.
  } as B
}
