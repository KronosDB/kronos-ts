import type { QueryBus } from "./query-bus.js"
import type { QueryMessage } from "./message.js"
import type { ProcessingContext } from "./processing-context.js"
import type { SubscriptionQueryResult, UpdateHandler } from "./subscription-query.js"
import type { UnitOfWorkFactory } from "./unit-of-work.js"
import { createUpdateHandler, runAfterCommitOrImmediately } from "./subscription-query.js"
import { defaultUnitOfWorkFactory } from "./unit-of-work.js"
import { qualifiedNameToString } from "@kronos-ts/common"

/**
 * Simple in-process query bus with subscription query support.
 *
 * Direct queries are dispatched within a UnitOfWork.
 * Subscription queries receive an initial result plus a stream of
 * incremental updates emitted via `emitUpdate()`.
 *
 * Interceptor support is provided by wrapping with
 * {@link createInterceptingQueryBus}, following Java's pattern of
 * separating concerns (SimpleQueryBus vs InterceptingQueryBus).
 */
export function createSimpleQueryBus(
  unitOfWorkFactory?: UnitOfWorkFactory,
): QueryBus {
  const factory = unitOfWorkFactory ?? defaultUnitOfWorkFactory()
  const handlers = new Map<string, (message: QueryMessage, ctx: ProcessingContext) => Promise<unknown>>()

  // Active subscription query handlers, keyed by query identifier
  const subscriptions = new Map<string, UpdateHandler>()

  const bus: QueryBus = {
    async query(message: QueryMessage, context?: ProcessingContext): Promise<unknown> {
      const key = qualifiedNameToString(message.name)
      const handler = handlers.get(key)
      if (!handler) {
        throw new Error(`No handler registered for query "${key}"`)
      }

      const uow = factory(message.metadata)
      return uow.executeWithResult(async (ctx) => {
        return handler(message, ctx)
      })
    },

    subscribe(
      queryName: string,
      handler: (message: QueryMessage, ctx: ProcessingContext) => Promise<unknown>,
    ) {
      const existing = handlers.get(queryName)
      if (existing && existing !== handler) {
        throw new Error(
          `A different handler is already registered for query "${queryName}". ` +
          `Duplicate query handler subscriptions are not allowed.`,
        )
      }
      handlers.set(queryName, handler)
    },

    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      const queryId = message.identifier

      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      const initialResult = bus.query(message)

      return {
        initialResult,
        updates: updateHandler.iterable,
        close: () => {
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      const queryId = message.identifier

      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      return {
        [Symbol.asyncIterator]: () => updateHandler.iterable[Symbol.asyncIterator](),
        close: () => {
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    async emitUpdate(
      queryName: string,
      filter: (queryPayload: unknown) => boolean,
      update: unknown,
      context?: ProcessingContext,
    ): Promise<void> {
      runAfterCommitOrImmediately(context, () => {
        for (const [id, handler] of subscriptions) {
          if (!handler.active) {
            subscriptions.delete(id)
            continue
          }

          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (!filter(handler.query.payload)) continue

          const accepted = handler.offer(update)
          if (!accepted) {
            handler.completeExceptionally(
              new Error("Subscription query update buffer overflow"),
            )
            subscriptions.delete(id)
          }
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: (queryPayload: unknown) => boolean,
      context?: ProcessingContext,
    ): Promise<void> {
      runAfterCommitOrImmediately(context, () => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue

          handler.complete()
          subscriptions.delete(id)
        }
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: (queryPayload: unknown) => boolean,
      context?: ProcessingContext,
    ): Promise<void> {
      runAfterCommitOrImmediately(context, () => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue

          handler.completeExceptionally(error)
          subscriptions.delete(id)
        }
      })
    },
  }

  return bus
}
