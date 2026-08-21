import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  emptyMetadata,
  type Metadata,
  type QueryDescriptor,
  type QueryMessage,
} from "../messaging/messages.js"
import { generateIdentifier } from "../messaging/identifier.js"
import type { QueryBus } from "./bus.js"
import type { UnitOfWork } from "../unit-of-work/unit-of-work.js"

/**
 * The result of a subscription query — initial result plus a stream
 * of incremental updates.
 *
 * ```typescript
 * const result = subscriptionQuery(queryBus, GetCourseView, { courseId: "cs-101" })
 * const initial = await result.initialResult
 * console.log("Initial:", initial)
 *
 * for await (const update of result.updates) {
 *   console.log("Update:", update)
 * }
 * ```
 */
export type SubscriptionQueryResult = {
  /** The initial query result (same as a regular query dispatch). */
  readonly initialResult: Promise<unknown>
  /** Stream of incremental updates. Completes when the subscription is closed or completed by the emitter. */
  readonly updates: AsyncIterable<unknown>
  /** Close the subscription and release resources. */
  close(): void
}

/**
 * Internal update handler — receives updates from emitUpdate() calls
 * and buffers them for the AsyncIterable consumer.
 */
export type UpdateHandler = {
  /** The original subscription query message (for filter matching). */
  readonly query: QueryMessage
  /** Offer an update to the buffer. Returns false if buffer is full. */
  offer(update: unknown): boolean
  /** Mark the subscription as complete (no more updates). */
  complete(): void
  /** Mark the subscription as failed. */
  completeExceptionally(error: Error): void
  /** Whether this handler is still active. */
  readonly active: boolean
}

/**
 * Deferred update tasks, one list per unit of work. A WeakMap rather than a
 * field on `UnitOfWork` because this is the subscription-query feature's own
 * bookkeeping, not part of the unit-of-work contract; entries die with the UoW.
 */
const updateTasks = new WeakMap<UnitOfWork, Array<() => void>>()

/**
 * Defers a task to `uow`'s AFTER_COMMIT phase, or runs it immediately when no
 * unit of work was handed in (or it has already closed).
 *
 * Ensures subscription query updates are only emitted after the transaction
 * commits successfully. UoW presence is now decided by the parameter rather
 * than an ambient lookup.
 */
export function runAfterCommitOrImmediately(task: () => void, uow?: UnitOfWork): void {
  if (uow === undefined || uow.closed) {
    task()
    return
  }
  let tasks = updateTasks.get(uow)
  if (tasks === undefined) {
    tasks = []
    updateTasks.set(uow, tasks)
    const list = tasks
    uow.onAfterCommit(() => {
      for (const t of list) t()
    })
  }
  tasks.push(task)
}

/**
 * Creates a bounded async buffer that implements both UpdateHandler
 * (for the producer side) and provides an AsyncIterable (for the consumer).
 *
 * @param query The subscription query message
 * @param bufferSize Maximum number of buffered updates (default: 256)
 */
export function updateHandler(
  query: QueryMessage,
  bufferSize: number = 256,
): UpdateHandler & { iterable: AsyncIterable<unknown> } {
  const buffer: unknown[] = []
  let completed = false
  let error: Error | undefined
  let waiting: { resolve: (value: IteratorResult<unknown>) => void } | null = null

  function wake() {
    if (waiting && (buffer.length > 0 || completed)) {
      const w = waiting
      waiting = null
      if (buffer.length > 0) {
        w.resolve({ value: buffer.shift()!, done: false })
      } else {
        w.resolve({ value: undefined, done: true })
      }
    }
  }

  const handler: UpdateHandler & { iterable: AsyncIterable<unknown> } = {
    query,

    offer(update: unknown): boolean {
      if (completed) return false
      if (buffer.length >= bufferSize) return false
      buffer.push(update)
      wake()
      return true
    },

    complete() {
      completed = true
      wake()
    },

    completeExceptionally(err: Error) {
      error = err
      completed = true
      wake()
    },

    get active() {
      return !completed
    },

    iterable: {
      [Symbol.asyncIterator]() {
        return {
          next(): Promise<IteratorResult<unknown>> {
            // If there's buffered data, return immediately
            if (buffer.length > 0) {
              return Promise.resolve({ value: buffer.shift()!, done: false })
            }

            // If completed, signal done
            if (completed) {
              if (error) return Promise.reject(error)
              return Promise.resolve({ value: undefined, done: true })
            }

            // Wait for data or completion
            return new Promise((resolve, reject) => {
              waiting = {
                resolve(result) {
                  if (error && result.done) {
                    reject(error)
                  } else {
                    resolve(result)
                  }
                },
              }
            })
          },

          return(): Promise<IteratorResult<unknown>> {
            completed = true
            buffer.length = 0
            return Promise.resolve({ value: undefined, done: true })
          },
        }
      },
    },
  }

  return handler
}

// ---------------------------------------------------------------------------
// The EDGE verb that starts one. Like `send` and `query` it takes the bus as
// its first argument and holds nothing.
// ---------------------------------------------------------------------------

/**
 * Start a subscription query: the initial result plus the stream of updates
 * handlers emit for it via `ctx.emitUpdate`.
 */
export function subscriptionQuery<
  P extends StandardSchemaV1,
  R extends StandardSchemaV1 | undefined = undefined,
>(
  bus: QueryBus,
  descriptor: QueryDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
): SubscriptionQueryResult {
  return bus.subscriptionQuery({
    kind: "query",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: metadata ?? emptyMetadata(),
  })
}
