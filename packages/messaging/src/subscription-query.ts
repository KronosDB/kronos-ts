import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import type { ProcessingContext } from "./processing-context.js"
import type { QueryMessage } from "./message.js"
import {
  processingStateStorage,
  computeIfAbsent,
  onAfterCommit,
} from "./processing-state.js"

/**
 * The result of a subscription query — initial result plus a stream
 * of incremental updates.
 *
 * ```typescript
 * const result = queryGateway.subscriptionQuery(GetCourseView, { courseId: "cs-101" })
 * const initial = await result.initialResult
 * console.log("Initial:", initial)
 *
 * for await (const update of result.updates) {
 *   console.log("Update:", update)
 * }
 * ```
 */
export interface SubscriptionQueryResult {
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
export interface UpdateHandler {
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

/** Resource key for deferred update tasks in ProcessingContext. */
const UPDATE_TASKS_KEY: ResourceKey<Array<() => void>> = resourceKey("subscriptionQueryUpdateTasks")

/**
 * Defers a task to AFTER_COMMIT if a UnitOfWork is active, otherwise runs
 * it immediately.
 *
 * This is the core pattern from AF5's `runAfterCommitOrImmediately`.
 * Ensures subscription query updates are only emitted after the
 * transaction commits successfully.
 *
 * Phase 3 / Plan 02 (CTX-03, D-32): UoW presence is now decided by ALS state
 * (`processingStateStorage.getStore() !== undefined`), NOT by the explicit
 * `_context` parameter — same precedent as `correlationDataDispatchInterceptor`
 * (Plan 02-03) and `getActiveTransaction` (Plan 02-04). The `_context`
 * parameter is retained until Plan 03 (signature strip) deletes it.
 */
export function runAfterCommitOrImmediately(
  _context: ProcessingContext | undefined,
  task: () => void,
): void {
  if (processingStateStorage.getStore() !== undefined) {
    const tasks = computeIfAbsent(UPDATE_TASKS_KEY, () => {
      const list: Array<() => void> = []
      onAfterCommit(() => {
        for (const t of list) t()
      })
      return list
    })
    tasks.push(task)
  } else {
    task()
  }
}

/**
 * Creates a bounded async buffer that implements both UpdateHandler
 * (for the producer side) and provides an AsyncIterable (for the consumer).
 *
 * @param query The subscription query message
 * @param bufferSize Maximum number of buffered updates (default: 256)
 */
export function createUpdateHandler(
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
