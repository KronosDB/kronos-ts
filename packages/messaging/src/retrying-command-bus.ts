import type { CommandBus } from "./command-bus.js"
import type { CommandMessage } from "./message.js"

/**
 * Determines whether a failed command dispatch should be retried.
 */
export interface RetryPolicy {
  /**
   * Returns the delay in ms before the next retry, or undefined to stop retrying.
   * @param error The error from the previous attempt
   * @param attempt The attempt number (0-based, so 0 = first failure)
   */
  shouldRetry(error: unknown, attempt: number): number | undefined
}

/**
 * Retries on transient errors (like AppendConditionError) with exponential backoff.
 * Non-transient errors are rethrown immediately.
 */
export function exponentialBackoffRetryPolicy(options?: {
  maxRetries?: number
  initialDelayMs?: number
  isTransient?: (error: unknown) => boolean
}): RetryPolicy {
  const maxRetries = options?.maxRetries ?? 5
  const initialDelayMs = options?.initialDelayMs ?? 10
  const isTransient = options?.isTransient ?? defaultIsTransient

  return {
    shouldRetry(error: unknown, attempt: number): number | undefined {
      if (attempt >= maxRetries) return undefined
      if (!isTransient(error)) return undefined
      return initialDelayMs * Math.pow(2, attempt)
    },
  }
}

function defaultIsTransient(error: unknown): boolean {
  if (error instanceof Error) {
    return error.name === "AppendConditionError"
  }
  return false
}

/**
 * A command bus decorator that retries failed dispatches based on a retry policy.
 *
 * When a command fails with a transient error (e.g., AppendConditionError from
 * an optimistic concurrency conflict), the entire dispatch is retried. This means
 * the handler re-sources state and re-makes its decision based on fresh data.
 *
 * Non-transient errors propagate immediately to the caller.
 */
export function createRetryingCommandBus(
  delegate: CommandBus,
  policy: RetryPolicy,
): CommandBus {
  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      let attempt = 0
      while (true) {
        try {
          return await delegate.dispatch(message)
        } catch (error) {
          const delay = policy.shouldRetry(error, attempt)
          if (delay === undefined) throw error

          attempt++
          if (delay > 0) {
            await new Promise((resolve) => setTimeout(resolve, delay))
          }
        }
      }
    },

    subscribe(commandName: string, handler: (message: CommandMessage) => Promise<unknown>) {
      delegate.subscribe(commandName, handler)
    },
  }
}
