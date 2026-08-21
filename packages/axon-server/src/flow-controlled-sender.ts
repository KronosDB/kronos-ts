/**
 * Flow-controlled sender for subscription query updates.
 *
 * Implements request-based backpressure: the receiver requests N updates,
 * and the sender only sends when permits are available. When permits are
 * exhausted, updates are buffered until more permits arrive.
 *
 * Aligned with Java's `FlowControlledResponseSender`.
 */
export type FlowControlledSender<T> = {
  /** Send an update. Buffers if no permits available. */
  offer(value: T): boolean
  /** Grant additional permits to the sender. */
  addPermits(count: number): void
  /** Complete the stream normally. */
  complete(): void
  /** Complete the stream with an error. */
  completeExceptionally(error: Error): void
  /** Whether the stream is still active. */
  readonly active: boolean
}

/**
 * Creates a flow-controlled sender that buffers updates when permits are
 * exhausted and drains the buffer when new permits arrive.
 *
 * @param send Function called to actually send an update downstream.
 * @param maxBufferSize Maximum number of updates to buffer. Default: 256.
 */
export function flowControlledSender<T>(
  send: (value: T) => void,
  onComplete?: () => void,
  onError?: (error: Error) => void,
  maxBufferSize: number = 256,
): FlowControlledSender<T> {
  const buffer: T[] = []
  let permits = 0
  let isActive = true

  function drain() {
    while (permits > 0 && buffer.length > 0 && isActive) {
      const value = buffer.shift()!
      permits--
      try {
        send(value)
      } catch (err) {
        console.warn("FlowControlledSender: send error", err)
      }
    }
  }

  return {
    offer(value: T): boolean {
      if (!isActive) return false

      if (permits > 0) {
        permits--
        send(value)
        return true
      }

      if (buffer.length >= maxBufferSize) {
        return false
      }

      buffer.push(value)
      return true
    },

    addPermits(count: number) {
      permits += count
      drain()
    },

    complete() {
      isActive = false
      buffer.length = 0
      if (onComplete) onComplete()
    },

    completeExceptionally(error: Error) {
      isActive = false
      buffer.length = 0
      if (onError) onError(error)
    },

    get active() {
      return isActive
    },
  }
}
