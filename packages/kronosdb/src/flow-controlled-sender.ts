/**
 * Flow-controlled sender for subscription query updates.
 */
export type FlowControlledSender<T> = {
  offer(value: T): boolean
  addPermits(count: number): void
  complete(): void
  completeExceptionally(error: Error): void
  readonly active: boolean
}

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
