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
  if (!Number.isSafeInteger(maxBufferSize) || maxBufferSize <= 0) throw new RangeError("maxBufferSize must be a positive integer")
  const buffer: T[] = []
  let permits = 0
  let isActive = true
  let completionRequested = false

  function fail(error: Error) {
    if (!isActive) return
    isActive = false
    buffer.length = 0
    onError?.(error)
  }
  function sendOne(value: T) {
    try { send(value) }
    catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)))
      throw error
    }
  }
  function finishIfDrained() {
    if (isActive && completionRequested && !buffer.length) {
      isActive = false
      onComplete?.()
    }
  }


  function drain() {
    while (permits > 0 && buffer.length > 0 && isActive) {
      const value = buffer.shift()!
      permits--
      sendOne(value)
    }
    finishIfDrained()
  }

  return {
    offer(value: T): boolean {
      if (!isActive || completionRequested) return false

      if (permits > 0) {
        permits--
        sendOne(value)
        return true
      }

      if (buffer.length >= maxBufferSize) {
        return false
      }

      buffer.push(value)
      return true
    },

    addPermits(count: number) {
      if (!Number.isSafeInteger(count) || count <= 0 || !Number.isSafeInteger(permits + count)) throw new RangeError("Permits must be positive safe integers")
      if (!isActive) return
      permits += count
      drain()
    },

    complete() {
      completionRequested = true
      finishIfDrained()
    },

    completeExceptionally(error: Error) {
      fail(error)
    },

    get active() {
      return isActive && !completionRequested
    },
  }
}
