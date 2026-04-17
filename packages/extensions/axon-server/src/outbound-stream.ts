/**
 * A queue-backed async iterable for feeding outbound messages to a
 * bidirectional gRPC stream. Buffers messages when the stream isn't
 * consuming, and resolves promises when the stream is waiting.
 */
export interface OutboundStream<T> {
  /** Send a message into the stream. */
  send(message: T): void
  /** The async iterable to pass to the gRPC client. */
  readonly iterable: AsyncIterable<T>
  /** Close the stream. */
  close(): void
}

export function createOutboundStream<T>(): OutboundStream<T> {
  let resolve: ((value: IteratorResult<T>) => void) | null = null
  const queue: T[] = []
  let closed = false

  return {
    send(message: T) {
      if (resolve) {
        const r = resolve
        resolve = null
        r({ value: message, done: false })
      } else {
        queue.push(message)
      }
    },

    iterable: {
      [Symbol.asyncIterator]() {
        return {
          next(): Promise<IteratorResult<T>> {
            const queued = queue.shift()
            if (queued) return Promise.resolve({ value: queued, done: false })
            if (closed) return Promise.resolve({ value: undefined as any, done: true })
            return new Promise((r) => { resolve = r })
          },
        }
      },
    },

    close() {
      closed = true
      if (resolve) {
        resolve({ value: undefined as any, done: true })
        resolve = null
      }
    },
  }
}
