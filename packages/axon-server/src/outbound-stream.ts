/** Bounded single-consumer queue feeding a gRPC request stream. */
export type OutboundStream<T> = {
  send(message: T): void
  readonly iterable: AsyncIterable<T>
  /** Wait until the consumer has requested the next frame after this batch. Not a server acknowledgement. */
  flush(): Promise<void>
  readonly buffered: number
  close(): void
}

export function outboundStream<T>(maxBuffered = 4096): OutboundStream<T> {
  if (!Number.isSafeInteger(maxBuffered) || maxBuffered <= 0) throw new RangeError("maxBuffered must be a positive integer")
  let resolve: ((value: IteratorResult<T>) => void) | undefined
  const queue: T[] = []
  let closed = false
  let claimed = false
  let handingOff = false
  let flushed: { promise: Promise<void>; resolve(): void; reject(error: Error): void } | undefined
  function acknowledgeRead() {
    handingOff = false
    if (!queue.length) { flushed?.resolve(); flushed = undefined }
  }
  const done = (): IteratorResult<T> => ({ value: undefined, done: true })
  function close() {
    closed = true
    if (!queue.length && !handingOff) { flushed?.resolve(); flushed = undefined }
    resolve?.(done())
    resolve = undefined
  }
  return {
    get buffered() { return queue.length },
    flush() {
      if (!queue.length && !handingOff) return Promise.resolve()
      if (closed) return Promise.reject(new Error("Outbound stream closed before flush"))
      if (!flushed) {
        let resolve!: () => void, reject!: (error: Error) => void
        const promise = new Promise<void>((a, b) => { resolve = a; reject = b })
        flushed = { promise, resolve, reject }
      }
      return flushed.promise
    },
    send(message) {
      if (closed) throw new Error("Outbound stream is closed")
      if (resolve) {
        const waiter = resolve
        resolve = undefined
        handingOff = true
        waiter({ value: message, done: false })
      } else {
        if (queue.length >= maxBuffered) throw new Error("Outbound stream buffer overflow")
        queue.push(message)
      }
    },
    iterable: {
      [Symbol.asyncIterator]() {
        if (claimed) throw new Error("Outbound stream supports one consumer")
        claimed = true
        return {
          next(): Promise<IteratorResult<T>> {
            acknowledgeRead()
            if (queue.length) {
              handingOff = true
              return Promise.resolve({ value: queue.shift()!, done: false })
            }
            if (closed) return Promise.resolve(done())
            if (resolve) return Promise.reject(new Error("Concurrent outbound stream reads are not supported"))
            return new Promise((r) => { resolve = r })
          },
          return(): Promise<IteratorResult<T>> {
            queue.length = 0
            flushed?.reject(new Error("Outbound stream cancelled before flush"))
            flushed = undefined
            handingOff = false
            close()
            return Promise.resolve(done())
          },
        }
      },
    },
    close,
  }
}
