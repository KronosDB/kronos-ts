/**
 * A shutdown latch that tracks in-flight operations and enables
 * graceful shutdown by draining pending work.
 */
export type ShutdownLatch = {
  onShutdown(callback: () => void): () => void
  registerActivity(): ActivityHandle
  initiateShutdown(): Promise<void>
  readonly shuttingDown: boolean
  readonly activeCount: number
}

export type ActivityHandle = {
  end(): void
}

export class ShutdownInProgressError extends Error {
  constructor(message: string = "Shutdown in progress") {
    super(message)
    this.name = "ShutdownInProgressError"
  }
}

export function shutdownLatch(): ShutdownLatch {
  const callbacks = new Set<() => void>()
  let activeCount = 0
  let shuttingDown = false
  let drainPromise: Promise<void> | undefined
  let drainResolve: (() => void) | null = null

  function checkDrained() {
    if (shuttingDown && activeCount === 0 && drainResolve) {
      drainResolve()
      drainResolve = null
    }
  }

  return {
    onShutdown(callback) {
      if (shuttingDown) callback()
      else callbacks.add(callback)
      return () => { callbacks.delete(callback) }
    },
    registerActivity(): ActivityHandle {
      if (shuttingDown) {
        throw new ShutdownInProgressError()
      }

      activeCount++
      let ended = false

      return {
        end() {
          if (ended) return
          ended = true
          activeCount--
          checkDrained()
        },
      }
    },

    initiateShutdown(): Promise<void> {
      shuttingDown = true
      for (const callback of callbacks) {
        try { callback() } catch (error) { console.error("Messaging shutdown callback failed", error) }
      }
      callbacks.clear()

      if (activeCount === 0) {
        return Promise.resolve()
      }

      drainPromise ??= new Promise((resolve) => {
        drainResolve = resolve
      })
      return drainPromise
    },

    get shuttingDown() {
      return shuttingDown
    },

    get activeCount() {
      return activeCount
    },
  }
}
