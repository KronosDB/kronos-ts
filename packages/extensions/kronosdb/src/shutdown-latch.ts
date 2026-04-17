/**
 * A shutdown latch that tracks in-flight operations and enables
 * graceful shutdown by draining pending work.
 */
export interface ShutdownLatch {
  registerActivity(): ActivityHandle
  initiateShutdown(): Promise<void>
  readonly shuttingDown: boolean
  readonly activeCount: number
}

export interface ActivityHandle {
  end(): void
}

export class ShutdownInProgressError extends Error {
  constructor(message: string = "Shutdown in progress") {
    super(message)
    this.name = "ShutdownInProgressError"
  }
}

export function createShutdownLatch(): ShutdownLatch {
  let activeCount = 0
  let shuttingDown = false
  let drainResolve: (() => void) | null = null

  function checkDrained() {
    if (shuttingDown && activeCount === 0 && drainResolve) {
      drainResolve()
      drainResolve = null
    }
  }

  return {
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

      if (activeCount === 0) {
        return Promise.resolve()
      }

      return new Promise((resolve) => {
        drainResolve = resolve
      })
    },

    get shuttingDown() {
      return shuttingDown
    },

    get activeCount() {
      return activeCount
    },
  }
}
