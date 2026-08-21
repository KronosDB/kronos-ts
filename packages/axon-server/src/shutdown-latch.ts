/**
 * A shutdown latch that tracks in-flight operations and enables
 * graceful shutdown by draining pending work.
 *
 * Usage:
 * - `registerActivity()` before starting a dispatch — returns a handle
 * - `handle.end()` when the dispatch completes
 * - `initiateShutdown()` prevents new activities and returns a promise
 *   that resolves when all in-flight operations complete
 *
 * This is the TypeScript equivalent of AF5's ShutdownLatch pattern.
 */
export type ShutdownLatch = {
  /**
   * Register an in-flight activity. Throws if shutdown is in progress.
   * Call `end()` on the returned handle when the activity completes.
   */
  registerActivity(): ActivityHandle

  /**
   * Begin graceful shutdown. Returns a promise that resolves when
   * all in-flight activities have completed.
   * New calls to `registerActivity()` will throw after this.
   */
  initiateShutdown(): Promise<void>

  /** Whether shutdown has been initiated. */
  readonly shuttingDown: boolean

  /** Number of currently in-flight activities. */
  readonly activeCount: number
}

export type ActivityHandle = {
  /** Mark this activity as complete. */
  end(): void
}

export class ShutdownInProgressError extends Error {
  constructor(message: string = "Shutdown in progress") {
    super(message)
    this.name = "ShutdownInProgressError"
  }
}

export function shutdownLatch(): ShutdownLatch {
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
