import type { ResilienceConfig } from "./resilience.js"

/** Retry failures of the async stream, not just synchronous stream creation. */
export function streamRecovery(
  reopen: () => void,
  canReconnect: () => boolean,
  config: Partial<ResilienceConfig> = {},
) {
  const log = (message: string) => {
    try {
      ;(config.log ?? console.warn)(message)
    } catch {
      /* Diagnostics cannot prevent recovery. */
    }
  }
  let timer: ReturnType<typeof setTimeout> | undefined
  let stopped = false
  let attempts = 0
  let openedAt = Date.now()

  function failed(error: unknown) {
    if (stopped || timer || !canReconnect()) return
    if (config.isRetryable?.(error) === false || attempts >= (config.maxAttempts ?? 30)) {
      log(`Provider stream recovery exhausted: ${String(error)}`)
      return
    }
    // Keep a positive floor even when jitter is zero. An immediately-ended
    // async generator must never create a microtask reconnect loop.
    const cap = Math.min(
      (config.initialDelayMs ?? 100) * (config.multiplier ?? 2) ** attempts++,
      config.maxDelayMs ?? 30000,
    )
    const delay = Math.max(1, cap * (0.5 + Math.random() * 0.5))
    log(`Provider stream failed; reconnecting in ${Math.round(delay)}ms: ${String(error)}`)
    timer = setTimeout(() => {
      timer = undefined
      if (stopped || !canReconnect()) return
      openedAt = Date.now()
      try {
        reopen()
      } catch (error) {
        failed(error)
      }
    }, delay)
    timer.unref?.()
  }

  return {
    failed,
    received() {
      // Subscription acknowledgements alone do not prove a stable connection.
      if (Date.now() - openedAt >= 30000) attempts = 0
    },
    restart() {
      if (stopped || !canReconnect()) return
      clearTimeout(timer)
      timer = undefined
      attempts = 0
      openedAt = Date.now()
      try {
        reopen()
      } catch (error) {
        failed(error)
      }
    },
    stop() {
      stopped = true
      clearTimeout(timer)
      timer = undefined
    },
  }
}
