/**
 * Connection resilience — PACKAGE-PRIVATE.
 *
 * Exponential backoff with full jitter, per-event attempt caps, terminal-error
 * short-circuit, and a warn-then-continue health probe. Nothing here is
 * exported from this package's barrel.
 *
 * It used to live in `@kronos-ts/core`, and it does not belong there. Core is
 * only what cannot be a helper, and this is a helper by every test: it touches
 * no core shape, no message, no unit of work — it is `setTimeout` and a loop
 * over a function anybody could have written. Its only consumers were the
 * transports and the pool, core's own `src/` never called it, and a helper
 * lives with its users. Same reasoning that moved each persistence package's
 * transaction glue out of core, applied one folder up.
 *
 * Each consuming package keeps its own copy, carrying only what it uses, so
 * each can also state its own retry policy without negotiating with the others.
 */

/** Classified retry sites used by transport extensions (D-97). */
export type RetryEvent =
  | "initial-connect"
  | "reconnect"
  | "per-operation"
  | "health-check"

export type ResilienceConfig = {
  /** Initial backoff delay (ms). Default: 100. */
  initialDelayMs: number
  /** Cap for backoff (ms). Default: 30_000. */
  maxDelayMs: number
  /** Max retry attempts before giving up. Per-event defaults applied if not provided. */
  maxAttempts: number
  /** Base for exponential growth. Default: 2. */
  multiplier: number
  /** Health-check threshold (ms) before warn-then-continue. Default: 5000. */
  healthCheckThresholdMs: number
  /** Optional logger. Defaults to console.warn. */
  log?: (msg: string) => void
  /** Per-attempt classification: returns false to short-circuit (terminal error). */
  isRetryable?: (err: unknown) => boolean
}

const DEFAULTS: Omit<ResilienceConfig, "log" | "isRetryable"> = {
  initialDelayMs: 100,
  maxDelayMs: 30_000,
  maxAttempts: 10,
  multiplier: 2,
  healthCheckThresholdMs: 5_000,
}

const ATTEMPTS_BY_EVENT: Record<RetryEvent, number> = {
  "initial-connect": 10,
  "reconnect": 30,
  "per-operation": 5,
  "health-check": 3,
}

function resolveConfig(
  event: RetryEvent,
  partial?: Partial<ResilienceConfig>,
): ResilienceConfig {
  return {
    ...DEFAULTS,
    maxAttempts: ATTEMPTS_BY_EVENT[event],
    ...partial,
  }
}

function fullJitterDelay(attempt: number, cfg: ResilienceConfig): number {
  const exponential = Math.min(
    cfg.initialDelayMs * Math.pow(cfg.multiplier, attempt),
    cfg.maxDelayMs,
  )
  return Math.random() * exponential
}

/**
 * Run `fn` with exponential-backoff full-jitter retries.
 *
 * Resolves with the first successful result, or rejects with the last error
 * after `maxAttempts` exhausted. If `isRetryable(err)` returns false on any
 * attempt, the error is rethrown immediately (terminal error short-circuit).
 *
 * D-101: opts may carry caller-supplied resilience overrides — this is the
 * per-extension knob path (`kronosDbConnection({ resilience: { ... } })`).
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  opts: { event: RetryEvent } & Partial<ResilienceConfig>,
): Promise<T> {
  const cfg = resolveConfig(opts.event, opts)
  let lastErr: unknown
  for (let attempt = 0; attempt < cfg.maxAttempts; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err
      if (cfg.isRetryable && !cfg.isRetryable(err)) throw err
      if (attempt === cfg.maxAttempts - 1) break
      const delay = fullJitterDelay(attempt, cfg)
      cfg.log?.(
        `[kronos:resilience] ${opts.event} attempt ${attempt + 1}/${cfg.maxAttempts} ` +
          `failed; retrying in ${Math.round(delay)}ms — ${
            (err as Error)?.message ?? String(err)
          }`,
      )
      await new Promise((r) => setTimeout(r, delay))
    }
  }
  throw lastErr
}

/**
 * Run a health-check probe with warn-then-continue semantics (D-100).
 *
 * Resolves WITHOUT throwing on success OR after `thresholdMs` elapses with
 * the probe still pending — production loops should NEVER stall on a flaky
 * health probe. Failures (rejection or threshold breach) emit a single warn
 * via `log` and otherwise resolve normally.
 */
export async function healthCheck(
  fn: () => Promise<void>,
  opts: { thresholdMs?: number; log?: (msg: string) => void } = {},
): Promise<void> {
  const threshold = opts.thresholdMs ?? DEFAULTS.healthCheckThresholdMs
  const log = opts.log ?? ((m: string) => console.warn(m))
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      fn(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error("health-check threshold exceeded")),
          threshold,
        )
      }),
    ])
  } catch (err) {
    log(
      `[kronos:resilience] health-check soft-failed (${
        (err as Error)?.message ?? String(err)
      }); continuing`,
    )
    // warn-then-continue: do NOT rethrow.
  } finally {
    if (timer) clearTimeout(timer)
  }
}
