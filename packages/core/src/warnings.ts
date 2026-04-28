/**
 * Smallest viable logger interface (D-51) — just `warn(msg: string)`.
 * Phase 5 only emits startup warnings for in-memory defaults; richer levels
 * are intentionally NOT in scope.
 */
export interface WarningLogger {
  warn(msg: string): void
}

export interface WarningChannelOptions {
  /** When true, suppress all warnings (kronos({ quiet: true })). */
  quiet?: boolean
  /** When set, route warnings here instead of console.warn. `quiet` takes precedence over `logger`. */
  logger?: WarningLogger
}

export interface WarningChannel {
  emit(msg: string): void
}

export function createWarningChannel(options: WarningChannelOptions = {}): WarningChannel {
  return {
    emit(msg: string): void {
      if (options.quiet) return
      if (options.logger) {
        options.logger.warn(msg)
        return
      }
      console.warn(msg)
    },
  }
}
