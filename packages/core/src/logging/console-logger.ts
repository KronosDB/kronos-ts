import type { LogFields, LogLevel, LogRecord, Logger } from "./logger.js"

const LEVEL_ORDER: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 }

/** Tuning for {@link consoleLogger}. */
export type ConsoleLoggerOptions = {
  /** Records below this level are skipped. Default: `"debug"` (everything). */
  readonly level?: LogLevel
  /** Where a line goes. Default: `console.log` for debug/info, `console.error` for warn/error. */
  readonly write?: (line: string) => void
}

/**
 * A {@link Logger} that writes one JSON object per record — the shape every
 * log-shipping pipeline already knows how to ingest, with no dependency
 * beyond `console`.
 *
 * A record is `{ time, level, message, ...fields }`. `fields` is spread
 * directly rather than nested, because the common case is a handler grepping
 * its own output for `accountId` — nesting it would cost every reader an
 * extra path segment for no benefit. The one thing it must never do is
 * clobber the three reserved keys: a field named `time`, `level` or `message`
 * is moved under a nested `fields` object instead of overwriting the record's
 * own.
 *
 * No timers, no module-level state, nothing async — a call either writes a
 * line or, below `options.level`, does nothing at all.
 */
export function consoleLogger(options: ConsoleLoggerOptions = {}): Logger {
  const threshold = LEVEL_ORDER[options.level ?? "debug"]
  const { write } = options

  function emit(carried: LogFields, level: LogLevel, message: string, fields?: LogFields): void {
    if (LEVEL_ORDER[level] < threshold) return
    const merged = fields ? { ...carried, ...fields } : carried
    const line = toLine({ level, message, fields: merged, time: Date.now() })
    const sink = write ?? (level === "debug" || level === "info" ? console.log : console.error)
    sink(line)
  }

  function make(carried: LogFields): Logger {
    return {
      debug: (message, fields) => emit(carried, "debug", message, fields),
      info: (message, fields) => emit(carried, "info", message, fields),
      warn: (message, fields) => emit(carried, "warn", message, fields),
      error: (message, fields) => emit(carried, "error", message, fields),
      with: (fields) => make({ ...carried, ...fields }),
    }
  }

  return make({})
}

const RESERVED = new Set(["time", "level", "message"])

/** One record, rendered to its JSON line. INTERNAL — the reserved-key split lives here alone. */
function toLine(record: LogRecord): string {
  const out: Record<string, unknown> = {
    time: new Date(record.time).toISOString(),
    level: record.level,
    message: record.message,
  }
  let collided: Record<string, unknown> | undefined
  for (const [key, value] of Object.entries(record.fields)) {
    if (RESERVED.has(key)) {
      collided ??= {}
      collided[key] = value
    } else {
      out[key] = value
    }
  }
  if (collided) out.fields = collided
  return JSON.stringify(out)
}
