// THE STRUCTURAL CONTRACT — a logger is four methods and a `with`, nothing
// more. No transport, no formatter and no destination lives here: this file
// says what a logger IS, and `console-logger.ts` is one destination among
// several — an OTLP-backed one (`@kronos-ts/otlp`) implements the same shape.

/** One field on a log record. Kept to what every destination can carry as-is. */
export type LogFields = Readonly<Record<string, string | number | boolean | null | undefined>>

/** The four levels a record can be at, ordered low to high severity. */
export type LogLevel = "debug" | "info" | "warn" | "error"

/**
 * A logger, structurally: four leveled write methods and `with`, which returns
 * a logger that merges `fields` into every record it goes on to write.
 *
 * `with` is how a caller carries context down a call chain without threading
 * it through every leveled call — {@link loggingHandler} uses exactly this to
 * hand a handler a logger already carrying the message's identity.
 */
export type Logger = {
  debug(message: string, fields?: LogFields): void
  info(message: string, fields?: LogFields): void
  warn(message: string, fields?: LogFields): void
  error(message: string, fields?: LogFields): void
  with(fields: LogFields): Logger
}

/**
 * One written record, in the shape any destination beyond `console` needs:
 * an OTLP exporter batches these, it does not format lines.
 */
export type LogRecord = {
  readonly level: LogLevel
  readonly message: string
  readonly fields: LogFields
  readonly time: number
}
