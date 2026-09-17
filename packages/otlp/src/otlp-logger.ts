import type { LogFields, LogLevel, Logger } from "@kronos-ts/core"
import type { AttributeValue, OtlpExporter } from "./otlp-exporter.js"

// ---------------------------------------------------------------------------
// The core `Logger` contract, exported over OTLP.
//
// Same four methods and `with` as `consoleLogger`; the destination is the
// exporter's `/v1/logs` batch. `trace_id`/`span_id` fields — what
// `loggingHandler` and `Trace.log` bind — become the record's trace and span
// ids, so a log line sits inside its span in any OTLP backend.
// ---------------------------------------------------------------------------

const LEVEL_RANK: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 }

export type OtlpLoggerOptions = {
  /** Records below this level are dropped before allocation. Default `"info"`. */
  readonly level?: LogLevel
}

/** A {@link Logger} whose records go to `exporter`. Build one per process, beside the exporter. */
export function otlpLogger(exporter: OtlpExporter, options: OtlpLoggerOptions = {}): Logger {
  const threshold = LEVEL_RANK[options.level ?? "info"]

  const make = (bound: LogFields): Logger => {
    const emit = (level: LogLevel, message: string, fields?: LogFields): void => {
      if (LEVEL_RANK[level] < threshold) return
      // Later fields win, and a later `null`/`undefined` CLEARS a bound key —
      // the only way a `with()`-bound field can be taken back for one record.
      const merged: Record<string, AttributeValue> = {}
      for (const source of [bound, fields]) {
        if (!source) continue
        for (const [key, value] of Object.entries(source)) {
          if (value === undefined || value === null) delete merged[key]
          else merged[key] = value
        }
      }
      const traceId = merged.trace_id
      const spanId = merged.span_id
      exporter.emitLog({
        time: Date.now(),
        level,
        message,
        attributes: merged,
        ...(typeof traceId === "string" && typeof spanId === "string" ? { trace: { traceId, spanId } } : {}),
      })
    }
    return {
      debug: (message, fields) => emit("debug", message, fields),
      info: (message, fields) => emit("info", message, fields),
      warn: (message, fields) => emit("warn", message, fields),
      error: (message, fields) => emit("error", message, fields),
      with: (fields) => make({ ...bound, ...fields }),
    }
  }

  return make({})
}
