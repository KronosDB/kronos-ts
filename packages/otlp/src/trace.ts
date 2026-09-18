import type { Logger } from "@kronos-ts/core"
import { SpanKind, type Attributes, type OtlpExporter, type OtlpSpan, type SpanKindValue, type TraceContext } from "./otlp-exporter.js"
import { formatTraceparent } from "./traceparent.js"

// ---------------------------------------------------------------------------
// A TRACE SCOPE — the one value that says "you are here" in a trace.
//
// It is a plain value, handed explicitly: `otlpHandler` puts one on `ctx` as
// `ctx.trace`, an HTTP edge builds one from the incoming header, and an
// outbound client takes one as a trailing argument. There is no ambient
// lookup and nothing global: if a piece of code has a `Trace`, somebody gave
// it one, and that is how per-invocation identity reaches code that a shared
// resource could never discover on its own.
// ---------------------------------------------------------------------------

/** How a child span is named and decorated. Nothing about a call's arguments ever goes here. */
export type SpanOptions = {
  /** The span's name. Defaults to the function's name, then `"operation"`. Never derived from arguments. */
  readonly name?: string
  readonly attributes?: Attributes
  /** Defaults to INTERNAL. */
  readonly kind?: SpanKindValue
}

export type Trace = {
  /** The span this scope is under, or `undefined` for an inert scope. */
  readonly context: TraceContext | undefined
  /** The W3C `traceparent` header value for this scope — what an outbound call puts on the wire. */
  readonly traceparent: string | undefined
  /**
   * `fn`, with every call a child span of this scope. The callable contract is
   * preserved: a sync function stays sync and rethrows synchronously; a
   * promise-returning one has its span end when the promise settles. Promise
   * identity is not preserved. Arguments and results are never recorded.
   */
  span<F extends (...args: any[]) => any>(fn: F, options?: SpanOptions): F
  /**
   * Run `fn` inside a child span, handing it the CHILD scope — for work that
   * needs the child's identity (an outbound client putting `child.traceparent`
   * on a request) rather than merely being timed.
   */
  run<T>(name: string, fn: (child: Trace) => T, options?: Omit<SpanOptions, "name">): T
  /** `logger`, bound to this scope's trace and span ids. */
  log(logger: Logger): Logger
}

/** Ends `span` when `result` settles; sync results end it now. Returns what the caller gets. */
function settle<T>(span: OtlpSpan, result: T): T {
  if (isThenable(result)) {
    return (result as unknown as PromiseLike<unknown>).then(
      (value) => {
        span.end()
        return value
      },
      (error: unknown) => {
        span.fail(error)
        throw error
      },
    ) as unknown as T
  }
  span.end()
  return result
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && typeof (value as { then?: unknown }).then === "function"
}

/**
 * A scope under `parent` — or a new root when `parent` is absent, which is
 * what an HTTP edge with no incoming header wants.
 *
 * ```ts
 * const incoming = trace(exporter, traceparentOf(headersAsMetadata(req.headers)))
 * await incoming.run("POST /orders", (span) =>
 *   send(commandBus, PlaceOrder, body, { traceparent: span.traceparent }))
 * ```
 */
export function trace(exporter: OtlpExporter, parent?: TraceContext): Trace {
  const context: TraceContext | undefined = parent
    ? { traceId: parent.traceId, spanId: parent.spanId, ...(parent.sampled === false ? { sampled: false } : {}) }
    : undefined

  const start = (name: string, options?: Omit<SpanOptions, "name">): OtlpSpan =>
    exporter.startSpan({
      name,
      kind: options?.kind ?? SpanKind.INTERNAL,
      ...(context ? { parent: context } : {}),
      ...(options?.attributes ? { attributes: options.attributes } : {}),
    })

  const scope: Trace = {
    context,
    traceparent: context ? formatTraceparent(context) : undefined,

    span(fn, options) {
      const name = options?.name ?? (fn.name || "operation")
      const traced = function (this: unknown, ...args: unknown[]) {
        const span = start(name, options)
        let result: unknown
        try {
          result = fn.apply(this, args)
        } catch (error) {
          span.fail(error)
          throw error
        }
        return settle(span, result)
      }
      return traced as typeof fn
    },

    run(name, fn, options) {
      const span = start(name, options)
      const child = trace(exporter, span)
      let result: ReturnType<typeof fn>
      try {
        result = fn(child)
      } catch (error) {
        span.fail(error)
        throw error
      }
      return settle(span, result)
    },

    log(logger) {
      return context ? logger.with({ trace_id: context.traceId, span_id: context.spanId }) : logger
    },
  }
  return scope
}

/**
 * A scope that records nothing: `span` returns the function it was given,
 * `run` hands itself to the callback, `log` returns the logger unchanged. For
 * a client that takes an optional trace and runs in a deployment without one:
 *
 * ```ts
 * charge: (card, amount, trace?: Trace) =>
 *   (trace ?? inertTrace).run("payments.charge", (child) => post(url, body, { traceparent: child.traceparent }))
 * ```
 */
export const inertTrace: Trace = {
  context: undefined,
  traceparent: undefined,
  span: (fn) => fn,
  run: (_name, fn) => fn(inertTrace),
  log: (logger) => logger,
}
