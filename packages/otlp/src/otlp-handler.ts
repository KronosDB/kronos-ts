import type { Message, MessageKind, Metadata, UnitOfWork } from "@kronos-ts/core"
import { describe, qualifiedNameToString, type Described } from "@kronos-ts/core"
import { SpanKind, type Attributes, type OtlpExporter, type SpanKindValue } from "./otlp-exporter.js"
import { metrics, type Metrics } from "./metrics.js"
import { trace, type Trace } from "./trace.js"
import { traceparentOf, withTraceparent } from "./traceparent.js"

// ---------------------------------------------------------------------------
// The CONSUMER side.
//
// A wrapper over the handler FUNCTION, in the shape the persistence packages
// use (`drizzleHandler(handler, db)`): take a handler, return a handler of the
// same shape. It reads NOTHING from the entry it was taken off — the span's name,
// its kind and whether it parents or links all come from the message being
// handled, which is where they honestly live.
//
// Three things happen per invocation, and they are how trace identity reaches
// everything else without an ambient context:
//
//   1. A span is opened for the handling.
//   2. The span is STAMPED onto the handled message's metadata as
//      `traceparent`, so `correlatingHandler`'s cargo carries it onto every
//      message the handler produces and onto every event it appends — the
//      same key that crosses transports and persists in the log.
//   3. A `Trace` scope under the span is SUPPLIED as `ctx.trace`, for child
//      spans, bound loggers and outbound clients.
//
// A fourth, per task: when the handler returns, a `commit` span is opened
// under the handler span and ended when the task completes or fails — the
// time from this handler's end to durability (event flush, adapter commit,
// after-commit work). A batch's handlers each get one; for a command it is
// exactly the post-handler work.
//
// And the three numbers you page on come with the span, not from a second
// wrapper: how many, how many failed (and as WHAT — the error's type, so a
// typed domain rejection is its own series), and how long. They share the
// span's name, so a series and its traces always agree. `ctx.metrics` is the
// same exporter bound to the same two attributes, for anything custom.
// Whether metrics leave the process at all is the EXPORTER's switch
// (`otlpExporter({ metrics: false })`), not a wiring decision.
// ---------------------------------------------------------------------------

const DURATION = "kronos.message.handler.duration"
const HANDLED = "kronos.messages.handled"
const FAILED = "kronos.messages.failed"

/** The error's TYPE — a constructor name, never its message. Bounded by the code's error vocabulary. */
function errorType(error: unknown): string {
  if (error instanceof Error) return error.name || error.constructor.name || "Error"
  return typeof error
}

/** The capability this wrapper supplies. A handler names it to reach `ctx.trace`. */
export type TraceCapability = {
  readonly trace: Trace
}

// `MetricsCapability` — `ctx.metrics` — lives with `Metrics` in `./metrics.ts`.
export type { Metrics }

/** The attributes a span over a message carries. Shared with the bus wrappers. */
export function messageAttributes(message: Message): Attributes {
  return {
    "kronos.message.name": qualifiedNameToString(message.name),
    "kronos.message.id": message.identifier,
    "kronos.message.kind": message.kind,
  }
}

/**
 * How a message names the span that handles it when the wrap site does not say
 * otherwise — its own qualified name, matching `dispatch(...)`/`query(...)` on
 * the producer side.
 */
export function messageName(message: Message): string {
  return qualifiedNameToString(message.name)
}

function handlerSpanKind(kind: MessageKind): SpanKindValue {
  // A query is answered synchronously to a waiting caller — SERVER, opposite
  // the CLIENT span `otlpQueryBus` opens. Commands and events are consumed off
  // a bus — CONSUMER, opposite PRODUCER.
  return kind === "query" ? SpanKind.SERVER : SpanKind.CONSUMER
}

type OtlpDescription<H> = {
  readonly name: "otlpHandler"
  readonly supplies: readonly ["trace", "metrics"]
  readonly stamps: readonly ["message.metadata"]
  readonly next: H
}

/**
 * Wrap a handler function so each invocation is a span, joined to the trace the
 * handled message arrived carrying, stamped onto the message and supplied as
 * `ctx.trace`.
 * Existing `ctx.load`, `ctx.source`, `ctx.send`, and `ctx.query` calls also
 * get child spans. Sends and queries carry their operation's traceparent to
 * the receiving handler; arguments and results are never recorded.
 *
 * How it joins differs by leg — read off `message.kind`, because the message
 * knows what it is:
 *
 * - COMMAND and QUERY messages PARENT onto the remote context. The dispatcher
 *   is still on the stack waiting for the result, so nesting is honest: the
 *   handler's duration really is part of the caller's.
 *
 * - EVENT messages LINK to it instead, and run as the root of their OWN trace.
 *   An event processor may handle an event long after the producing trace
 *   finished — a projection catching up over a batch of month-old events would
 *   otherwise be swallowed into whatever produced them, reporting a month-long
 *   span. The link keeps the correlation without the lie.
 *
 * `label` ABSENT names the span after the message's qualified name. Pass one to
 * name it otherwise; it is a function OF THE MESSAGE, never a per-handler
 * string closed over at wiring time.
 *
 * ORDER: this wrapper goes OUTSIDE `correlatingHandler` and `loggingHandler`,
 * which read the message it stamps. Inside them, the compiler refuses with a
 * sentence and `kronos()` refuses at boot.
 *
 * ```ts
 * const wrap = (h) => otlpHandler(loggingHandler(correlatingHandler(drizzleHandler(h, db)), log), exporter)
 * ```
 */
export function otlpHandler<M extends Message, C, R>(
  next: (message: M, context: C) => R,
  exporter: OtlpExporter,
  label?: (message: Message) => string,
): ((message: M, context: Omit<C, "trace" | "metrics">) => Promise<Awaited<R>>) &
  Described<OtlpDescription<(message: M, context: C) => R>> {
  const wrapped = async (message: M, context: Omit<C, "trace" | "metrics">): Promise<Awaited<R>> => {
    const name = label ? label(message) : messageName(message)
    const remote = traceparentOf(message.metadata)
    const linked = message.kind === "event"
    const span = exporter.startSpan({
      name,
      kind: handlerSpanKind(message.kind),
      parent: linked ? undefined : remote,
      links: linked && remote ? [remote] : undefined,
      attributes: messageAttributes(message),
    })

    // The two attributes every series of this handling carries — the standard
    // three and anything the handler records through `ctx.metrics`.
    const series: Attributes = { message_type: message.kind, message_name: name }
    const stamped = { ...message, metadata: withTraceparent(message.metadata, span) } as M
    const scope = trace(exporter, span)
    const supplied = {
      ...instrumented(context, scope, exporter),
      trace: scope,
      metrics: metrics(exporter, series),
    } as unknown as C

    const started = performance.now()
    try {
      const result = await next(stamped, supplied)
      span.end()
      timeToDurability(context, span, message)
      return result
    } catch (error) {
      span.fail(error)
      // `failed` is counted IN ADDITION to `handled`, so an error rate is one
      // division, and it carries the error's type so a typed domain rejection
      // and a bug are different series.
      exporter.addCount({
        name: FAILED,
        value: 1,
        unit: "1",
        description: "Count of message handler invocations that threw",
        attributes: { ...series, error_type: errorType(error) },
      })
      throw error
    } finally {
      exporter.addCount({
        name: HANDLED,
        value: 1,
        unit: "1",
        description: "Count of message handler invocations",
        attributes: series,
      })
      exporter.recordHistogram({
        name: DURATION,
        value: performance.now() - started,
        unit: "ms",
        description: "Handler invocation duration",
        attributes: series,
      })
    }
  }
  return describe(wrapped, {
    name: "otlpHandler",
    supplies: ["trace", "metrics"],
    stamps: ["message.metadata"],
    next,
  } as const)

  /** The `commit` span: from this handler's end until its task completes or fails. */
  function timeToDurability(context: unknown, parent: { traceId: string; spanId: string }, message: Message): void {
    const uow = (context as { readonly unitOfWork?: UnitOfWork } | undefined)?.unitOfWork
    // A real task, still open. (A test's stand-in context may carry anything here.)
    if (uow === undefined || typeof uow.whenComplete !== "function" || uow.closed) return
    const commit = exporter.startSpan({
      name: "commit",
      kind: SpanKind.INTERNAL,
      parent,
      attributes: messageAttributes(message),
    })
    uow.whenComplete(() => commit.end())
    uow.onError((error) => commit.fail(error))
  }
}

/**
 * The context with its Kronos operations as child spans of `scope`. Which
 * operations exist is asked of the context, not assumed — a query context has
 * no `send` — and no argument or result is ever recorded.
 */
function instrumented<C>(context: C, scope: Trace, exporter: OtlpExporter): C {
  const source = context as Record<string, unknown> | undefined
  const wrapped: Record<string, unknown> = { ...source }

  for (const name of ["load", "source"] as const) {
    const operation = source?.[name]
    if (typeof operation !== "function") continue
    wrapped[name] = scope.span((...args: unknown[]) => operation.apply(context, args), {
      name: `ctx.${name}`,
    })
  }

  for (const name of ["send", "query"] as const) {
    const operation = source?.[name]
    if (typeof operation !== "function") continue
    wrapped[name] = (descriptor: unknown, payload: unknown, metadata: Metadata = {}) => {
      // Correlation may already have carried the handler's traceparent here.
      // An explicit parent (for example from trace.run) wins, just as it does
      // at the bus boundary. The outgoing message then carries THIS operation.
      const parent = traceparentOf(metadata) ?? scope.context
      return trace(exporter, parent).run(`ctx.${name}`, (child) =>
        operation.call(context, descriptor, payload, withTraceparent(metadata, child.context!)),
      )
    }
  }

  return wrapped as C
}
