import { qualifiedNameToString, type Message } from "../messaging/messages.js"
import { describe, type DeclaresInside, type Described } from "../composition/describe.js"
import type { Logger, LogFields } from "./logger.js"

/**
 * The order rule this wrapper owns: `ctx.log` is bound to the handled
 * message's trace ids, so a wrapper that STAMPS the message (a tracing wrapper
 * writing its span) must sit outside it, or every log line misses its trace.
 */
type StampedInside<H> = DeclaresInside<H, "stamps", "message.metadata"> extends true
  ? "a wrapper that stamps message.metadata is inside loggingHandler: move it outside, so log lines carry the stamp"
  : unknown

/** The handler must DEMAND `ctx.log`; wrapping one that does not (or wrapping twice) is refused with a sentence. */
type DemandsLog<H> = H extends (message: any, context: infer C) => any
  ? C extends LogCapability
    ? unknown
    : "loggingHandler supplies ctx.log, but the handler it wraps does not demand it — wrap the handler that names LogCapability, and only once"
  : never

type LoggingDescription<H> = {
  readonly name: "loggingHandler"
  readonly supplies: readonly ["log"]
  readonly reads: readonly ["message.metadata"]
  readonly next: H
}

/** The `log` capability this wrapper adds to a handler context. */
export type LogCapability = {
  readonly log: Logger
}

const TRACEPARENT_RE = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/i

/**
 * The fields every logged record from one handling should carry — the
 * message's identity, plus its correlation pair and trace context when the
 * message actually has them. `undefined` never appears in the result: a
 * missing field is left OFF rather than written as `null` or `undefined`,
 * which is what keeps a JSON line free of noise for the common uncorrelated
 * message.
 */
export function messageFields(message: Message): LogFields {
  const { metadata } = message
  const fields: Record<string, string> = {
    "message.name": qualifiedNameToString(message.name),
    "message.id": message.identifier,
    "message.kind": message.kind,
  }

  if (typeof metadata.correlationId === "string") fields.correlationId = metadata.correlationId
  if (typeof metadata.causationId === "string") fields.causationId = metadata.causationId

  const traceparent = metadata.traceparent
  const parsed = typeof traceparent === "string" ? TRACEPARENT_RE.exec(traceparent) : null
  if (parsed) {
    fields.trace_id = parsed[1]!
    fields.span_id = parsed[2]!
  }

  return fields
}

/**
 * Wrap a HANDLER FUNCTION so its context gains `log`, a {@link Logger}
 * already carrying this invocation's message identity — its qualified name,
 * id, kind, correlation pair and (when present) trace ids parsed off a W3C
 * `traceparent`.
 *
 * Same shape as `postgresHandler`: a plain function over a plain function,
 * supplying the one field it adds and erasing it from what `next` had to ask
 * for, so the host spreads the entry and nothing about `log` leaks into it:
 *
 * ```ts
 * const editWidget = commandHandler(EditWidget, async (message, ctx: CommandHandlerContext & LogCapability) => {
 *   ctx.log.info("editing widget", { widgetId: message.payload.widgetId })
 * })
 *
 * kronos({
 *   commandHandlers: [editWidget]
 *     .map((h) => ({ ...h, handler: loggingHandler(h.handler, consoleLogger()) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * Not async, and it changes neither `next`'s return value nor whether it is
 * one: `next` decides sync or async, this wrapper only hands it a context.
 */
export function loggingHandler<H extends (message: any, context: any) => any>(
  next: H & DemandsLog<H> & StampedInside<H>,
  logger: Logger,
): ((message: Parameters<H>[0], context: Omit<Parameters<H>[1], "log">) => ReturnType<H>) &
  Described<LoggingDescription<H>> {
  const wrapped = (message: Message, context: Omit<Parameters<H>[1], "log">): ReturnType<H> =>
    next(message, { ...context, log: logger.with(messageFields(message)) } as Parameters<H>[1])
  return describe(wrapped, { name: "loggingHandler", supplies: ["log"], reads: ["message.metadata"], next } as const)
}
