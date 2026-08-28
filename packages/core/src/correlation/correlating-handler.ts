import { emptyMetadata, mergeMetadata, type Metadata, type Message } from "../messaging/messages.js"
import type { CorrelatingUnitOfWork } from "./correlating.js"

/**
 * The one thing a context must offer for this wrapper to work: a unit of work
 * that carries a correlation map. Everything else it touches — `send`,
 * `query`, `append`, `schedule`, `scheduleAfter` — is wrapped only if the
 * context actually has it, which is what lets ONE wrapper serve all three
 * handler kinds (a query context has neither `send` nor `append`).
 */
type CorrelatingCapable = {
  readonly unitOfWork: CorrelatingUnitOfWork
}

/**
 * CORRELATION IS THE CARRYING MECHANISM: metadata jumping from the message a
 * handler is handling onto every message that handling gives birth to, and from
 * there onto everything THOSE births cause, all the way down the chain.
 *
 * `from` is the CARGO — what jumps. It is a plain function of the handled
 * message, and it is REQUIRED: the mechanism has no opinion about what is worth
 * carrying, and conjuring a default here would decide that for every host in
 * the world. The pair everybody starts from is two lines the host writes —
 * the chain is inherited or seeded, the cause is the parent, unconditionally:
 *
 * ```ts
 * const correlationFrom = (parent: Message): Metadata => ({
 *   correlationId: String(parent.metadata.correlationId ?? parent.identifier),
 *   causationId: String(parent.identifier),
 * })
 *
 * correlatingHandler(h.handler, correlationFrom)
 *
 * // the pair plus the host's own per-request facts
 * correlatingHandler(h.handler, (m) => ({
 *   ...correlationFrom(m),
 *   actor: String(m.metadata.actor ?? ""),
 * }))
 * ```
 *
 * Two things happen per invocation:
 *
 * 1. `from(message)` is ATTACHED to the unit of work's correlation map.
 * 2. The context's birth verbs are wrapped to OVERLAY that map through their
 *    trailing `metadata` parameter, so everything the handler gives birth to
 *    carries it — and, being on the message, carries it across any transport.
 *
 * The overlay is read PER CALL, not captured at wrap time: a handler that
 * attaches more mid-handling — `ctx.unitOfWork.attachCorrelationData({
 * traceparent })` — has it on the next verb, and a later attach wins over an
 * earlier one. Metadata the CALLER passes to a verb wins over the overlay,
 * because a caller naming a key means it.
 *
 * The demand is the point, and it is made ON THE WRAPPER'S OUTPUT, never on the
 * handler it wraps. `next` asks for whatever context it likes — usually an
 * unannotated one that knows nothing about tasks — and what comes back asks
 * for `C & { unitOfWork: CorrelatingUnitOfWork }`. So a handler wrapped here
 * does not typecheck against a bus or a processor built from a bare
 * `() => unitOfWork()` factory: wrap your handlers, and the compiler makes you
 * wrap your unit of work — and the handler never had to say so, because
 * carrying is something done TO a handling, not something a handling does.
 * A handler that reaches for the map itself (`ctx.unitOfWork.attachCorrelationData`)
 * is the one exception, and it annotates `ctx` the way any other demand does.
 * This is a wrapper you OPT IN to —
 * nothing in core demands it, which is why the previous attempt (a correlation
 * capability hardcoded into `ctx` and the bus signatures) had to be reverted.
 * An unconditional demand propagates contravariantly through every transport; a
 * conditional one propagates exactly as far as somebody asked for it.
 */
export function correlatingHandler<M extends Message, C, R>(
  next: (message: M, context: C) => R,
  from: (message: Message) => Metadata,
): (message: M, context: C & CorrelatingCapable) => R {
  return (message, context) => {
    const uow = context.unitOfWork
    uow.attachCorrelationData(stringly(from(message)))
    return next(message, overlaid(context, uow) as unknown as C)
  }
}

/**
 * The map is `Record<string, string>` because that is what survives a wire:
 * every transport's metadata encoding is string-keyed and string-valued, and a
 * cargo function that returned a nested object would carry differently
 * in-process than it does across a broker.
 */
function stringly(metadata: Metadata): Record<string, string> {
  const carried: Record<string, string> = {}
  for (const [key, value] of Object.entries(metadata)) carried[key] = String(value)
  return carried
}

/**
 * The context with its birth verbs overlaid. A fresh record per invocation —
 * the contexts are plain per-invocation literals, so spreading one is honest
 * here in a way that spreading a unit of work (whose `phase`/`closed` are
 * getters) never is.
 */
function overlaid(context: CorrelatingCapable, uow: CorrelatingUnitOfWork): CorrelatingCapable {
  const overlay = (provided?: Metadata): Metadata =>
    mergeMetadata(uow.correlationData(), provided ?? emptyMetadata())

  const source = context as CorrelatingCapable & Record<string, unknown>
  const wrapped: Record<string, unknown> = { ...source }

  const { send, query, append, schedule, scheduleAfter } = source

  if (typeof send === "function") {
    wrapped.send = (descriptor: unknown, payload: unknown, metadata?: Metadata) =>
      send(descriptor, payload, overlay(metadata))
  }

  if (typeof query === "function") {
    wrapped.query = (descriptor: unknown, payload: unknown, metadata?: Metadata) =>
      query(descriptor, payload, overlay(metadata))
  }

  if (typeof append === "function") {
    // `append` has a batch form, and the batch form carries its metadata per
    // entry — so the overlay goes onto each tuple rather than onto the call.
    wrapped.append = (descriptorOrList: unknown, payload?: unknown, metadata?: Metadata): unknown =>
      Array.isArray(descriptorOrList)
        ? append(
            descriptorOrList.map(
              ([descriptor, entryPayload, entryMetadata]: [unknown, unknown, Metadata?]) =>
                [descriptor, entryPayload, overlay(entryMetadata)] as const,
            ),
          )
        : append(descriptorOrList, payload, overlay(metadata))
  }

  if (typeof schedule === "function") {
    wrapped.schedule = (descriptor: unknown, payload: unknown, at: unknown, metadata?: Metadata) =>
      schedule(descriptor, payload, at, overlay(metadata))
  }

  if (typeof scheduleAfter === "function") {
    wrapped.scheduleAfter = (
      descriptor: unknown,
      payload: unknown,
      delayMs: unknown,
      metadata?: Metadata,
    ) => scheduleAfter(descriptor, payload, delayMs, overlay(metadata))
  }

  return wrapped as unknown as CorrelatingCapable
}
