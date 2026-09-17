import { emptyMetadata, mergeMetadata, type Metadata, type Message } from "../messaging/messages.js"
import { describe, type DeclaresInside, type Described } from "../composition/describe.js"
import { messageOrigin } from "./message-origin.js"

/** Any handler function. The wrapper is generic in the WHOLE function so its type survives intact. */
type AnyHandler = (message: any, context: any) => any

/**
 * The one order rule this wrapper owns: it READS the handled message's
 * metadata, so a wrapper that STAMPS the message (a tracing wrapper writing
 * its span) must sit outside it, or the cargo is computed before the stamp.
 * Refused in the type with a sentence, and at boot by `kronos()`.
 */
type StampedInside<H> = DeclaresInside<H, "stamps", "message.metadata"> extends true
  ? "a wrapper that stamps message.metadata is inside correlatingHandler: move it outside, so the cargo sees the stamp"
  : unknown

/** What this wrapper says about itself; see `describe`. */
type CorrelatingDescription<H> = {
  readonly name: "correlatingHandler"
  readonly reads: readonly ["message.metadata"]
  readonly next: H
}

/**
 * CORRELATION IS THE CARRYING MECHANISM: metadata jumping from the message a
 * handler is handling onto every message that handling gives birth to, and from
 * there onto everything THOSE births cause, all the way down the chain.
 *
 * `from` is the CARGO — what jumps. It is a plain function of the handled
 * message, and it defaults to {@link messageOrigin}: the chain is inherited or
 * seeded, the cause is the parent, and a trace context rides along when the
 * message carries one. A host that carries more spreads the default:
 *
 * ```ts
 * correlatingHandler(h.handler)
 *
 * // the standard cargo plus the host's own per-request facts
 * correlatingHandler(h.handler, (m) => ({
 *   ...messageOrigin(m),
 *   actor: String(m.metadata.actor ?? ""),
 * }))
 * ```
 *
 * Per invocation, `from(message)` is computed ONCE and held in this
 * invocation's closure; the context's birth verbs are wrapped to OVERLAY it
 * through their trailing `metadata` parameter, so everything the handler gives
 * birth to carries it — and, being on the message, carries it across any
 * transport. Metadata the CALLER passes to a verb wins over the overlay,
 * because a caller naming a key means it.
 *
 * THE CARGO BELONGS TO THE INVOCATION, NOT TO THE TASK. It used to be written
 * into a map on the unit of work and read back per verb call — which meant a
 * nested handling on the same task (a processor batch delivering several
 * events) overwrote its caller's cargo, so a message sent after the nested
 * handling claimed the wrong cause. Held in the closure, two invocations on
 * one task cannot see each other, and nothing is stored on the unit of work.
 * (Axon 5 does the same: its correlation interceptor puts the cargo on a
 * branched processing context, never on the unit of work.)
 *
 * Nothing here demands a capability of `C` — `C` in, `C` out, exactly as
 * `validatingHandler` — so a wrapped handler wires against exactly the buses
 * the unwrapped one did. This is a wrapper you OPT IN to: nothing in core
 * carries anything unless a host composes this.
 */
export function correlatingHandler<H extends AnyHandler>(
  next: H & StampedInside<H>,
  from: (message: Message) => Metadata = messageOrigin,
): ((message: Parameters<H>[0], context: Parameters<H>[1]) => ReturnType<H>) & Described<CorrelatingDescription<H>> {
  // A FRESH function type on the way out — never `H` itself intersected with
  // this description, which would pile this description onto whatever `H`
  // carried and leave the next rule reading a merged, meaningless one.
  const wrapped = (message: Parameters<H>[0], context: Parameters<H>[1]): ReturnType<H> =>
    next(message, overlaid(context, stringly(from(message))))
  return describe(wrapped, { name: "correlatingHandler", reads: ["message.metadata"], next } as const)
}

/**
 * The cargo is `Record<string, string>` because that is what survives a wire:
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
 * the contexts are plain per-invocation literals, so spreading one is honest.
 * Which verbs exist is asked of the context, not assumed: a query context has
 * neither `send` nor `append`, an event context has no `append`, and one
 * wrapper serves all three kinds because it wraps only what it finds.
 */
function overlaid<C>(context: C, cargo: Record<string, string>): C {
  const overlay = (provided?: Metadata): Metadata => mergeMetadata(cargo, provided ?? emptyMetadata())

  const source = context as unknown as Record<string, unknown>
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

  return wrapped as unknown as C
}
