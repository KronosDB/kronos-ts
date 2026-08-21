import type { Message, Metadata, MessageDescriptor } from "../messaging/messages.js"
import { validate, validatedNow } from "./validate.js"

/**
 * VALIDATION IS THE GATE MECHANISM: nothing crosses a handling boundary
 * unparsed — not the message coming IN, and not a single message the handling
 * gives birth to on the way out.
 *
 * The descriptor is the second argument because the wrapper composes exactly
 * where a descriptor and a handler already sit together — the ENTRY:
 *
 * ```ts
 * kronos({
 *   commandHandlers: handlers
 *     .map((h) => ({ ...h, handler: validatingHandler(h.handler, h.descriptor) }))
 *     .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
 * })
 * ```
 *
 * Two things happen per invocation, and they are two different questions:
 *
 * 1. INBOUND — the message's payload is validated against the ENTRY's
 *    descriptor, the one this handler was declared for. A message off a wire is
 *    a claim, not a fact; a transport that decoded JSON knows the shape parsed,
 *    not that it is a command this handler can act on.
 *
 * 2. OUTBOUND — every birth verb the context has is wrapped, and each validates
 *    its payload against the descriptor IT was called with. `ctx.append` is the
 *    load-bearing one: THE LOG NEVER ACCEPTS A LIE. A buggy handler's garbage
 *    fails at the moment of the lie, before the store ever sees it, rather than
 *    years later in a replay that did nothing wrong.
 *
 * IN BOTH DIRECTIONS THE PARSED VALUE REPLACES THE INPUT. Standard validation
 * is a parse — coercions, defaults and transforms are part of what a schema
 * says — so `next` is handed a message whose payload is what the schema
 * produced, and each wrapped verb gives birth to the produced value, never the
 * one the caller happened to pass.
 *
 * Which verbs exist is asked of the context, not assumed: a query context has
 * neither `send` nor `append`, an event context has no `append`, and one wrapper
 * serves all three kinds because it wraps only what it finds. The
 * SYNC/ASYNC SPLIT falls out of the verbs themselves — `ctx.send` and
 * `ctx.query` already answer promises, so an async schema is simply awaited;
 * `ctx.append` returns `void` and `ctx.schedule` builds its message in the
 * caller's turn, so an async schema there throws, naming the message type and
 * the verb.
 *
 * The wrapper is ASYNC by return type (`Promise<Awaited<R>>`, the shape
 * `otlpHandler` uses) because inbound validation may have to be awaited. It
 * composes with the other function-level wrappers in any order:
 *
 * ```ts
 * validatingHandler(correlatingHandler(otlpHandler(h.handler, exporter), correlationFrom), h.descriptor)
 * ```
 *
 * Nothing here demands a capability of `C` — validation asks the context for no
 * type it did not already have, which is why `C` is unconstrained and a wrapped
 * handler wires against exactly the buses the unwrapped one did.
 */
export function validatingHandler<M extends Message, C, R>(
  next: (message: M, context: C) => R,
  descriptor: MessageDescriptor,
): (message: M, context: C) => Promise<Awaited<R>> {
  return async (message, context): Promise<Awaited<R>> => {
    const payload = await validate(descriptor, message.payload)
    return await next({ ...message, payload } as M, overlaid(context))
  }
}

/**
 * The context with its birth verbs overlaid. A fresh record per invocation —
 * the contexts are plain per-invocation literals, so spreading one is honest
 * here in the way `correlatingHandler` relies on too.
 *
 * Every verb takes its descriptor FIRST, so each wrapper validates against the
 * descriptor of the message being born rather than against the entry's — a
 * command handler appending three different events checks three different
 * schemas, and none of them is the command's.
 */
function overlaid<C>(context: C): C {
  const source = context as unknown as Record<string, unknown>
  const wrapped: Record<string, unknown> = { ...source }

  const { send, query, append, schedule, scheduleAfter } = source

  if (typeof send === "function") {
    // Async: `ctx.send` answers a promise already, so an async schema costs the
    // caller nothing it was not paying.
    wrapped.send = async (descriptor: MessageDescriptor, payload: unknown, metadata?: Metadata) =>
      send(descriptor, await validate(descriptor, payload), metadata)
  }

  if (typeof query === "function") {
    wrapped.query = async (descriptor: MessageDescriptor, payload: unknown, metadata?: Metadata) =>
      query(descriptor, await validate(descriptor, payload), metadata)
  }

  if (typeof append === "function") {
    // `append` has a batch form, and each tuple in it names its OWN descriptor —
    // so the validation goes onto each entry rather than onto the call.
    wrapped.append = (descriptorOrList: unknown, payload?: unknown, metadata?: Metadata): unknown =>
      Array.isArray(descriptorOrList)
        ? append(
            descriptorOrList.map(
              ([descriptor, entryPayload, entryMetadata]: [MessageDescriptor, unknown, Metadata?]) =>
                [descriptor, validatedNow(descriptor, entryPayload, "append"), entryMetadata] as const,
            ),
          )
        : append(
            descriptorOrList,
            validatedNow(descriptorOrList as MessageDescriptor, payload, "append"),
            metadata,
          )
  }

  if (typeof schedule === "function") {
    wrapped.schedule = (descriptor: unknown, payload: unknown, at: unknown, metadata?: Metadata) =>
      schedule(
        descriptor,
        validatedNow(descriptor as MessageDescriptor, payload, "schedule"),
        at,
        metadata,
      )
  }

  if (typeof scheduleAfter === "function") {
    wrapped.scheduleAfter = (
      descriptor: unknown,
      payload: unknown,
      delayMs: unknown,
      metadata?: Metadata,
    ) =>
      scheduleAfter(
        descriptor,
        validatedNow(descriptor as MessageDescriptor, payload, "scheduleAfter"),
        delayMs,
        metadata,
      )
  }

  return wrapped as unknown as C
}
