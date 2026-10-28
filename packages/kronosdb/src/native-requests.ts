// ---------------------------------------------------------------------------
// REQUESTS ON A CONTEXT — the three steps every event store call repeats.
//
// Get the live transport (connecting if there is none), get the wire id of the
// context on it (binding it the first time), send. The first two are
// synchronous when everything is already set up, which is every call after the
// first, so the hot path pays no await for them.
//
// A transport can die between the steps. If it did before anything was sent
// the request simply goes to its replacement; once the frame is written a
// failure is the caller's to see, because an append that was written may have
// been applied.
// ---------------------------------------------------------------------------

import { KronosDbError, KronosDbErrorCode } from "./errors.js"
import type { NativeLink } from "./native-link.js"
import type { FrameSink, NativeStream, NativeTransport, ReplyParser } from "./native-transport.js"
import { type Writer } from "./protocol/kronosdb-protocol.js"

/** A live transport, and the id of the context or bus called `name` on it. */
async function bound(link: NativeLink, plane: "context" | "bus", name: string): Promise<{ transport: NativeTransport; id: number }> {
  for (let attempt = 0; ; attempt++) {
    const transport = link.current() ?? (await link.ready())
    const pending = plane === "context" ? transport.contextId(name) : transport.busId(name)
    const id = typeof pending === "number" ? pending : await pending
    if (!transport.closed || attempt >= 2) return { transport, id }
  }
}

/** Send a unary request addressed to `context`; `body` is handed the context's wire id. */
export async function contextRequest<T>(
  link: NativeLink,
  context: string,
  kind: number,
  body: (w: Writer, context: number) => void,
  parse: ReplyParser<T>,
): Promise<T> {
  const transport = link.current()
  if (transport !== undefined) {
    const pending = transport.contextId(context)
    if (typeof pending === "number") return transport.request(kind, (w) => body(w, pending), parse)
  }
  const { transport: live, id } = await bound(link, "context", context)
  return live.request(kind, (w) => body(w, id), parse)
}

/**
 * Open a stream addressed to `context`. Resolves once the opening frame is
 * written; what the server sends afterwards reaches `sink`, and a failure
 * (including the connection dying) reaches `fail`.
 */
export async function contextStream(
  link: NativeLink,
  context: string,
  kind: number,
  body: (w: Writer, context: number) => void,
  end: boolean,
  sink: FrameSink,
  fail: (error: Error) => void,
): Promise<NativeStream> {
  const { transport, id } = await bound(link, "context", context)
  return transport.open(kind, (w) => body(w, id), end, sink, fail)
}

/** The reply was not the frame this request is answered with. */
export function unexpected(kind: number, wanted: string): KronosDbError {
  return new KronosDbError(`Expected ${wanted} from KronosDB, got frame ${kind.toString(16)}`, KronosDbErrorCode.OTHER, false)
}

/**
 * Open a stream addressed to the messaging bus `bus`. The common case, a live
 * transport with the bus already bound, returns the stream itself and costs no
 * await; otherwise a promise of it. Either way the opening frame is written by
 * the time the stream exists, what the server sends afterwards reaches `sink`,
 * and a failure (including the connection dying) reaches `fail`.
 */
export function busStream(
  link: NativeLink,
  bus: string,
  kind: number,
  body: (w: Writer, bus: number) => void,
  end: boolean,
  sink: FrameSink,
  fail: (error: Error) => void,
): NativeStream | Promise<NativeStream> {
  const transport = link.current()
  if (transport !== undefined) {
    const pending = transport.busId(bus)
    if (typeof pending === "number") return transport.open(kind, (w) => body(w, pending), end, sink, fail)
  }
  return bound(link, "bus", bus).then(({ transport: live, id }) => live.open(kind, (w) => body(w, id), end, sink, fail))
}
