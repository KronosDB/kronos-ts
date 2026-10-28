// ---------------------------------------------------------------------------
// A HANDLER STREAM — the long-lived stream a command or query bus serves on.
//
// The client opens it with OpenCommandHandlers / OpenQueryHandlers, subscribes
// to names on it, grants the server credits, and the server sends one command
// or query per credit. It lives as long as the handler does, so it is the one
// request in the protocol that has to come back by itself: this file owns
// that, and nothing about what the frames MEAN.
//
//   WHEN IT IS OPEN. Lazily, at the first subscription. A process that only
//   dispatches never opens one.
//
//   WHAT COMES BACK. Every incarnation announces itself from scratch —
//   `announce` re-sends the names and the credits — because the server forgot
//   the old stream's registrations the moment it ended. Each incarnation is its
//   own object, so a frame, a failure or a response that belongs to an old one
//   is recognised and dropped instead of being taken for the new one's.
//
//   WHO ENDS IT. The server does, when the client's platform stream ends, when
//   a newer handler stream of the same client replaces it, or when the
//   connection goes. All three reach `fail` and all three are retried with
//   backoff; only closing (or the owner's shutdown) stops that.
//
//   WHEN IT IS READY. Subscriptions are acknowledged one by one. `acknowledged`
//   is the barrier `kdb.start()` waits on: the stream is open and nothing sent
//   on it is still unanswered.
// ---------------------------------------------------------------------------

import { KronosDbError, KronosDbErrorCode } from "./errors.js"
import type { NativeLink } from "./native-link.js"
import { busStream } from "./native-requests.js"
import type { NativeStream } from "./native-transport.js"
import { FrameKind, decodeServerAck, reader, type Writer } from "./protocol/kronosdb-protocol.js"
import type { ResilienceConfig } from "./resilience.js"
import { streamRecovery } from "./stream-recovery.js"

const identities = new WeakMap<object, Map<string, number>>()

/**
 * The client id a handler stream is registered under.
 *
 * It is the connection's own: the server ties a client's handler streams to its
 * platform stream, and a newer stream of the same client on the same bus
 * REPLACES an older one — which is how a restarted handler takes over from its
 * stale stream. Two buses built for the same bus name on one connection are not
 * a restart, though, and would replace each other for good, each reopening to
 * take the name back. So the first bus of a kind and name has the connection's
 * id and any further one a suffixed id of its own.
 */
export function handlerClientId(connection: { readonly config: { readonly clientId: string } } & object, kind: "command" | "query", bus: string): string {
  let counts = identities.get(connection)
  if (counts === undefined) identities.set(connection, (counts = new Map()))
  const key = `${kind}:${bus}`
  const n = counts.get(key) ?? 0
  counts.set(key, n + 1)
  return n === 0 ? connection.config.clientId : `${connection.config.clientId}#${n + 1}`
}

/** What the bus-specific code is given to work with while a stream is open. */
export type HandlerSession = {
  /** Write a frame on this incarnation. Dropped silently once it has ended. */
  send(kind: number, body: (w: Writer) => void): void
  /**
   * Write a frame the server answers with ServerAck. The barrier waits for the
   * answer; `acknowledged` rejects if the server refuses it.
   */
  instruct(kind: number, body: (w: Writer, instructionId: string) => void): void
}

export type HandlerStreamOptions = {
  readonly link: NativeLink
  readonly bus: string
  /** OpenCommandHandlers or OpenQueryHandlers. */
  readonly openKind: number
  readonly writeOpen: (w: Writer, bus: number) => void
  /** Whether the owner may retry now: not shutting down, connection not closed or mid-reconnect. */
  readonly canReconnect: () => boolean
  readonly resilience?: Partial<ResilienceConfig>
  /** A stream is open: subscribe to every name and grant credits. */
  readonly announce: (session: HandlerSession) => void
  /**
   * A frame from the server (never ServerAck). `view` is the received chunk;
   * keep nothing but copies. `session` is the incarnation it arrived on, the
   * one to answer on.
   */
  readonly onFrame: (session: HandlerSession, kind: number, buf: Uint8Array, start: number, end: number) => void
  /** The incarnation ended: forget what belonged to it. */
  readonly onEnded: () => void
}

export type HandlerStream = {
  /** Open the stream if it is not open or opening. Idempotent. */
  start(): void
  readonly started: boolean
  readonly open: boolean
  /** The open incarnation, to write on outside a callback. */
  readonly session: HandlerSession | undefined
  /** Resolves when the stream is open and no instruction on it is unanswered. */
  acknowledged(): Promise<void>
  /** The connection was reset: end the stream now (recovery is `restart`). */
  abandon(): void
  /** Reopen at once, instead of waiting out a backoff. */
  restart(): void
  /**
   * Stop retrying, and leave an open stream alone: a draining bus still has
   * commands to answer on it.
   */
  retire(): void
  /** Stop for good. */
  close(): void
}

type Incarnation = {
  stream: NativeStream | undefined
  session: HandlerSession | undefined
  readonly pending: Set<string>
  next: number
}

export function handlerStream(options: HandlerStreamOptions): HandlerStream {
  let live: Incarnation | undefined
  let started = false
  let closed = false
  const waiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = []

  const recovery = streamRecovery(open, () => !closed && options.canReconnect(), options.resilience)

  function settle(): void {
    if (live?.stream === undefined || live.pending.size > 0) return
    for (const waiter of waiters.splice(0)) waiter.resolve()
  }

  function refuse(error: Error): void {
    for (const waiter of waiters.splice(0)) waiter.reject(error)
  }

  function end(mine: Incarnation): boolean {
    if (live !== mine) return false
    live = undefined
    options.onEnded()
    return true
  }

  function open(): void {
    if (closed) return
    started = true
    const previous = live
    const mine: Incarnation = { stream: undefined, session: undefined, pending: new Set(), next: 0 }
    live = mine
    if (previous !== undefined) previous.stream?.cancel()

    const session: HandlerSession = {
      send(kind, body) {
        if (live === mine) mine.stream?.send(kind, body)
      },
      instruct(kind, body) {
        if (live !== mine || mine.stream === undefined) return
        const id = `i${++mine.next}`
        mine.pending.add(id)
        mine.stream.send(kind, (w) => body(w, id))
      },
    }

    mine.session = session

    const sink = (kind: number, _flags: number, buf: Uint8Array, start: number, stop: number): void => {
      if (live !== mine) return
      recovery.received()
      if (kind === FrameKind.ServerAck) {
        const { ack } = decodeServerAck(reader(buf, start, stop))
        if (!mine.pending.delete(ack.instructionId)) return
        if (!ack.success) {
          refuse(new KronosDbError(ack.error?.message ?? "KronosDB refused a handler registration", KronosDbErrorCode.OTHER, false))
          return
        }
        settle()
        return
      }
      options.onFrame(session, kind, buf, start, stop)
    }

    const fail = (error: Error): void => {
      if (!end(mine)) return
      // A stream that ends cleanly is as lost as one that fails: the handler is gone.
      recovery.failed(error)
    }

    const attach = (stream: NativeStream): void => {
      if (live !== mine) {
        stream.cancel()
        return
      }
      mine.stream = stream
      try {
        options.announce(session)
      } catch (error) {
        fail(error instanceof Error ? error : new Error(String(error)))
        return
      }
      settle()
    }

    try {
      const opened = busStream(options.link, options.bus, options.openKind, options.writeOpen, false, sink, fail)
      if (opened instanceof Promise) opened.then(attach, fail)
      else attach(opened)
    } catch (error) {
      fail(error instanceof Error ? error : new Error(String(error)))
    }
  }

  return {
    start() {
      if (!started) open()
    },
    get started() {
      return started
    },
    get open() {
      return live?.stream !== undefined
    },
    get session() {
      return live?.stream !== undefined ? live.session : undefined
    },
    acknowledged() {
      if (!started) return Promise.resolve()
      if (live?.stream !== undefined && live.pending.size === 0) return Promise.resolve()
      return new Promise<void>((resolve, reject) => waiters.push({ resolve, reject }))
    },
    abandon() {
      const mine = live
      if (mine === undefined) return
      end(mine)
      mine.stream?.cancel()
    },
    restart() {
      if (closed || !started) return
      recovery.restart()
    },
    retire() {
      recovery.stop()
    },
    close() {
      closed = true
      recovery.stop()
      const mine = live
      if (mine !== undefined) {
        end(mine)
        mine.stream?.cancel()
      }
      refuse(new KronosDbError("The handler stream was closed", KronosDbErrorCode.CONNECTION_FAILED, true))
    },
  }
}
