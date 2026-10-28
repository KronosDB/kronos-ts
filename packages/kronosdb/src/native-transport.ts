// ---------------------------------------------------------------------------
// THE NATIVE TRANSPORT — one socket, many requests.
//
// A KronosDB native connection is one TCP (or TLS) socket carrying multiplexed
// streams of length-prefixed frames. This file is the whole client half of
// that: the preface and Hello/Welcome handshake, stream ids, request and stream
// slots, write coalescing, heartbeat answers, and a close that fails whatever
// is in flight. It knows frames and streams and nothing about events — what a
// request MEANS lives in the stores that send them.
//
// IT IS BUILT AROUND TWO COSTS, because they are what the grpc-js client it
// replaces could not avoid.
//
//   WRITES ARE COALESCED. Every frame is encoded straight into one shared
//   buffer, and the buffer is handed to the socket once per tick — in a
//   microtask that runs after every continuation already queued. Sixty-four
//   concurrent appends issued from one reply-processing pass therefore leave in
//   ONE socket write, and a lone append costs one write and no timer.
//
//   READS ALLOCATE NOTHING PER FRAME. A chunk off the socket is walked with one
//   DataView, each frame is handed to its slot as `(kind, flags, buf, start,
//   end)` — a view into the chunk — and the slot decodes only what it needs.
//   Events stay as bytes until somebody takes them.
//
// A frame that straddles two chunks is assembled into a buffer allocated at its
// final size, so a large read is copied once, not once per chunk.
// ---------------------------------------------------------------------------

import { connect as netConnect, isIP } from "node:net"
import { connect as tlsConnect, type ConnectionOptions as TlsConnectionOptions } from "node:tls"
import { ConnectionFailedError, KronosDbError, KronosDbErrorCode, requestError } from "./errors.js"
import {
  PREFACE,
  PROTOCOL_VERSION,
  CredentialKind,
  FLAG_END,
  FRAME_HEADER_LEN,
  FrameKind,
  decodeRequestFailed,
  decodeServerPing,
  decodeWelcome,
  encodeBindBus,
  encodeBindContext,
  encodeCancel,
  encodeClientPong,
  encodeHello,
  ensure,
  reader,
  writer,
  type Writer,
} from "./protocol/kronosdb-protocol.js"

/** The stored record layout this client decodes (protocol version 1). */
const RECORD_FORMAT = 3

/** What the server said in Welcome, in this package's units. */
export type NativeWelcome = {
  readonly version: number
  readonly serverName: string
  readonly principal: string
  /** Largest frame either side may send, header included. */
  readonly maxFrame: number
  /** How often the server pings an idle connection, in milliseconds. */
  readonly heartbeatMs: number
}

export type NativeTransportOptions = {
  readonly host: string
  readonly port: number
  /** Present for TLS: trust roots and a client certificate, as `node:tls` takes them. */
  readonly tls?: Pick<TlsConnectionOptions, "ca" | "cert" | "key" | "servername" | "rejectUnauthorized">
  /** Application name, shown in the admin console. */
  readonly clientName: string
  /** This process, unique per instance. */
  readonly clientId: string
  /** A bearer token, or "" for none (an open server, or a TLS client certificate). */
  readonly token: string
  /** Connection plus handshake budget. Default: 10000. */
  readonly connectTimeoutMs?: number
}

/**
 * Where a stream's frames go: `(kind, flags, buf, start, end)`, where `buf` is
 * the received chunk and `start..end` is the frame BODY. Valid only for the
 * call unless the receiver keeps `buf` — it is a view, never reused.
 */
export type FrameSink = (kind: number, flags: number, buf: Uint8Array, start: number, end: number) => void

/** Parses the one frame that answers a unary request. May throw. */
export type ReplyParser<T> = (kind: number, buf: Uint8Array, start: number, end: number) => T

/** The client's end of an open stream. */
export type NativeStream = {
  /** Send another frame on the stream (a subscription's Credit). */
  send(kind: number, body: (w: Writer) => void): void
  /**
   * Give up on the stream: tell the server, and stop delivering. Nothing more
   * reaches the sink or `fail` after this returns. Safe to call twice.
   */
  cancel(): void
}

export type NativeTransport = {
  readonly welcome: NativeWelcome
  readonly closed: boolean
  /** The server sent GoAway: finish what is open, start nothing new here. */
  readonly draining: boolean
  /** Send a request answered by exactly one frame. */
  request<T>(kind: number, body: (w: Writer) => void, parse: ReplyParser<T>): Promise<T>
  /**
   * Open a stream. With `end` the client sends nothing more (a Source); without
   * it the client may keep sending (a Subscribe and its credits). Every frame
   * the server sends on the stream reaches `sink`; a RequestFailed, or the
   * connection closing, reaches `fail` instead and ends the stream.
   */
  open(
    kind: number,
    body: (w: Writer) => void,
    end: boolean,
    sink: FrameSink,
    fail: (error: Error) => void,
  ): NativeStream
  /** The wire id of a context on this connection, binding it on first use. */
  contextId(name: string): number | Promise<number>
  /** The wire id of a messaging bus on this connection, binding it on first use. */
  busId(name: string): number | Promise<number>
  /** Called once when the connection closes, with the reason if it was not asked to. */
  onClose(listener: (error?: Error) => void): void
  /** Called when the server announces it is draining. */
  onGoAway(listener: () => void): void
  /** Close the connection. In-flight requests fail with a ConnectionFailedError. */
  close(): void
}

type Slot = {
  /** A unary slot ends with its first frame, whatever the flags say. */
  readonly unary: boolean
  readonly sink: FrameSink
  readonly fail: (error: Error) => void
}

const DEFAULT_CONNECT_TIMEOUT_MS = 10_000
/** Until Welcome says otherwise: room for Hello and a Welcome carrying long names. */
const HANDSHAKE_MAX_FRAME = 1 << 20
const MIN_WRITE_BUFFER = 1024
const MAX_WRITE_BUFFER = 1 << 20

const WELCOME = FrameKind.Welcome
const REQUEST_FAILED = FrameKind.RequestFailed

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * Open a connection and complete the handshake.
 *
 * Resolves with a live transport, or rejects with the reason the server (or
 * the network) refused: an AuthenticationError for bad credentials, a
 * ConnectionFailedError for everything between here and the server.
 */
export function openNativeTransport(options: NativeTransportOptions): Promise<NativeTransport> {
  return new Promise<NativeTransport>((resolve, reject) => {
    const connectTimeoutMs = options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS
    const target = `${options.host}:${options.port}`

    const socket = options.tls
      ? tlsConnect({ host: options.host, port: options.port, servername: isIP(options.host) === 0 ? options.host : undefined, ...options.tls })
      : netConnect({ host: options.host, port: options.port })
    socket.setNoDelay(true)

    // ── state ────────────────────────────────────────────────────────────
    let welcome: NativeWelcome | undefined
    let maxFrame = HANDSHAKE_MAX_FRAME
    let closed = false
    let draining = false
    let w = writer(MIN_WRITE_BUFFER)
    let flushQueued = false
    let lastStream = 0
    let lastTraffic = Date.now()
    let livenessTimer: ReturnType<typeof setInterval> | undefined
    let handshakeTimer: ReturnType<typeof setTimeout> | undefined
    const slots = new Map<number, Slot>()
    const closeListeners: Array<(error?: Error) => void> = []
    const goAwayListeners: Array<() => void> = []

    // A frame that arrived in pieces: `pending` is allocated at its full size
    // once the length prefix is known, `filled` bytes of it are in.
    let pending: Uint8Array | undefined
    let filled = 0
    let pendingTotal = 0

    // ── writing ──────────────────────────────────────────────────────────
    function flush(): void {
      flushQueued = false
      if (closed || w.pos === 0) return
      const out = w.buf.subarray(0, w.pos)
      // The socket may hold `out` until the kernel takes it, so it gets its own
      // buffer and the next frames start in a fresh one, sized to the last burst.
      w = writer(Math.min(MAX_WRITE_BUFFER, Math.max(MIN_WRITE_BUFFER, 1 << (32 - Math.clz32(out.length)))))
      socket.write(out)
    }

    /** Encode one frame into the shared buffer; it leaves at the end of the tick. */
    function writeFrame(stream: number, flags: number, kind: number, body: (w: Writer) => void): void {
      if (closed) throw new ConnectionFailedError(`The connection to ${target} is closed`)
      const start = w.pos
      try {
        ensure(w, FRAME_HEADER_LEN)
        const view = w.view
        view.setUint16(start + 4, kind, true)
        w.buf[start + 6] = flags
        view.setUint32(start + 7, stream, true)
        w.pos = start + FRAME_HEADER_LEN
        body(w)
        const size = w.pos - start
        if (size > maxFrame) {
          throw new KronosDbError(
            `A frame of ${size} bytes exceeds the server's limit of ${maxFrame}; split the request into smaller batches`,
            KronosDbErrorCode.EVENT_PAYLOAD_TOO_LARGE,
            false,
          )
        }
        // `body` may have grown the buffer, so the view is read again.
        w.view.setUint32(start, size - 4, true)
      } catch (error) {
        w.pos = start
        throw error
      }
      if (!flushQueued) {
        flushQueued = true
        queueMicrotask(flush)
      }
    }

    // ── closing ──────────────────────────────────────────────────────────
    function shutdown(error: Error | undefined, graceful: boolean): void {
      if (closed) return
      closed = true
      clearInterval(livenessTimer)
      clearTimeout(handshakeTimer)
      if (graceful && w.pos > 0) {
        socket.write(w.buf.subarray(0, w.pos))
        w.pos = 0
      }
      const failure = error ?? new ConnectionFailedError(`The connection to ${target} is closed`)
      const open = [...slots.values()]
      slots.clear()
      for (const slot of open) {
        try {
          slot.fail(failure)
        } catch {
          // A sink's failure cannot keep the others from hearing the connection died.
        }
      }
      if (graceful) socket.end()
      socket.destroy()
      if (welcome === undefined) reject(failure)
      for (const listener of closeListeners) {
        try {
          listener(error)
        } catch {
          // Listener failures cannot prevent teardown.
        }
      }
    }

    // ── reading ──────────────────────────────────────────────────────────
    function frame(buf: Uint8Array, view: DataView, at: number, total: number): void {
      const kind = view.getUint16(at + 4, true)
      const flags = buf[at + 6]!
      const stream = view.getUint32(at + 7, true)
      const start = at + FRAME_HEADER_LEN
      const end = at + total

      if (stream === 0) {
        connectionFrame(kind, buf, start, end)
        return
      }
      const slot = slots.get(stream)
      // Finished or cancelled: the server may still have frames in flight.
      if (slot === undefined) return
      if (kind === REQUEST_FAILED) {
        slots.delete(stream)
        const failed = decodeRequestFailed(reader(buf, start, end))
        slot.fail(requestError(failed.code, failed.message))
        return
      }
      if (slot.unary || (flags & FLAG_END) !== 0) slots.delete(stream)
      try {
        slot.sink(kind, flags, buf, start, end)
      } catch (error) {
        // A sink that throws is a bug in the receiver or a frame it cannot read;
        // that stream is over, the rest of the connection is not.
        slots.delete(stream)
        try {
          writeFrame(stream, FLAG_END, FrameKind.Cancel, encodeCancelBody)
        } catch {
          // The connection is going away; nothing more to say on it.
        }
        slot.fail(error instanceof Error ? error : new Error(String(error)))
      }
    }

    function connectionFrame(kind: number, buf: Uint8Array, start: number, end: number): void {
      switch (kind) {
        case WELCOME:
          if (welcome === undefined) {
            handshakeFinished(buf, start, end)
            return
          }
          throw new KronosDbError("The server sent Welcome twice", KronosDbErrorCode.OTHER, false)
        case FrameKind.ServerPing: {
          const ping = decodeServerPing(reader(buf, start, end))
          writeFrame(0, 0, FrameKind.ClientPong, (out) => encodeClientPong(out, ping))
          return
        }
        case FrameKind.Pong:
          return
        case FrameKind.GoAway: {
          draining = true
          for (const listener of goAwayListeners) {
            try {
              listener()
            } catch {
              // Listener failures are isolated.
            }
          }
          return
        }
        case REQUEST_FAILED: {
          // On stream 0 a failure is about the connection itself: refused
          // credentials during the handshake, a protocol violation afterwards.
          const failed = decodeRequestFailed(reader(buf, start, end))
          throw requestError(failed.code, failed.message)
        }
        default:
          throw new KronosDbError(
            `The server sent frame ${kind.toString(16)} on stream 0, where only connection frames travel`,
            KronosDbErrorCode.OTHER,
            false,
          )
      }
    }

    function handshakeFinished(buf: Uint8Array, start: number, end: number): void {
      const w1 = decodeWelcome(reader(buf, start, end))
      if (w1.version !== PROTOCOL_VERSION) {
        throw new KronosDbError(
          `The server speaks native protocol version ${w1.version}; this client speaks ${PROTOCOL_VERSION}`,
          KronosDbErrorCode.OTHER,
          false,
        )
      }
      if (w1.recordFormat !== RECORD_FORMAT) {
        throw new KronosDbError(
          `The server stores record format ${w1.recordFormat}; this client decodes format ${RECORD_FORMAT}`,
          KronosDbErrorCode.OTHER,
          false,
        )
      }
      welcome = {
        version: w1.version,
        serverName: w1.serverName,
        principal: w1.principal,
        maxFrame: w1.maxFrame,
        heartbeatMs: w1.heartbeat,
      }
      maxFrame = w1.maxFrame
      clearTimeout(handshakeTimer)
      // The server pings an idle connection every heartbeat; three of those
      // with nothing at all means it is gone. (The server gives up after two
      // unanswered pings, so a live-but-silent peer is closed on its side first.)
      const interval = Math.max(w1.heartbeat, 1000)
      livenessTimer = setInterval(() => {
        if (Date.now() - lastTraffic > interval * 3) {
          shutdown(new ConnectionFailedError(`The server at ${target} stopped answering`), false)
        }
      }, interval)
      livenessTimer.unref?.()
      resolve(transport)
    }

    /** Walk the whole frames in `buf`; keep the tail of a partial one. */
    function consume(buf: Uint8Array): void {
      const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength)
      let at = 0
      while (buf.length - at >= 4) {
        const len = view.getUint32(at, true)
        const total = len + 4
        if (len < FRAME_HEADER_LEN - 4 || total > maxFrame) {
          if (welcome === undefined) {
            throw new KronosDbError(
              `${target} does not speak the KronosDB native protocol: it answered the handshake with something else. ` +
                `Check the address, and that the server is a KronosDB build with the native protocol (and that ssl matches whether it serves TLS)`,
              KronosDbErrorCode.CONNECTION_FAILED,
              false,
            )
          }
          throw new KronosDbError(
            `The server sent a frame of ${total} bytes (limit ${maxFrame}); closing the connection`,
            KronosDbErrorCode.OTHER,
            false,
          )
        }
        if (buf.length - at < total) {
          pending = new Uint8Array(total)
          pending.set(buf.subarray(at))
          filled = buf.length - at
          pendingTotal = total
          return
        }
        frame(buf, view, at, total)
        at += total
        if (closed) return
      }
      if (at < buf.length) {
        // Not even a length prefix yet: keep the few bytes there are.
        pending = new Uint8Array(buf.subarray(at))
        filled = pending.length
        pendingTotal = 0
      }
    }

    function onData(chunk: Uint8Array): void {
      lastTraffic = Date.now()
      try {
        if (pending === undefined) {
          consume(chunk)
          return
        }
        if (pendingTotal === 0) {
          // Fewer than four bytes were waiting: join and look again.
          const joined = new Uint8Array(filled + chunk.length)
          joined.set(pending)
          joined.set(chunk, filled)
          pending = undefined
          consume(joined)
          return
        }
        const take = Math.min(chunk.length, pendingTotal - filled)
        pending.set(chunk.subarray(0, take), filled)
        filled += take
        if (filled < pendingTotal) return
        const whole = pending
        pending = undefined
        consume(whole)
        if (!closed && take < chunk.length) onData(chunk.subarray(take))
      } catch (error) {
        shutdown(
          error instanceof KronosDbError ? error : new ConnectionFailedError(`The native protocol failed: ${describe(error)}`),
          false,
        )
      }
    }

    // ── the transport ────────────────────────────────────────────────────
    function nextStream(): number {
      do {
        lastStream = lastStream === 0xffffffff ? 1 : lastStream + 1
      } while (slots.has(lastStream))
      return lastStream
    }

    const encodeCancelBody = (out: Writer): void => encodeCancel(out, {})

    function request<T>(kind: number, body: (w: Writer) => void, parse: ReplyParser<T>): Promise<T> {
      return new Promise<T>((resolveReply, rejectReply) => {
        const id = nextStream()
        slots.set(id, {
          unary: true,
          sink: (replyKind, _flags, buf, start, end) => {
            try {
              resolveReply(parse(replyKind, buf, start, end))
            } catch (error) {
              rejectReply(error)
            }
          },
          fail: rejectReply,
        })
        try {
          writeFrame(id, FLAG_END, kind, body)
        } catch (error) {
          slots.delete(id)
          rejectReply(error)
        }
      })
    }

    function open(
      kind: number,
      body: (w: Writer) => void,
      end: boolean,
      sink: FrameSink,
      fail: (error: Error) => void,
    ): NativeStream {
      const id = nextStream()
      const slot: Slot = { unary: false, sink, fail }
      slots.set(id, slot)
      try {
        writeFrame(id, end ? FLAG_END : 0, kind, body)
      } catch (error) {
        slots.delete(id)
        throw error
      }
      return {
        send(frameKind, frameBody) {
          if (slots.get(id) !== slot) return
          writeFrame(id, 0, frameKind, frameBody)
        },
        cancel() {
          if (slots.get(id) !== slot) return
          slots.delete(id)
          if (closed) return
          try {
            writeFrame(id, FLAG_END, FrameKind.Cancel, encodeCancelBody)
          } catch {
            // Nothing to tell a server that is already gone.
          }
        },
      }
    }

    // A context and a bus are bound the same way: a small integer the connection
    // agrees on once, so requests carry two bytes instead of a name. Id 0 is
    // "default" from the start. A binder hands the id back synchronously when the
    // name is already bound (every call after the first) and a promise while the
    // server is still checking it.
    function binder(
      what: string,
      requestKind: number,
      boundKind: number,
      encode: (out: Writer, id: number, name: string) => void,
    ): (name: string) => number | Promise<number> {
      const bound = new Map<string, number | Promise<number>>([["default", 0]])
      let next = 1
      return (name) => {
        const known = bound.get(name)
        if (known !== undefined) return known
        if (next > 0xffff) {
          throw new KronosDbError(`More than 65536 ${what}s on one connection`, KronosDbErrorCode.OTHER, false)
        }
        const id = next++
        const pending = request(
          requestKind,
          (out) => encode(out, id, name),
          (kind) => {
            if (kind !== boundKind) {
              throw new KronosDbError(`Expected ${what} binding for '${name}', got frame ${kind.toString(16)}`, KronosDbErrorCode.OTHER, false)
            }
            return id
          },
        ).then(
          (done) => {
            bound.set(name, done)
            return done
          },
          (error) => {
            bound.delete(name)
            throw error
          },
        )
        bound.set(name, pending)
        return pending
      }
    }

    const contextId = binder("context", FrameKind.BindContext, FrameKind.ContextBound, (out, id, name) => encodeBindContext(out, { id, name }))
    const busId = binder("bus", FrameKind.BindBus, FrameKind.BusBound, (out, id, name) => encodeBindBus(out, { id, name }))

    const transport: NativeTransport = {
      get welcome() {
        return welcome!
      },
      get closed() {
        return closed
      },
      get draining() {
        return draining
      },
      request,
      open,
      contextId,
      busId,
      onClose(listener) {
        if (closed) listener()
        else closeListeners.push(listener)
      },
      onGoAway(listener) {
        goAwayListeners.push(listener)
      },
      close() {
        shutdown(undefined, true)
      },
    }

    // ── connecting ───────────────────────────────────────────────────────
    handshakeTimer = setTimeout(() => {
      shutdown(new ConnectionFailedError(`Timed out after ${connectTimeoutMs}ms connecting to ${target}`), false)
    }, connectTimeoutMs)

    socket.on("data", onData)
    socket.on("error", (error: Error) => {
      shutdown(new ConnectionFailedError(`Cannot reach KronosDB at ${target}: ${describe(error)}`), false)
    })
    socket.on("close", () => {
      shutdown(welcome === undefined ? new ConnectionFailedError(`${target} closed the connection during the handshake`) : undefined, false)
    })
    socket.once(options.tls ? "secureConnect" : "connect", () => {
      // The preface and Hello go out together. Hello is the first frame, so the
      // server decides credentials before any request is read.
      const first = writer(256)
      first.buf.set(PREFACE, 0)
      first.pos = PREFACE.length
      const at = first.pos
      ensure(first, FRAME_HEADER_LEN)
      first.view.setUint16(at + 4, FrameKind.Hello, true)
      first.buf[at + 6] = 0
      first.view.setUint32(at + 7, 0, true)
      first.pos = at + FRAME_HEADER_LEN
      try {
        encodeHello(first, {
          version: PROTOCOL_VERSION,
          features: 0n,
          recordFormat: RECORD_FORMAT,
          clientName: options.clientName,
          clientId: options.clientId,
          credentialKind: options.token === "" ? CredentialKind.none : CredentialKind.token,
          credential: options.token,
        })
      } catch (error) {
        shutdown(new KronosDbError(`Cannot encode Hello: ${describe(error)}`, KronosDbErrorCode.OTHER, false), false)
        return
      }
      first.view.setUint32(at, first.pos - at - 4, true)
      socket.write(first.buf.subarray(0, first.pos))
    })
  })
}
