import { afterEach, describe, expect, it } from "bun:test"
import { AuthenticationError, ConnectionFailedError, KronosDbRequestError } from "../errors.js"
import { openNativeTransport, type NativeTransport } from "../native-transport.js"
import {
  ErrorCode,
  FLAG_END,
  FrameKind,
  decodeHead,
  encodeBindContext,
  encodeGetHead,
  ensure,
  reader,
  type Frame,
} from "../protocol/kronosdb-protocol.js"
import { fakeNativeServer, type FakeNativeServer, type FakeNativeServerOptions } from "./fake-native-server.js"

let server: FakeNativeServer | undefined
let transport: NativeTransport | undefined

afterEach(async () => {
  transport?.close()
  transport = undefined
  await server?.close()
  server = undefined
})

async function connected(options: FakeNativeServerOptions = {}, token = "") {
  server = await fakeNativeServer(options)
  const next = server.nextConnection()
  transport = await openNativeTransport({
    host: "127.0.0.1",
    port: server.port,
    clientName: "transport-test",
    clientId: "client-1",
    token,
    connectTimeoutMs: 2000,
  })
  return { transport, server, connection: await next }
}

const getHead = (t: NativeTransport) =>
  t.request(
    FrameKind.GetHead,
    (w) => encodeGetHead(w, { context: 0 }),
    (_kind, buf, start, end) => decodeHead(reader(buf, start, end)).position,
  )

const head = (position: bigint): Frame => ({ name: "Head", body: { position } })

describe("native transport — handshake", () => {
  it("sends the preface and Hello, and reports what Welcome says", async () => {
    const { transport, connection } = await connected({ principal: "token:ci", maxFrame: 4096, heartbeatMs: 5000 }, "secret")
    expect(connection.hello).toMatchObject({
      version: 1,
      recordFormat: 3,
      clientName: "transport-test",
      clientId: "client-1",
      credential: "secret",
    })
    expect(connection.hello.credentialKind).toBe(1)
    expect(transport.welcome).toEqual({ version: 1, serverName: "fake", principal: "token:ci", maxFrame: 4096, heartbeatMs: 5000 })
  })

  it("sends no credential when there is no token", async () => {
    const { connection } = await connected()
    expect(connection.hello.credentialKind).toBe(0)
    expect(connection.hello.credential).toBe("")
  })

  it("rejects refused credentials as an AuthenticationError", async () => {
    server = await fakeNativeServer({ refuse: { code: ErrorCode.unauthenticated, message: "bad token" } })
    const failure = await openNativeTransport({
      host: "127.0.0.1",
      port: server.port,
      clientName: "x",
      clientId: "y",
      token: "nope",
    }).then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(AuthenticationError)
    expect((failure as Error).message).toBe("bad token")
  })

  it("reports a closed port as a transient ConnectionFailedError", async () => {
    const probe = await fakeNativeServer()
    const port = probe.port
    await probe.close()
    const failure = await openNativeTransport({ host: "127.0.0.1", port, clientName: "x", clientId: "y", token: "" }).then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(ConnectionFailedError)
    expect((failure as ConnectionFailedError).transient).toBe(true)
  })
})

describe("native transport — requests", () => {
  it("multiplexes: answers may arrive in any order", async () => {
    const { transport, connection } = await connected({
      onFrame() {},
    })
    const first = getHead(transport)
    const second = getHead(transport)
    const third = getHead(transport)
    await Bun.sleep(20)
    const streams = connection.received.map((r) => r.stream)
    expect(new Set(streams).size).toBe(3)
    // Answer in reverse; each caller still gets its own answer.
    connection.reply(streams[2]!, FLAG_END, head(30n))
    connection.reply(streams[0]!, FLAG_END, head(10n))
    connection.reply(streams[1]!, FLAG_END, head(20n))
    expect(await Promise.all([first, second, third])).toEqual([10n, 20n, 30n])
    // Requests are unary: they carry END.
    expect(connection.received.every((r) => (r.flags & FLAG_END) !== 0)).toBe(true)
  })

  it("writes everything issued in one tick in one socket write", async () => {
    const { transport, connection } = await connected()
    for (let i = 0; i < 64; i++) void getHead(transport).catch(() => {})
    await Bun.sleep(30)
    expect(connection.received.length).toBe(64)
    expect(connection.framesPerRead.reduce((a, b) => a + b, 0)).toBe(64)
    expect(connection.framesPerRead.length).toBeLessThanOrEqual(2)
    transport.close()
  })

  it("answers pipelined requests whose replies arrive glued into one chunk", async () => {
    const { transport, connection } = await connected({
      onFrame(conn, received) {
        // Echo the position back as the stream id, END.
        conn.reply(received.stream, FLAG_END, head(BigInt(received.stream)))
      },
    })
    const heads = await Promise.all(Array.from({ length: 50 }, () => getHead(transport)))
    expect(heads.length).toBe(50)
    expect(new Set(heads).size).toBe(50)
    expect(connection.received.length).toBe(50)
  })

  it("reassembles a reply that arrives one byte at a time", async () => {
    const { transport, connection } = await connected({
      onFrame(conn, received) {
        void conn.replyInPieces(received.stream, FLAG_END, head(7n))
      },
    })
    expect(await getHead(transport)).toBe(7n)
    expect(connection.received.length).toBe(1)
  })

  it("rejects with the server's status when a request fails", async () => {
    const { transport } = await connected({
      onFrame(conn, received) {
        conn.reply(received.stream, FLAG_END, {
          name: "RequestFailed",
          body: { code: ErrorCode.not_found, message: "no such thing" },
        })
      },
    })
    const failure = await getHead(transport).then(
      () => undefined,
      (e) => e,
    )
    expect(failure).toBeInstanceOf(KronosDbRequestError)
    expect(failure.status).toBe("not_found")
    expect(failure.message).toBe("no such thing")
    expect(failure.transient).toBe(false)
  })

  it("marks `unavailable` as transient and unknown codes as internal", async () => {
    const codes = [ErrorCode.unavailable, 999]
    const seen: Array<{ status: string; transient: boolean }> = []
    const { transport } = await connected({
      onFrame(conn, received) {
        conn.reply(received.stream, FLAG_END, {
          name: "RequestFailed",
          body: { code: codes[seen.length]!, message: "m" },
        })
      },
    })
    for (let i = 0; i < codes.length; i++) {
      const failure = await getHead(transport).then(
        () => undefined,
        (e) => e,
      )
      seen.push({ status: failure.status, transient: failure.transient })
    }
    expect(seen).toEqual([
      { status: "unavailable", transient: true },
      { status: "internal", transient: false },
    ])
  })

  it("fails a request whose frame exceeds the server's limit, and keeps the connection", async () => {
    const { transport } = await connected({
      maxFrame: 64,
      onFrame(conn, received) {
        conn.reply(received.stream, FLAG_END, head(1n))
      },
    })
    // A context name this long cannot fit a 64-byte frame.
    const tooBig = transport.request(
      FrameKind.BindContext,
      (w) => encodeBindContext(w, { id: 5, name: "x".repeat(200) }),
      () => undefined,
    )
    const failure = await tooBig.then(
      () => undefined,
      (e) => e,
    )
    expect(failure.message).toMatch(/exceeds the server's limit of 64/)
    expect(transport.closed).toBe(false)
    expect(await getHead(transport)).toBe(1n)
  })
})

describe("native transport — streams", () => {
  it("delivers every frame to the sink until END, then forgets the stream", async () => {
    const { transport, connection } = await connected({
      onFrame(conn, received) {
        conn.reply(received.stream, 0, head(1n))
        conn.reply(received.stream, 0, head(2n))
        conn.reply(received.stream, FLAG_END, head(3n))
        // After END the stream is finished: a stray frame is dropped.
        conn.reply(received.stream, 0, head(4n))
      },
    })
    const seen: bigint[] = []
    await new Promise<void>((resolve, reject) => {
      transport.open(
        FrameKind.GetHead,
        (w) => encodeGetHead(w, { context: 0 }),
        true,
        (_kind, flags, buf, start) => {
          seen.push(new DataView(buf.buffer, buf.byteOffset, buf.byteLength).getBigUint64(start, true))
          if (flags & FLAG_END) resolve()
        },
        reject,
      )
    })
    await Bun.sleep(20)
    expect(seen).toEqual([1n, 2n, 3n])
    expect(connection.received.length).toBe(1)
  })

  it("fails the stream with the server's reason", async () => {
    const { transport } = await connected({
      onFrame(conn, received) {
        conn.reply(received.stream, 0, head(1n))
        conn.reply(received.stream, FLAG_END, { name: "RequestFailed", body: { code: ErrorCode.rejected, message: "stopped" } })
      },
    })
    const failure = await new Promise<Error>((resolve) => {
      transport.open(FrameKind.GetHead, (w) => encodeGetHead(w, { context: 0 }), false, () => {}, resolve)
    })
    expect(failure.message).toBe("stopped")
  })

  it("cancel sends Cancel with END and silences the stream", async () => {
    const { transport, connection } = await connected()
    let delivered = 0
    let failed = 0
    const stream = transport.open(
      FrameKind.GetHead,
      (w) => encodeGetHead(w, { context: 0 }),
      false,
      () => delivered++,
      () => failed++,
    )
    await Bun.sleep(10)
    const opened = connection.received[0]!
    expect((opened.flags & FLAG_END)).toBe(0)
    stream.cancel()
    stream.cancel()
    await Bun.sleep(20)
    const cancels = connection.received.filter((r) => r.frame.name === "Cancel")
    expect(cancels.length).toBe(1)
    expect(cancels[0]!.stream).toBe(opened.stream)
    expect(cancels[0]!.flags & FLAG_END).toBe(FLAG_END)
    // The server's confirmation arrives after the client stopped listening.
    connection.reply(opened.stream, FLAG_END, {
      name: "RequestFailed",
      body: { code: ErrorCode.cancelled, message: "cancelled" },
    })
    connection.reply(opened.stream, 0, head(1n))
    await Bun.sleep(20)
    expect({ delivered, failed }).toEqual({ delivered: 0, failed: 0 })
  })

  it("lets the client send more frames on an open stream", async () => {
    const { transport, connection } = await connected()
    const stream = transport.open(FrameKind.GetHead, (w) => encodeGetHead(w, { context: 0 }), false, () => {}, () => {})
    stream.send(FrameKind.Credit, (w) => {
      ensure(w, 4)
      w.view.setUint32(w.pos, 5, true)
      w.pos += 4
    })
    await Bun.sleep(20)
    expect(connection.received.map((r) => r.frame.name)).toEqual(["GetHead", "Credit"])
    expect(connection.received[1]!.stream).toBe(connection.received[0]!.stream)
    expect(connection.received[1]!.frame).toEqual({ name: "Credit", body: { credits: 5 } })
  })

  it("survives a sink that throws: that stream fails, the connection does not", async () => {
    const { transport } = await connected({
      onFrame(conn, received) {
        conn.reply(received.stream, 0, head(1n))
      },
    })
    const failure = await new Promise<Error>((resolve) => {
      transport.open(
        FrameKind.GetHead,
        (w) => encodeGetHead(w, { context: 0 }),
        false,
        () => {
          throw new Error("cannot read this")
        },
        resolve,
      )
    })
    expect(failure.message).toBe("cannot read this")
    expect(transport.closed).toBe(false)
  })
})

describe("native transport — the connection", () => {
  it("answers ServerPing with ClientPong carrying the nonce", async () => {
    const { connection } = await connected()
    connection.reply(0, 0, { name: "ServerPing", body: { nonce: 42n } })
    await Bun.sleep(30)
    const pong = connection.received.find((r) => r.frame.name === "ClientPong")
    expect(pong).toBeDefined()
    expect(pong!.stream).toBe(0)
    expect(pong!.frame).toEqual({ name: "ClientPong", body: { nonce: 42n } })
  })

  it("marks the transport draining on GoAway but keeps serving open streams", async () => {
    const { transport, connection } = await connected()
    let away = 0
    transport.onGoAway(() => away++)
    const pending = getHead(transport)
    await Bun.sleep(10)
    connection.reply(0, 0, { name: "GoAway", body: { code: ErrorCode.unavailable, message: "restarting" } })
    await Bun.sleep(10)
    expect(transport.draining).toBe(true)
    expect(away).toBe(1)
    connection.reply(connection.received[0]!.stream, FLAG_END, head(9n))
    expect(await pending).toBe(9n)
  })

  it("fails everything in flight when the server drops the connection, and says so once", async () => {
    const { transport, connection } = await connected()
    const closes: Array<Error | undefined> = []
    transport.onClose((e) => closes.push(e))
    const request = getHead(transport)
    let streamFailure: Error | undefined
    transport.open(FrameKind.GetHead, (w) => encodeGetHead(w, { context: 0 }), false, () => {}, (e) => (streamFailure = e))
    await Bun.sleep(10)
    connection.destroy()
    const failure = await request.then(
      () => undefined,
      (e) => e,
    )
    await Bun.sleep(10)
    expect(failure).toBeInstanceOf(ConnectionFailedError)
    expect(streamFailure).toBeInstanceOf(ConnectionFailedError)
    expect(transport.closed).toBe(true)
    expect(closes.length).toBe(1)
    // A request on a closed transport fails fast.
    const late = await getHead(transport).then(
      () => undefined,
      (e) => e,
    )
    expect(late).toBeInstanceOf(ConnectionFailedError)
  })

  it("closes on a frame larger than the server promised", async () => {
    const { transport, connection } = await connected({ maxFrame: 64 })
    connection.raw(new Uint8Array([0xff, 0xff, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0]))
    await Bun.sleep(30)
    expect(transport.closed).toBe(true)
  })

  it("close() fails open requests and is idempotent", async () => {
    const { transport } = await connected()
    const request = getHead(transport)
    transport.close()
    transport.close()
    expect((await request.then(() => undefined, (e) => e)) instanceof ConnectionFailedError).toBe(true)
  })

  it("binds a named context once, however many callers ask", async () => {
    const { transport, connection } = await connected({
      onFrame(conn, received) {
        if (received.frame.name === "BindContext") {
          conn.reply(received.stream, FLAG_END, { name: "ContextBound", body: { id: received.frame.body.id } })
        }
      },
    })
    expect(transport.contextId("default")).toBe(0)
    const ids = await Promise.all([transport.contextId("orders"), transport.contextId("orders"), transport.contextId("billing")])
    expect(ids).toEqual([1, 1, 2])
    // Bound now: answered without a round trip.
    expect(transport.contextId("orders")).toBe(1)
    expect(connection.received.filter((r) => r.frame.name === "BindContext").length).toBe(2)
  })

  it("forgets a binding the server refused, so a later call can try again", async () => {
    let answers = 0
    const { transport } = await connected({
      onFrame(conn, received) {
        if (received.frame.name !== "BindContext") return
        answers++
        conn.reply(
          received.stream,
          FLAG_END,
          answers === 1
            ? { name: "RequestFailed", body: { code: ErrorCode.not_found, message: "context not found: ghost" } }
            : { name: "ContextBound", body: { id: received.frame.body.id } },
        )
      },
    })
    const refused = await Promise.resolve(transport.contextId("ghost")).then(
      () => undefined,
      (e) => e,
    )
    expect(refused.message).toBe("context not found: ghost")
    expect(await transport.contextId("ghost")).toBe(2)
  })
})
