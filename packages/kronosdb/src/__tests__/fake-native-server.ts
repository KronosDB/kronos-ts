// A KronosDB native server in thirty lines of node:net, for tests that need to
// say exactly which bytes arrive and when: the handshake, an answer split into
// single bytes, a connection that dies mid-reply. It speaks through the vendored
// codec, so a test reads and writes frames, not offsets.

import { createServer, type Server, type Socket } from "node:net"
import {
  PREFACE,
  decodeFrameBody,
  encodeFrame,
  type Frame,
  type Hello,
  readFrameHeader,
  reader,
  writer,
  written,
} from "../protocol/kronosdb-protocol.js"

export type ReceivedFrame = {
  readonly stream: number
  readonly flags: number
  readonly frame: Frame
}

export type FakeConnection = {
  readonly hello: Hello
  /** Frames received after Hello, in order. */
  readonly received: ReceivedFrame[]
  /** Frames per socket read: how the client's writes were coalesced. */
  readonly framesPerRead: number[]
  /** Encode a frame and write it. */
  reply(stream: number, flags: number, frame: Frame): void
  /** Write bytes exactly as given. */
  raw(bytes: Uint8Array): void
  /** Encode a frame and write it one byte at a time. */
  replyInPieces(stream: number, flags: number, frame: Frame): Promise<void>
  destroy(): void
  /** The socket has closed, from either end. */
  readonly closed: boolean
}

export type FakeNativeServerOptions = {
  /** Called for every frame after Hello. */
  onFrame?: (connection: FakeConnection, received: ReceivedFrame) => void
  /** Called when a connection that finished its handshake closes. */
  onClose?: (connection: FakeConnection) => void
  /** Answer the handshake with this failure instead of Welcome. */
  refuse?: { code: number; message: string }
  maxFrame?: number
  heartbeatMs?: number
  principal?: string
  /** Listen here instead of on a free port: a server coming back where it was. */
  port?: number
}

export type FakeNativeServer = {
  readonly port: number
  readonly connections: FakeConnection[]
  /** Resolves with the next connection to finish its handshake. */
  nextConnection(): Promise<FakeConnection>
  close(): Promise<void>
}

export async function fakeNativeServer(options: FakeNativeServerOptions = {}): Promise<FakeNativeServer> {
  const connections: FakeConnection[] = []
  const waiting: Array<(connection: FakeConnection) => void> = []
  const sockets = new Set<Socket>()

  const server: Server = createServer((socket) => {
    sockets.add(socket)
    let closed = false
    socket.on("close", () => {
      sockets.delete(socket)
      closed = true
      if (connection) options.onClose?.(connection)
    })
    socket.on("error", () => {})
    socket.setNoDelay(true)
    let buffered = new Uint8Array(0)
    let connection: FakeConnection | undefined
    let prefaceSeen = false

    const send = (stream: number, flags: number, frame: Frame): Uint8Array => {
      const w = writer(64)
      encodeFrame(w, stream, flags, frame)
      return written(w).slice()
    }

    socket.on("data", (chunk: Uint8Array) => {
      const joined = new Uint8Array(buffered.length + chunk.length)
      joined.set(buffered)
      joined.set(chunk, buffered.length)
      buffered = joined
      if (!prefaceSeen) {
        if (buffered.length < PREFACE.length) return
        prefaceSeen = true
        buffered = buffered.slice(PREFACE.length)
      }
      let at = 0
      let frames = 0
      for (;;) {
        const header = readFrameHeader(buffered, at)
        if (!header) break
        const frame = decodeFrameBody(header.kind, reader(buffered, header.bodyStart, header.end))
        if (!frame) throw new Error(`fake server: unknown frame kind ${header.kind}`)
        at = header.end
        frames++
        if (connection === undefined) {
          if (frame.name !== "Hello") throw new Error("fake server: the first frame must be Hello")
          if (options.refuse) {
            socket.write(send(0, 0, { name: "RequestFailed", body: options.refuse }))
            socket.end()
            continue
          }
          const hello = frame.body
          const received: ReceivedFrame[] = []
          const framesPerRead: number[] = []
          connection = {
            hello,
            received,
            framesPerRead,
            reply: (stream, flags, f) => void socket.write(send(stream, flags, f)),
            raw: (bytes) => void socket.write(bytes),
            replyInPieces: async (stream, flags, f) => {
              const bytes = send(stream, flags, f)
              for (const byte of bytes) {
                socket.write(new Uint8Array([byte]))
                await new Promise((resolve) => setTimeout(resolve, 0))
              }
            },
            destroy: () => socket.destroy(),
            get closed() {
              return closed
            },
          }
          connections.push(connection)
          socket.write(
            send(0, 0, {
              name: "Welcome",
              body: {
                version: 1,
                features: 0n,
                recordFormat: 3,
                serverName: "fake",
                principal: options.principal ?? "fake:tester",
                maxFrame: options.maxFrame ?? 1 << 20,
                heartbeat: options.heartbeatMs ?? 15_000,
              },
            }),
          )
          waiting.shift()?.(connection)
          frames = 0
          continue
        }
        const received = { stream: header.stream, flags: header.flags, frame }
        connection.received.push(received)
        options.onFrame?.(connection, received)
      }
      if (connection && frames > 0) connection.framesPerRead.push(frames)
      buffered = buffered.slice(at)
    })
  })

  await new Promise<void>((resolve) => server.listen(options.port ?? 0, "127.0.0.1", resolve))
  const address = server.address()
  if (address === null || typeof address === "string") throw new Error("fake server: no port")

  return {
    port: address.port,
    connections,
    nextConnection() {
      return new Promise((resolve) => waiting.push(resolve))
    },
    close() {
      for (const socket of sockets) socket.destroy()
      return new Promise((resolve) => server.close(() => resolve()))
    },
  }
}
