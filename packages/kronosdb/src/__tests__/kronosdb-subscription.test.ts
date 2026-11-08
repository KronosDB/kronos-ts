import { afterEach, describe, expect, it } from "bun:test"
import { jsonSerializer } from "@kronos-ts/core"
import { connectToKronosDb, type KronosDbConnection } from "../connection.js"
import { kronosDbEventStore } from "../kronosdb-event-store.js"
import { kronosDbSubscription } from "../kronosdb-log-reads.js"
import { ErrorCode, FLAG_END, encodeFrame, writer, written, type EventRecord, type Frame } from "../protocol/kronosdb-protocol.js"
import { until } from "./bus-test-kit.js"
import { fakeNativeServer, type FakeConnection, type FakeNativeServer } from "./fake-native-server.js"

const serializer = jsonSerializer()
const enc = new TextEncoder()

let server: FakeNativeServer | undefined
let connection: KronosDbConnection | undefined

afterEach(async () => {
  connection?.close()
  connection = undefined
  await server?.close()
  server = undefined
})

async function subscribed(window?: number) {
  server = await fakeNativeServer()
  const opened = server.nextConnection()
  connection = connectToKronosDb({ host: "127.0.0.1", port: server.port, componentName: "sub-test", ...(window !== undefined ? { window } : {}) })
  const kdb = { connection, serializer }
  return { kdb, connection: () => opened }
}

const record = (position: bigint, padding = 0): { flags: number; event: EventRecord } => ({
  flags: 0,
  event: {
    position,
    identifier: `e${position}`,
    name: "shop.Ticked",
    version: "1",
    timestamp: 1,
    metadata: [],
    tags: [],
    payload: enc.encode(JSON.stringify({ n: Number(position), pad: "x".repeat(padding) })),
  },
})

const events = (from: number, count: number, padding = 0): Frame => ({
  name: "Events",
  body: { events: Array.from({ length: count }, (_, i) => record(BigInt(from + i), padding)) },
})

/** What a frame costs the stream's window: every byte of it, length prefix included. */
const wireSize = (frame: Frame): number => {
  const w = writer(1024)
  encodeFrame(w, 1, 0, frame)
  return written(w).length
}

/** The Subscribe the client sent, and its stream id. */
async function subscribeFrame(fake: FakeConnection) {
  for (let i = 0; i < 200; i++) {
    const frame = fake.received.find((r) => r.frame.name === "Subscribe")
    if (frame && frame.frame.name === "Subscribe") return { stream: frame.stream, flags: frame.flags, body: frame.frame.body }
    await Bun.sleep(5)
  }
  throw new Error("no Subscribe arrived")
}

const windows = (fake: FakeConnection) =>
  fake.received.flatMap((r) => (r.frame.name === "Window" ? [{ stream: r.stream, flags: r.flags, bytes: r.frame.body.bytes }] : []))

const HALF = 32 * 1024

describe("subscription (native)", () => {
  it("opens with Subscribe: where to start, what to match, END and no credits", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 25n, query: { tags: { orderId: "o-1" }, types: ["shop.Ticked"] } })
    const fake = await conn()
    const sub = await subscribeFrame(fake)

    // The client sends nothing in a subscription; the window paces it, and Window and Cancel may still follow.
    expect(sub.flags & FLAG_END).toBe(FLAG_END)
    // A query is a selective reader, so the server is asked to say how far it has looked.
    expect(sub.body).toMatchObject({ context: 0, from: 25n, batchSize: 0, progressInterval: 1000, excludeNames: [] })
    expect(Object.keys(sub.body).sort()).toEqual(["batchSize", "context", "criteria", "excludeNames", "from", "progressInterval"])
    expect(sub.body.criteria).toEqual([
      { names: ["shop.Ticked"], tags: [{ key: enc.encode("orderId"), value: enc.encode("o-1") }] },
    ])
    stream.close()
  })

  it("subscribes to everything when no query is given", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const sub = await subscribeFrame(await conn())
    expect(sub.body.criteria).toEqual([])
    expect(sub.body.progressInterval).toBe(0)
    stream.close()
  })

  it("leaves how far to read ahead to the window: readAhead changes nothing on the wire", async () => {
    const { kdb, connection: conn } = await subscribed()
    const big = kronosDbEventStore(kdb).open({ position: 0n, readAhead: 5000 })
    const small = kronosDbEventStore(kdb).open({ position: 0n, readAhead: 10 })
    const fake = await conn()
    for (let i = 0; i < 200 && fake.received.filter((r) => r.frame.name === "Subscribe").length < 2; i++) await Bun.sleep(5)
    const bodies = fake.received.flatMap((r) => (r.frame.name === "Subscribe" ? [r.frame.body] : []))
    expect(bodies).toHaveLength(2)
    expect(bodies[0]).toEqual(bodies[1]!)
    big.close()
    small.close()
  })

  it("lets the host override how it subscribes", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb, "default", { progressIntervalMs: 50 }).open({ position: 0n, query: { types: ["shop.Ticked"] } })
    const sub = await subscribeFrame(await conn())
    expect(sub.body.progressInterval).toBe(50)
    stream.close()
  })

  it("delivers events in order, decoded when taken", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    let woken = 0
    stream.setCallback(() => woken++)

    expect(stream.hasNextAvailable()).toBe(false)
    fake.reply(id, 0, events(0, 3))
    await Bun.sleep(20)
    expect(woken).toBe(1)
    expect(stream.hasNextAvailable()).toBe(true)

    const taken = [stream.next(), stream.next(), stream.next()]
    expect(taken.map((t) => t!.sequence)).toEqual([0n, 1n, 2n])
    expect(taken.map((t) => (t!.event.payload as { n: number }).n)).toEqual([0, 1, 2])
    expect(stream.next()).toBeUndefined()
    expect(stream.hasNextAvailable()).toBe(false)
    expect(stream.isCompleted()).toBe(false)
    stream.close()
  })

  it("peek does not consume, and next after peek returns the same event", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    fake.reply(id, 0, events(5, 2))
    await Bun.sleep(20)

    expect(stream.peek()!.sequence).toBe(5n)
    expect(stream.peek()!.sequence).toBe(5n)
    expect(stream.hasNextAvailable()).toBe(true)
    expect(stream.next()!.sequence).toBe(5n)
    expect(stream.next()!.sequence).toBe(6n)
    expect(stream.peek()).toBeUndefined()
    stream.close()
  })

  it("holds back a slow consumer instead of failing it: nothing is granted until its events are taken", async () => {
    const { kdb, connection: conn } = await subscribed(2 * HALF)
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)

    // Four frames of about 12 KiB: more than half of the 64 KiB window, none of it taken.
    const frames = [0, 10, 20, 30].map((from) => events(from, 10, 1200))
    for (const frame of frames) fake.reply(id, 0, frame)
    await Bun.sleep(30)
    expect(stream.error()).toBeUndefined()
    expect(stream.isCompleted()).toBe(false)
    expect(windows(fake)).toEqual([])

    // Taking a frame's events gives its bytes back, once half the window has been taken.
    const sizes = frames.map(wireSize)
    expect(sizes[0]! + sizes[1]!).toBeLessThan(HALF)
    expect(sizes[0]! + sizes[1]! + sizes[2]!).toBeGreaterThanOrEqual(HALF)
    for (let i = 0; i < 20; i++) stream.next()
    await Bun.sleep(20)
    expect(windows(fake)).toEqual([])
    for (let i = 0; i < 10; i++) stream.next()
    await Bun.sleep(20)
    // It goes on the subscription's own stream, after the client's END, as a frame about it.
    expect(windows(fake)).toEqual([{ stream: id, flags: 0, bytes: sizes[0]! + sizes[1]! + sizes[2]! }])

    // The last frame is still waiting; taking it does not reach half again.
    for (let i = 0; i < 10; i++) expect(stream.next()).toBeDefined()
    await Bun.sleep(20)
    expect(windows(fake)).toHaveLength(1)
    stream.close()
  })

  it("gives a frame back only when its LAST event is taken", async () => {
    const { kdb, connection: conn } = await subscribed(2 * HALF)
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)

    // One frame of about 40 KiB: more than half the window by itself.
    const big = events(0, 10, 4000)
    fake.reply(id, 0, big)
    await Bun.sleep(20)
    expect(wireSize(big)).toBeGreaterThan(HALF)
    for (let i = 0; i < 9; i++) stream.next()
    await Bun.sleep(20)
    expect(windows(fake)).toEqual([])
    stream.next()
    await Bun.sleep(20)
    expect(windows(fake)).toEqual([{ stream: id, flags: 0, bytes: wireSize(big) }])
    stream.close()
  })

  it("a peeked event counts as taken, and a spent frame is given back without another read", async () => {
    const { kdb, connection: conn } = await subscribed(2 * HALF)
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    const big = events(0, 1, 40_000)
    fake.reply(id, 0, big)
    await Bun.sleep(20)
    // One event, one frame: peek takes it from the queue, and nothing more has to be read for the bytes to go back.
    stream.peek()
    await Bun.sleep(20)
    expect(windows(fake)).toEqual([{ stream: id, flags: 0, bytes: wireSize(big) }])
    stream.close()
  })

  it("spends no window on the frame that ends the stream", async () => {
    const { kdb, connection: conn } = await subscribed(2 * HALF)
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    fake.reply(id, FLAG_END, events(0, 10, 4000))
    await Bun.sleep(20)
    for (let i = 0; i < 10; i++) expect(stream.next()).toBeDefined()
    await Bun.sleep(20)
    expect(windows(fake)).toEqual([])
    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()).toBeUndefined()
  })

  it("gives Progress frames back as they arrive: they hold no events for anyone to take", async () => {
    const { kdb, connection: conn } = await subscribed(2 * HALF)
    const stream = kronosDbSubscription(kdb, "default", { position: 0n }, { progressIntervalMs: 1 })
    const fake = await conn()
    const sub = await subscribeFrame(fake)
    const progress = wireSize({ name: "Progress", body: { position: 1n } })
    const needed = Math.ceil(HALF / progress)
    for (let i = 0; i < needed; i++) fake.reply(sub.stream, 0, { name: "Progress", body: { position: BigInt(i) } })
    await until(() => windows(fake).length > 0)
    expect(windows(fake)).toEqual([{ stream: sub.stream, flags: 0, bytes: needed * progress }])
    // The reader never took one, and the stream lives on.
    expect(stream.error()).toBeUndefined()
    stream.close()
  })

  it("keeps the latest Progress and does not mistake it for an event", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbSubscription(kdb, "default", { position: 0n }, { progressIntervalMs: 250 })
    const fake = await conn()
    const sub = await subscribeFrame(fake)
    expect(sub.body.progressInterval).toBe(250)

    expect(stream.progress()).toBeUndefined()
    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 40n } })
    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 90n } })
    await Bun.sleep(20)
    expect(stream.progress()).toBe(90n)
    expect(stream.hasNextAvailable()).toBe(false)
    stream.close()
  })

  it("holds a Progress behind the events that arrived before it until they are taken", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbSubscription(kdb, "default", { position: 0n, query: { types: ["shop.Ticked"] } }, { progressIntervalMs: 250 })
    const fake = await conn()
    const sub = await subscribeFrame(fake)

    fake.reply(sub.stream, 0, events(0, 3))
    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 90n } })
    await Bun.sleep(20)

    // The reader has taken nothing: moving its token to 90 would skip the three.
    expect(stream.progress()).toBeUndefined()
    expect(stream.progressToken?.()).toBeUndefined()
    stream.next()
    stream.next()
    expect(stream.progress()).toBeUndefined()
    stream.next()
    expect(stream.progress()).toBe(90n)
    expect(stream.progressToken?.()?.position()).toBe(90n)
    stream.close()
  })

  it("a peeked event is not yet taken, and a later batch does not hold back an earlier Progress", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbSubscription(kdb, "default", { position: 0n }, { progressIntervalMs: 250 })
    const fake = await conn()
    const sub = await subscribeFrame(fake)

    fake.reply(sub.stream, 0, events(0, 1))
    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 1n } })
    fake.reply(sub.stream, 0, events(5, 2))
    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 7n } })
    await Bun.sleep(20)

    stream.peek()
    expect(stream.progress()).toBeUndefined()
    stream.next()
    expect(stream.progress()).toBe(1n)
    stream.next()
    stream.next()
    expect(stream.progress()).toBe(7n)
    stream.close()
  })

  it("wakes the reader when a Progress arrives, so an idle reader moves its token", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbSubscription(kdb, "default", { position: 0n }, { progressIntervalMs: 250 })
    const fake = await conn()
    const sub = await subscribeFrame(fake)
    let woken = 0
    stream.setCallback(() => woken++)

    fake.reply(sub.stream, 0, { name: "Progress", body: { position: 40n } })
    await Bun.sleep(20)

    expect(woken).toBe(1)
    expect(stream.hasNextAvailable()).toBe(false)
    expect(stream.progress()).toBe(40n)
    stream.close()
  })

  it("passes the types to skip", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbSubscription(kdb, "default", { position: 0n }, { excludeNames: ["shop.Noise"] })
    const sub = await subscribeFrame(await conn())
    expect(sub.body).toMatchObject({ excludeNames: ["shop.Noise"] })
    stream.close()
  })

  it("close() cancels the stream at the server with END, once, and drops what was waiting", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    fake.reply(id, 0, events(0, 3))
    await Bun.sleep(20)

    stream.close()
    stream.close()
    await Bun.sleep(20)
    const cancels = fake.received.filter((r) => r.frame.name === "Cancel")
    expect(cancels).toHaveLength(1)
    expect(cancels[0]!.stream).toBe(id)
    expect(cancels[0]!.flags & FLAG_END).toBe(FLAG_END)
    expect(stream.hasNextAvailable()).toBe(false)
    expect(stream.isCompleted()).toBe(true)
    expect(stream.next()).toBeUndefined()
  })

  it("closed before it connected, it never subscribes at all — or cancels at once", async () => {
    const { kdb } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    stream.close()
    await Bun.sleep(100)
    const received = server!.connections[0]?.received.map((r) => r.frame.name) ?? []
    // Either order of the race is acceptable; what matters is no live subscription is left behind.
    expect(received.length === 0 || (received[0] === "Subscribe" && received[1] === "Cancel")).toBe(true)
  })

  it("ends with the server's reason when it refuses the subscription", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    let woken = 0
    stream.setCallback(() => woken++)
    fake.reply(id, FLAG_END, { name: "RequestFailed", body: { code: ErrorCode.permission_denied, message: "needs read" } })
    await Bun.sleep(20)

    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()?.message).toBe("needs read")
    expect(woken).toBe(1)
  })

  it("drains what arrived before it ends: events first, then completion", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    fake.reply(id, 0, events(0, 2))
    fake.reply(id, FLAG_END, { name: "RequestFailed", body: { code: ErrorCode.unavailable, message: "going away" } })
    await Bun.sleep(20)

    expect(stream.isCompleted()).toBe(false)
    expect(stream.next()!.sequence).toBe(0n)
    expect(stream.next()!.sequence).toBe(1n)
    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()?.message).toBe("going away")
  })

  it("ends with a ConnectionFailedError when the connection drops", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    await subscribeFrame(fake)
    fake.destroy()
    await Bun.sleep(30)
    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()?.name).toBe("ConnectionFailedError")
  })

  it("ends with the error when the server cannot be reached at all", async () => {
    const probe = await fakeNativeServer()
    const port = probe.port
    await probe.close()
    connection = connectToKronosDb({ host: "127.0.0.1", port, componentName: "x" })
    const stream = kronosDbEventStore({ connection, serializer }).open({ position: 0n })
    await Bun.sleep(100)
    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()?.name).toBe("ConnectionFailedError")
  })

  it("ends the subscription, not the process, when the server sends bytes it cannot read", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    const fake = await conn()
    const { stream: id } = await subscribeFrame(fake)
    // A record whose payload is not JSON: taking it fails.
    const poisoned = record(0n)
    poisoned.event.payload = enc.encode("{ not json")
    fake.reply(id, 0, { name: "Events", body: { events: [poisoned] } })
    await Bun.sleep(20)

    expect(stream.hasNextAvailable()).toBe(true)
    expect(stream.next()).toBeUndefined()
    expect(stream.isCompleted()).toBe(true)
    expect(stream.error()).toBeInstanceOf(Error)
  })

  it("rejects a window the server would refuse, where the connection is configured", () => {
    for (const window of [0, 65535, 1.5, 2 ** 32]) {
      expect(() => connectToKronosDb({ componentName: "x", window })).toThrow(RangeError)
    }
  })

  it("grants an 8 MiB window in Hello unless the connection says otherwise", async () => {
    const { kdb, connection: conn } = await subscribed()
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    expect((await conn()).hello.window).toBe(8 * 1024 * 1024)
    stream.close()
  })

  it("grants the window the connection was configured with", async () => {
    const { kdb, connection: conn } = await subscribed(1 << 20)
    const stream = kronosDbEventStore(kdb).open({ position: 0n })
    expect((await conn()).hello.window).toBe(1 << 20)
    stream.close()
  })
})
