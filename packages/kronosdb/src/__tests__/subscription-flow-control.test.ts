/**
 * CREDIT-BASED FLOW CONTROL ON A SUBSCRIPTION QUERY.
 *
 * OpenSubscriptionQuery grants a window of permits; the server sends at most
 * that many updates and then waits for the subscriber to top it back up with
 * Credit. A client that grants a window and never refills works until exactly
 * `window` updates have flowed and then stalls with no error anywhere — the
 * failure this pins.
 */
import { afterEach, describe, expect, it } from "bun:test"
import { localQueryBus, qn, unitOfWork, type QueryMessage } from "@kronos-ts/core"
import { kronosDbQueryBus } from "../kronosdb-query-bus.js"
import { FLAG_END } from "../protocol/kronosdb-protocol.js"
import { clientOf, until, type Client } from "./bus-test-kit.js"
import { fakeNativeServer, type FakeConnection, type FakeNativeServer } from "./fake-native-server.js"

type Opened = { connection: FakeConnection; stream: number; permits: bigint; subscriptionId: string; credits: number[]; cancelled: boolean }

describe("kronosDbQueryBus — subscription query flow control", () => {
  let server: FakeNativeServer
  let client: Client
  const opened: Opened[] = []

  afterEach(async () => {
    client?.connection.close()
    await server?.close()
  })

  async function setup(bufferSize: number) {
    opened.length = 0
    server = await fakeNativeServer({
      onFrame(connection, { stream, frame }) {
        if (frame.name === "OpenSubscriptionQuery") {
          opened.push({
            connection,
            stream,
            permits: frame.body.subscription.permits,
            subscriptionId: frame.body.subscription.subscriptionId,
            credits: [],
            cancelled: false,
          })
        } else if (frame.name === "Credit") opened.find((o) => o.stream === stream)?.credits.push(frame.body.credits)
        else if (frame.name === "Cancel") {
          const o = opened.find((o) => o.stream === stream)
          if (o) o.cancelled = true
        }
      },
    })
    client = clientOf(server as never)
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), client.host)
    const result = bus.subscriptionQuery(message(), bufferSize)
    void result.initialResult.catch(() => {})
    await until(() => opened.length === 1, "the subscription")
    return { result, sent: opened[0]! }
  }

  const message = (): QueryMessage => ({
    identifier: "q-1",
    kind: "query",
    name: qn("kronos.test", "WatchValue"),
    payload: { id: "x" },
    metadata: {},
    timestamp: 1,
  })

  const update = (sent: Opened, n: number) =>
    sent.connection.reply(sent.stream, 0, {
      name: "SubscriptionQueryEvent",
      body: {
        subscriptionId: sent.subscriptionId,
        event: {
          kind: "update",
          value: {
            messageId: "", metadata: [], clientId: "", componentName: "", errorCode: "", error: undefined,
            payload: { typeName: "kronos.test.Update", revision: "", data: new TextEncoder().encode(JSON.stringify(n)) },
          },
        },
      },
    })

  const settle = () => new Promise((resolve) => setTimeout(resolve, 20))

  it("keeps wire headroom independent of a small consumer buffer", async () => {
    const { sent } = await setup(8)
    expect(sent.permits).toBe(256n)
  })

  it("never asks for more credit than the server will grant", async () => {
    // The server clamps the initial grant to [1, 1024]. Asking for 5000 and
    // then pacing refills against a 5000-wide window would let 1250 updates
    // pass before topping up — 226 of them beyond the credit the server
    // actually holds, and silently dropped. So the window we track is the one
    // the server gives.
    const { result, sent } = await setup(5000)
    expect(sent.permits).toBe(1024n)

    // …and the refill cadence follows the granted window (1024 / 4 = 256),
    // not the requested one.
    for (let i = 0; i < 255; i++) update(sent, i)
    await settle()
    expect(sent.credits).toHaveLength(0)
    update(sent, 255)
    await until(() => sent.credits.length === 1, "the refill")
    expect(sent.credits).toEqual([256])
    result.close()
  })

  it("rejects a zero-sized consumer buffer", async () => {
    await expect(setup(0)).rejects.toThrow("bufferSize must be a positive integer")
  })

  it("refills a quarter-window at a time as updates are consumed", async () => {
    const { result, sent } = await setup(256) // quarter window = 64

    for (let i = 0; i < 64; i++) update(sent, i)
    await until(() => sent.credits.length === 1, "the first refill")
    expect(sent.credits).toEqual([64])

    for (let i = 0; i < 64; i++) update(sent, i)
    await until(() => sent.credits.length === 2, "the second refill")
    expect(sent.credits).toEqual([64, 64])

    // What was granted back equals what was taken — the window never shrinks.
    expect(sent.credits.reduce((sum, n) => sum + n, 0)).toBe(128)
    result.close()
  })

  it("does not refill before a quarter-window has accrued", async () => {
    const { result, sent } = await setup(8)
    update(sent, 1)
    await settle()
    expect(sent.credits).toHaveLength(0)
    result.close()
  })

  it("sends every refill on the subscription's own stream", async () => {
    const { result, sent } = await setup(256)
    for (let i = 0; i < 64; i++) update(sent, i)
    await until(() => sent.credits.length === 1, "the refill")
    // The stream is the address: Credit carries no subscription id of its own.
    expect(sent.subscriptionId).toBeTruthy()
    result.close()
  })

  it("stops granting credit once the subscription is closed", async () => {
    const { result, sent } = await setup(4)
    result.close()
    await until(() => sent.cancelled, "the cancel")
    const after = sent.credits.length
    update(sent, 1)
    update(sent, 2)
    sent.connection.reply(sent.stream, FLAG_END, { name: "RequestFailed", body: { code: 10, message: "cancelled" } })
    await settle()
    expect(sent.credits).toHaveLength(after)
  })
})
