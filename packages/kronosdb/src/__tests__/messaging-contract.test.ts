import { afterEach, describe, expect, it } from "bun:test"
import {
  jsonSerializer,
  localCommandBus,
  localQueryBus,
  qn,
  unitOfWork,
  type CommandMessage,
  type MessagingLimits,
  type QueryMessage,
  type Serializer,
  type SubscriptionCapableQueryBus,
} from "@kronos-ts/core"
import { connectToKronosDb } from "../connection.js"
import { kronosDbCommandBus } from "../kronosdb-command-bus.js"
import { kronosDbQueryBus } from "../kronosdb-query-bus.js"
import { FLAG_END, type Frame } from "../protocol/kronosdb-protocol.js"
import { shutdownLatch } from "../shutdown-latch.js"
import { clientOf, deferred, rawSubscription, rig, tick, until, type Client, type Rig } from "./bus-test-kit.js"
import { fakeNativeServer, type FakeConnection, type FakeNativeServer } from "./fake-native-server.js"

const serializer = jsonSerializer()

function message(name: string, payload: unknown = 0, kind: "command" | "query" = "query"): CommandMessage | QueryMessage {
  return { kind, identifier: crypto.randomUUID(), name: qn("qa", name), payload, metadata: { tenant: "test" }, timestamp: 123 }
}

// Both buses cross the real wire to a hub that does what the server does where
// a bus can tell: handler streams with credit, answers routed back to the
// stream that dispatched. No call reaches `next` without being decoded by the
// adapter. The real-server suites complement this.
function harness(
  r: Rig,
  kind: "command" | "query",
  options: { codec?: Serializer; limits?: MessagingLimits; timeoutMs?: number } = {},
) {
  r.server.holdUnhandled = 1000
  const client = options.codec === undefined ? r.client : r.another({}, options.codec)
  const busOptions = {
    limits: options.limits,
    timeoutMs: options.timeoutMs,
    flowControl: { permits: 1, refillThreshold: 0 },
  }
  const bus: any =
    kind === "command"
      ? kronosDbCommandBus(localCommandBus(unitOfWork), client.host, "default", busOptions)
      : kronosDbQueryBus(localQueryBus(unitOfWork), client.host, "default", busOptions)
  const calls = {
    /** What the handler side has answered. */
    responses(): any[] {
      return r.server.connections.flatMap((c) => c.received.flatMap((f) => (f.frame.name === "CommandHandled" ? [f.frame.body.result] : f.frame.name === "QueryHandled" ? [f.frame.body.result] : [])))
    },
  }
  return {
    bus,
    client,
    responses: calls.responses,
    get latch() {
      return client.latch
    },
    dispatch(name: string, payload: unknown = 0): Promise<unknown> {
      return kind === "command" ? bus.dispatch(message(name, payload, kind)) : bus.query(message(name, payload, kind))
    },
    /** A caller on another client, so nothing of this bus's outbound side can cover for its inbound side. */
    /** A subscription opened by another client that speaks the wire itself. */
    remoteSubscription(name: string) {
      return rawSubscription(r.another(), crypto.randomUUID(), name, {}, 8n)
    },
    remote(name: string, payload: unknown = 1): Promise<unknown> {
      const other = r.another()
      const remoteBus: any =
        kind === "command"
          ? kronosDbCommandBus(localCommandBus(unitOfWork), other.host)
          : kronosDbQueryBus(localQueryBus(unitOfWork), other.host)
      return kind === "command" ? remoteBus.dispatch(message(name, payload, kind)) : remoteBus.query(message(name, payload, kind))
    },
  }
}

for (const kind of ["command", "query"] as const) {
  describe(`kronos ${kind} transport contract`, () => {
    let r: Rig
    afterEach(() => r?.close())

    it("ends its provider stream when the connection is reset", async () => {
      r = await rig()
      const h = harness(r, kind)
      h.bus.subscribe("qa.Done", async () => 42)
      await until(() => r.server.handlerStreams[0]?.names.size === 1, "the handler stream")
      expect(r.server.handlerStreams[0]!.ended).toBe(false)
      await h.client.connection.reconnect()
      await until(() => r.server.handlerStreams[0]!.ended, "the old stream to end")
    })

    it("handles nesting beyond the receive window, preserving response order and fresh units", async () => {
      r = await rig()
      const h = harness(r, kind)
      const units = new Set()
      h.bus.subscribe("qa.Nested", async (m: any, uow: any) => {
        units.add(uow)
        return m.payload === 0 ? 0 : 1 + Number(await h.dispatch("Nested", m.payload - 1))
      })
      expect(await h.dispatch("Nested", 4)).toBe(4)
      expect(units.size).toBe(5)
      expect(h.responses()).toHaveLength(5)
      expect(h.latch.activeCount).toBe(0)
    })

    it("rejects a nested child at saturation and releases the parent's capacity", async () => {
      r = await rig()
      const snapshots: any[] = []
      const h = harness(r, kind, { limits: { maxConcurrentHandlers: 1, observe: (s) => snapshots.push(s) } })
      h.bus.subscribe("qa.Nested", async (m: any) => (m.payload ? h.dispatch("Nested", 0) : 42))
      await expect(h.dispatch("Nested", 1)).rejects.toThrow(/overloaded/)
      expect(await h.dispatch("Nested", 0)).toBe(42)
      expect(r.server.handlerStreams).toHaveLength(1)
      expect(h.latch.activeCount).toBe(0)
      expect(snapshots.filter((s) => s.name === "inbound handlers").every((s) => s.active <= 1)).toBe(true)
    })

    it("correlates a burst of out-of-order responses without exceeding handler capacity", async () => {
      r = await rig()
      const snapshots: any[] = []
      const h = harness(r, kind, { limits: { maxConcurrentHandlers: 8, observe: (s) => snapshots.push(s) } })
      h.bus.subscribe("qa.Burst", async (m: any) => {
        await new Promise((resolve) => setTimeout(resolve, 8 - (m.payload % 8)))
        return m.payload
      })
      const results = await Promise.all(
        Array.from({ length: 128 }, async (_, index) => {
          try {
            expect(await h.dispatch("Burst", index)).toBe(index)
            return "ok"
          } catch (error) {
            expect(String(error)).toContain("overloaded")
            return "overloaded"
          }
        }),
      )
      expect(results).toContain("ok")
      expect(results).toContain("overloaded")
      expect(Math.max(...snapshots.filter((s) => s.name === "inbound handlers").map((s) => s.active))).toBe(8)
      expect(h.latch.activeCount).toBe(0)
    })

    it("rejects duplicate pending identifiers without stealing the original response", async () => {
      r = await rig()
      const h = harness(r, kind)
      const entered = deferred(), release = deferred()
      h.bus.subscribe("qa.Slow", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      const m = message("Slow", 0, kind)
      const dispatch = () => (kind === "command" ? h.bus.dispatch(m) : h.bus.query(m))
      const first = dispatch()
      await entered.promise
      try {
        await expect(dispatch()).rejects.toThrow(/already pending/)
      } finally {
        release.resolve()
      }
      expect(await first).toBe(7)
    })

    it("bounds pending callers independently of handler slots", async () => {
      r = await rig()
      const h = harness(r, kind, { limits: { maxPendingRequests: 1 } })
      const entered = deferred(), release = deferred()
      h.bus.subscribe("qa.Slow", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      const first = h.dispatch("Slow")
      await entered.promise
      try {
        await expect(h.dispatch("Slow")).rejects.toThrow(/pending requests overloaded/)
      } finally {
        release.resolve()
        await first
      }
      expect(h.latch.activeCount).toBe(0)
    })

    it("cancels the request at its deadline while retaining capacity for a running handler", async () => {
      r = await rig()
      const h = harness(r, kind, { limits: { maxConcurrentHandlers: 1 }, timeoutMs: 20 })
      const entered = deferred(), release = deferred()
      h.bus.subscribe("qa.Slow", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      const first = h.dispatch("Slow")
      void first.catch(() => {})
      await entered.promise
      try {
        await expect(first).rejects.toThrow(/deadline/)
        expect(h.latch.activeCount).toBe(1)
        await expect(h.dispatch("Slow")).rejects.toThrow(/overloaded/)
      } finally {
        release.resolve()
        await tick()
      }
      expect(h.latch.activeCount).toBe(0)
    })

    it("isolates result serialization failure and keeps the stream usable", async () => {
      r = await rig()
      const h = harness(r, kind)
      h.bus.subscribe("qa.Bad", async () => {
        const cycle: any = {}
        cycle.self = cycle
        return cycle
      })
      h.bus.subscribe("qa.Good", async () => 7)
      await expect(h.dispatch("Bad")).rejects.toThrow(/cyclic|circular/i)
      expect(await h.dispatch("Good")).toBe(7)
      expect(r.server.handlerStreams).toHaveLength(1)
    })

    it("preserves serializer type and revision on both request and response", async () => {
      r = await rig()
      const h = harness(r, kind, {
        codec: {
          ...serializer,
          serialize(value, type) {
            return serializer.serialize(value, type, "v1")
          },
          deserialize<T>(value: any): T {
            expect(value.type).not.toBe("")
            expect(value.revision).toBe("v1")
            return serializer.deserialize<T>(value)
          },
        },
      })
      h.bus.subscribe("qa.Typed", async () => 7)
      expect(await h.dispatch("Typed")).toBe(7)
    })

    it("tracks remote inbound work until completion during shutdown", async () => {
      r = await rig()
      const h = harness(r, kind)
      const entered = deferred(), release = deferred()
      h.bus.subscribe("qa.Slow", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      const result = h.remote("Slow")
      await entered.promise
      let drained = false
      const drain = h.latch.initiateShutdown().then(() => {
        drained = true
      })
      try {
        await tick()
        expect(h.latch.activeCount).toBe(1)
        expect(drained).toBe(false)
      } finally {
        release.resolve()
        await result
        await drain
      }
    })
  })
}

describe("kronos query lifecycle", () => {
  let r: Rig
  afterEach(() => r?.close())

  function queries(options: Parameters<typeof harness>[2] = {}) {
    const h = harness(r, "query", options)
    return Object.assign(h, { bus: h.bus as SubscriptionCapableQueryBus })
  }

  it("times out a stream whose end is lost after a response, without replaying", async () => {
    r = await rig()
    const h = queries({ timeoutMs: 20 })
    let calls = 0
    h.bus.subscribe("qa.Done", async () => {
      calls++
      return 42
    })
    r.server.stallQueryEnd = true
    const error = await h.dispatch("Done").catch((error: Error) => error)
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toMatch(/deadline/)
    expect(calls).toBe(1)
    expect(h.latch.activeCount).toBe(0)
  })

  it("consumes the end of the stream instead of cancelling right after a response", async () => {
    r = await rig()
    const h = queries()
    h.bus.subscribe("qa.Done", async () => 42)
    expect(await h.dispatch("Done")).toBe(42)
    await tick()
    expect(r.server.connections.some((c) => c.received.some((f) => f.frame.name === "Cancel"))).toBe(false)
  })

  it("does not retain a subscription when request serialization fails", async () => {
    r = await rig()
    const h = queries({
      codec: {
        ...serializer,
        serialize(value, type, revision) {
          if ((value as any)?.fail) throw new Error("serialization failed")
          return serializer.serialize(value, type, revision)
        },
      },
    })
    const m = message("Watch", { fail: true })
    expect(() => h.bus.subscriptionQuery(m as QueryMessage)).toThrow("serialization failed")
    const sub = h.bus.subscriptionQuery({ ...m, payload: {} } as QueryMessage)
    sub.close()
    await until(() => r.server.connections.some((c) => c.received.some((f) => f.frame.name === "Cancel")), "the Cancel")
    expect(r.server.subscriptions).toHaveLength(1)
  })

  it("shutdown settles active subscriptions and refuses new registrations", async () => {
    r = await rig()
    const h = queries()
    const sub = h.bus.subscriptionQuery(message("Watch") as QueryMessage)
    const update = sub.updates[Symbol.asyncIterator]().next()
    void update.catch(() => {})
    void sub.initialResult.catch(() => {})
    await h.latch.initiateShutdown()
    await expect(sub.initialResult).rejects.toThrow(/shutdown/)
    await expect(update).rejects.toThrow(/shutdown/)
    expect(() => h.bus.subscriptionQuery(message("Watch") as QueryMessage)).toThrow(/shutdown/)
  })

  it("processes an unsubscribe while the initial handler is still awaiting work", async () => {
    r = await rig()
    const h = queries()
    const entered = deferred(), release = deferred()
    h.bus.subscribe("qa.Watch", async () => {
      entered.resolve()
      await release.promise
      return 7
    })
    h.bus.subscribe("qa.Barrier", async () => 1)
    const sub = await h.remoteSubscription("qa.Watch")
    await entered.promise
    sub.cancel()
    try {
      expect(await h.dispatch("Barrier")).toBe(1)
    } finally {
      release.resolve()
    }
    await tick()
    await h.bus.emitUpdate("qa.Watch", () => true, 9)
    await tick()
    // The handler answered neither the initial result nor an update.
    const handlerSent = r.server.handlerStreams[0]!.connection.received.filter((f) => f.frame.name === "SubscriptionQueryHandled")
    expect(handlerSent).toHaveLength(0)
    expect(h.latch.activeCount).toBe(0)
  })

  it("defers updates until commit and suppresses rolled-back updates", async () => {
    r = await rig()
    const h = queries()
    h.bus.subscribe("qa.Watch", async () => 0)
    const sub = await h.remoteSubscription("qa.Watch")
    await until(() => sub.events.length === 1, "the initial result")
    const committed = unitOfWork()
    await committed.execute(async () => {
      await h.bus.emitUpdate("qa.Watch", () => true, 1, committed)
      await tick()
      expect(sub.events).toHaveLength(1)
    })
    await until(() => sub.events.length === 2, "the committed update")
    const rolledBack = unitOfWork()
    await expect(
      rolledBack.execute(async () => {
        await h.bus.emitUpdate("qa.Watch", () => true, 2, rolledBack)
        throw new Error("rollback")
      }),
    ).rejects.toThrow("rollback")
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(sub.events).toHaveLength(2)
  })
})

// A server that stays silent on a subscription stream until the test speaks:
// the subscriber's reactions to what the stream does.
describe("kronos subscription query — the subscriber's stream", () => {
  let fake: FakeNativeServer
  let client: Client
  const subscriptions: Array<{ connection: FakeConnection; stream: number; credits: number[]; cancelled: boolean }> = []

  async function setup() {
    subscriptions.length = 0
    fake = await fakeNativeServer({
      onFrame(connection, { stream, frame }) {
        if (frame.name === "OpenSubscriptionQuery") subscriptions.push({ connection, stream, credits: [], cancelled: false })
        else if (frame.name === "Credit") subscriptions.find((s) => s.stream === stream)?.credits.push(frame.body.credits)
        else if (frame.name === "Cancel") {
          const s = subscriptions.find((s) => s.stream === stream)
          if (s) s.cancelled = true
        }
      },
    })
    client = clientOf(fake as never)
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), client.host) as SubscriptionCapableQueryBus
    return bus
  }

  afterEach(async () => {
    client?.connection.close()
    await fake?.close()
  })

  const event = (connection: FakeConnection, stream: number, flags: number, what: Extract<Frame, { name: "SubscriptionQueryEvent" }>["body"]["event"]) =>
    connection.reply(stream, flags, { name: "SubscriptionQueryEvent", body: { subscriptionId: "s", event: what } })
  const result = (value: unknown) => ({
    messageId: "", requestId: "q", errorCode: "", metadata: [], instructions: [],
    payload: { typeName: "result", revision: "", data: new TextEncoder().encode(JSON.stringify(value)) },
  })
  const update = (value: unknown) => ({
    messageId: "", metadata: [], clientId: "", componentName: "", errorCode: "", error: undefined,
    payload: { typeName: "result", revision: "", data: new TextEncoder().encode(JSON.stringify(value)) },
  })

  it("rejects an initial result if the stream ends before one arrives", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage)
    const settled = sub.initialResult.then(() => "resolved", (e: Error) => e.message)
    await until(() => subscriptions.length === 1, "the open")
    const [s] = subscriptions
    s!.connection.reply(s!.stream, FLAG_END, { name: "RequestFailed", body: { code: 1, message: "gone" } })
    expect(await settled).toMatch(/gone/)
    expect(await sub.updates[Symbol.asyncIterator]().next().then((v) => v, (e: Error) => e.message)).toMatch(/gone/)
  })

  it("settles a pending initial result when the caller closes", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage)
    const settled = sub.initialResult.then(() => "resolved", (e: Error) => e.message)
    sub.close()
    expect(await settled).toMatch(/closed/i)
    await until(() => subscriptions[0]?.cancelled === true, "the cancel")
  })

  it("reports update overflow instead of silently dropping data", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage, 1)
    await until(() => subscriptions.length === 1, "the open")
    const [s] = subscriptions
    event(s!.connection, s!.stream, 0, { kind: "initialResult", value: result(0) })
    await sub.initialResult
    event(s!.connection, s!.stream, 0, { kind: "update", value: update(1) })
    event(s!.connection, s!.stream, 0, { kind: "update", value: update(2) })
    await tick()
    await until(() => s!.cancelled, "the subscription to be given up")
    const iterator = sub.updates[Symbol.asyncIterator]()
    expect((await iterator.next()).value).toBe(1)
    await expect(iterator.next()).rejects.toThrow(/overflow/i)
    sub.close()
  })

  it("fails the update stream if the stream ends after the initial result without a completion", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage)
    await until(() => subscriptions.length === 1, "the open")
    const [s] = subscriptions
    event(s!.connection, s!.stream, 0, { kind: "initialResult", value: result(0) })
    await sub.initialResult
    const next = sub.updates[Symbol.asyncIterator]().next()
    event(s!.connection, s!.stream, FLAG_END, { kind: "update", value: update(1) })
    // The update arrives, then the stream is over without saying so.
    expect((await next).value).toBe(1)
    await expect(sub.updates[Symbol.asyncIterator]().next()).rejects.toThrow(/ended unexpectedly/)
  })

  it("fails the update stream if the connection is lost after the initial result", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage)
    await until(() => subscriptions.length === 1, "the open")
    const [s] = subscriptions
    event(s!.connection, s!.stream, 0, { kind: "initialResult", value: result(0) })
    await sub.initialResult
    const next = sub.updates[Symbol.asyncIterator]().next()
    s!.connection.destroy()
    await expect(next).rejects.toThrow()
  })

  it("times out an initial result that never comes", async () => {
    const bus = await setup()
    const timed = kronosDbQueryBus(localQueryBus(unitOfWork), client.host, "default", { timeoutMs: 30 }) as SubscriptionCapableQueryBus
    const sub = timed.subscriptionQuery(message("Watch") as QueryMessage)
    await expect(sub.initialResult).rejects.toThrow(/timed out/)
    await until(() => subscriptions[0]?.cancelled === true, "the cancel")
    void bus
  })

  it("settles a subscription that was shut down before its stream opened", async () => {
    const bus = await setup()
    const sub = bus.subscriptionQuery(message("Watch") as QueryMessage)
    sub.close()
    await expect(sub.initialResult).rejects.toThrow(/closed/)
    await until(() => subscriptions[0]?.cancelled === true, "the cancel of a stream opened after it was abandoned")
  })
})

it("concurrent shutdown callers share the same drain", async () => {
  const latch = shutdownLatch()
  const activity = latch.registerActivity()
  const a = latch.initiateShutdown(),
    b = latch.initiateShutdown()
  const settled = a.then(() => "drained")
  activity.end()
  await b
  expect(await Promise.race([settled, Promise.resolve("still pending")])).toBe("drained")
})
