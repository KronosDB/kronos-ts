import { afterEach, describe, expect, it } from "bun:test"
import {
  localFirstQueryBus, localQueryBus, payloadEquals, qn, unitOfWork,
  type QueryMessage, type SubscriptionQueryResult,
} from "@kronos-ts/core"
import { KronosDbError, KronosDbRequestError, QueryExecutionError } from "../errors.js"
import { kronosDbQueryBus } from "../kronosdb-query-bus.js"
import { FLAG_END, ProcessingKey, type Frame } from "../protocol/kronosdb-protocol.js"
import { deferred, rawSubscription, rig, serverSide, until, type Client, type Rig } from "./bus-test-kit.js"
import { fakeNativeServer } from "./fake-native-server.js"
import { jsonSerializer } from "@kronos-ts/core"
import { connectToKronosDb } from "../connection.js"

const Watch = "kronos.test.WatchValue"

function query(name: string, payload: unknown, identifier: string = crypto.randomUUID()): QueryMessage {
  const [namespace, ...rest] = name.split(".")
  return { kind: "query", identifier, name: qn(namespace!, rest.join(".")), payload, metadata: {}, timestamp: Date.now() }
}

const take = async <T>(iterator: AsyncIterator<T>): Promise<IteratorResult<T>> => iterator.next()

function bus(client: Client, options: Parameters<typeof kronosDbQueryBus>[3] = {}, name = "default") {
  return kronosDbQueryBus(localQueryBus(unitOfWork), client.host, name, options)
}

describe("kronosDbQueryBus — asking", () => {
  let r: Rig
  afterEach(() => r?.close())

  async function answering(handler: (m: QueryMessage) => Promise<unknown>) {
    const server = bus(r.another())
    server.subscribe(Watch, async (m) => handler(m))
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    return server
  }

  it("asks one handler, and says how long it will wait", async () => {
    r = await rig()
    await answering(async (m) => `answer:${(m.payload as { id: string }).id}`)
    const asking = bus(r.client, { timeoutMs: 1500 })
    expect(await asking.query(query(Watch, { id: "x" }))).toBe("answer:x")
    const [sent] = r.server.dispatchedQueries
    expect(sent!.name).toBe(Watch)
    expect(sent!.instructions.map((i) => [i.key, i.value?.kind === "number" ? Number(i.value.value) : undefined])).toEqual([
      [ProcessingKey.timeout, 1500],
      [ProcessingKey.nr_of_results, 1],
    ])
  })

  it("fails a query nobody handles with the server's own reason", async () => {
    r = await rig()
    const failure = await bus(r.client).query(query(Watch, 1)).then(() => undefined, (e) => e)
    expect(failure).toBeInstanceOf(KronosDbRequestError)
    expect(failure.status).toBe("unavailable")
    expect(failure.message).toMatch(/no handler available/)
  })

  it("surfaces a handler's failure as an error with its code", async () => {
    r = await rig()
    await answering(async () => { throw new Error("no such thing") })
    const failure = await bus(r.client).query(query(Watch, 1)).then(() => undefined, (e) => e)
    expect(failure).toBeInstanceOf(QueryExecutionError)
    expect(failure.message).toBe("no such thing")
    expect(failure.errorCode).toBe("KRONOS-5001")
  })

  it("answers undefined for a handler that answers with nothing", async () => {
    r = await rig()
    await answering(async () => undefined)
    expect(await bus(r.client).query(query(Watch, 1))).toBeUndefined()
  })

  it("takes the first of several answers and reads on to the end of the stream", async () => {
    // A server that scatters regardless of the request: the bus asked for one result and keeps the first.
    const sent: Frame[] = []
    const fake = await fakeNativeServer({
      onFrame(connection, { stream, frame }) {
        sent.push(frame)
        if (frame.name !== "DispatchQuery") return
        const result = (text: string) => ({
          messageId: "", requestId: frame.body.query.messageId, errorCode: "", metadata: [], instructions: [],
          payload: { typeName: "r", revision: "", data: new TextEncoder().encode(JSON.stringify(text)) },
        })
        connection.reply(stream, 0, { name: "QueryResponse", body: { result: result("first") } })
        connection.reply(stream, 0, { name: "QueryResponse", body: { result: result("second") } })
        setTimeout(() => connection.reply(stream, FLAG_END, { name: "QueryEnd", body: {} }), 20)
      },
    })
    const connection = connectToKronosDb({ host: "127.0.0.1", port: fake.port, componentName: "scatter" })
    try {
      const asking = kronosDbQueryBus(localQueryBus(unitOfWork), { connection, serializer: jsonSerializer(), registerShutdownLatch() {} })
      const began = Date.now()
      expect(await asking.query(query(Watch, 1))).toBe("first")
      // It did not return before the server said the rest were done.
      expect(Date.now() - began).toBeGreaterThanOrEqual(15)
    } finally {
      connection.close()
      await fake.close()
    }
  })

  it("does not replay a query whose end was lost after its answer", async () => {
    r = await rig()
    r.server.stallQueryEnd = true
    let calls = 0
    await answering(async () => { calls++; return 42 })
    const error = await bus(r.client, { timeoutMs: 30 }).query(query(Watch, 1)).then(() => undefined, (e) => e)
    expect(error).toBeInstanceOf(Error)
    expect(error.message).toMatch(/deadline/i)
    expect(calls).toBe(1)
    // The abandoned stream is cancelled.
    await until(() => r.server.connections.some((c) => c.received.some((f) => f.frame.name === "Cancel")), "the Cancel")
  })

  it("answers from a co-located handler under localFirstQueryBus, without going out", async () => {
    r = await rig()
    const local = localQueryBus(unitOfWork)
    const both = localFirstQueryBus(kronosDbQueryBus(local, r.client.host, "default"), local)
    both.subscribe(Watch, async () => "local")
    expect(await both.query(query(Watch, 1))).toBe("local")
    expect(r.server.dispatchedQueries).toHaveLength(0)
  })

  it("tells the handler to stop working on a query the caller gave up on", async () => {
    r = await rig()
    const entered = deferred()
    const handler = r.another()
    const server = bus(handler)
    server.subscribe(Watch, async () => { entered.resolve(); await new Promise(() => {}) })
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    const call = bus(r.client, { timeoutMs: 40 }).query(query(Watch, 1)).then(() => undefined, (e) => e)
    await entered.promise
    expect((await call).message).toMatch(/deadline/i)
    await until(() => handler.connection.native.current() !== undefined && r.server.connections.some((c) => c.received.some((f) => f.frame.name === "Cancel")), "the cancel")
  })
})

describe("kronosDbQueryBus — subscription queries, asking", () => {
  let r: Rig
  afterEach(() => r?.close())

  async function watching(run: (handlerBus: ReturnType<typeof bus>) => void | Promise<void> = () => {}, handlerResult: unknown = "initial") {
    const handler = r.another()
    const handlerBus = bus(handler)
    handlerBus.subscribe(Watch, async () => handlerResult)
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    await run(handlerBus)
    return handlerBus
  }

  const open = (client: Client = r.client, payload: unknown = { id: "x" }, buffer?: number) =>
    bus(client).subscriptionQuery(query(Watch, payload), buffer)

  it("delivers the initial result, then updates, then ends when the handler completes", async () => {
    r = await rig()
    const handlerBus = await watching()
    const sub = open()
    expect(await sub.initialResult).toBe("initial")
    const updates = sub.updates[Symbol.asyncIterator]()
    await until(() => r.server.subscriptions.length === 1, "the subscription")
    await handlerBus.emitUpdate(Watch, payloadEquals({ id: "x" }), "v1")
    await handlerBus.emitUpdate(Watch, payloadEquals({ id: "x" }), "v2")
    expect((await take(updates)).value).toBe("v1")
    expect((await take(updates)).value).toBe("v2")
    await handlerBus.completeSubscription(Watch)
    expect(await take(updates)).toEqual({ value: undefined, done: true })
  })

  it("asks for an initial window of credit, mirroring what the server will grant", async () => {
    r = await rig()
    await watching()
    open(r.client, { id: "a" }, 5000).close()
    open(r.client, { id: "b" }, 10).close()
    await until(() => r.server.subscriptions.length === 2, "both subscriptions")
    expect(r.server.subscriptions.map((s) => Number(s.permits))).toEqual([1024, 256])
    expect(r.server.subscriptions[0]!.query.instructions.map((i) => i.key)).toEqual([ProcessingKey.timeout, ProcessingKey.nr_of_results])
  })

  it("grants more credit as updates are taken, in batches of a quarter window", async () => {
    r = await rig()
    const handlerBus = await watching()
    const sub = open()
    await sub.initialResult
    for (let i = 0; i < 130; i++) await handlerBus.emitUpdate(Watch, () => true, i)
    const iterator = sub.updates[Symbol.asyncIterator]()
    for (let i = 0; i < 130; i++) expect((await take(iterator)).value).toBe(i)
    const credits = () => serverSide(r.server, r.client).received.flatMap((f) => (f.frame.name === "Credit" ? [f.frame.body.credits] : []))
    await until(() => credits().length === 2, "two refills")
    expect(credits()).toEqual([64, 64])
    sub.close()
  })

  it("rejects the initial result with the handler's error, and ends the subscription", async () => {
    r = await rig()
    const handler = r.another()
    const handlerBus = bus(handler)
    handlerBus.subscribe(Watch, async () => { throw new Error("cannot start") })
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    const sub = open()
    const error = await sub.initialResult.then(() => undefined, (e) => e)
    expect(error).toBeInstanceOf(KronosDbError)
    expect(error.message).toBe("cannot start")
    await expect(sub.updates[Symbol.asyncIterator]().next()).rejects.toThrow("cannot start")
  })

  it("rejects the initial result when no handler is there", async () => {
    r = await rig()
    const sub = open()
    const error = await sub.initialResult.then(() => undefined, (e) => e)
    expect(error).toBeInstanceOf(KronosDbRequestError)
    expect(error.message).toMatch(/no handler available/)
  })

  it("fails the updates with the handler's reason when it ends the subscription exceptionally", async () => {
    r = await rig()
    const handlerBus = await watching()
    const sub = open()
    await sub.initialResult
    const next = sub.updates[Symbol.asyncIterator]().next()
    await until(() => r.server.subscriptions.length === 1, "the subscription")
    await handlerBus.completeSubscriptionExceptionally(Watch, new Error("boom"))
    await expect(next).rejects.toThrow("boom")
  })

  it("cancels the stream when the caller closes, and the handler hears of it", async () => {
    r = await rig()
    const handlerBus = await watching()
    const sub = open()
    await sub.initialResult
    sub.close()
    const subscriber = serverSide(r.server, r.client)
    await until(() => subscriber.received.some((f) => f.frame.name === "Cancel" && (f.flags & FLAG_END) !== 0), "the Cancel")
    // The handler no longer has the subscription: an update reaches no one.
    await handlerBus.emitUpdate(Watch, () => true, "late")
    await new Promise((resolve) => setTimeout(resolve, 20))
    const handlerSent = r.server.handlerStreams[0]!.connection.received.filter((f) => f.frame.name === "SubscriptionQueryHandled")
    expect(handlerSent.map((f) => (f.frame.name === "SubscriptionQueryHandled" ? f.frame.body.event.kind : ""))).toEqual(["initialResult"])
  })

  it("fails the updates when the connection is lost mid-subscription", async () => {
    r = await rig()
    await watching()
    const subscriber = r.another()
    const sub = open(subscriber)
    await sub.initialResult
    const next = sub.updates[Symbol.asyncIterator]().next()
    subscriber.connection.native.current()!.close()
    await expect(next).rejects.toThrow()
  })
})

describe("kronosDbQueryBus — subscription queries, serving", () => {
  let r: Rig
  afterEach(() => r?.close())

  const rawSubscriber = (client: Client, id: string, payload: unknown, permits: bigint) =>
    rawSubscription(client, id, Watch, payload, permits)

  async function serving() {
    const handlerBus = bus(r.another())
    handlerBus.subscribe(Watch, async () => "initial")
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    return handlerBus
  }

  it("sends the initial result, and the updates only to the subscribers a filter picks", async () => {
    r = await rig()
    const handlerBus = await serving()
    const x = await rawSubscriber(r.client, "sub-x", { id: "x" }, 8n)
    const y = await rawSubscriber(r.client, "sub-y", { id: "y" }, 8n)
    await until(() => x.events.length === 1 && y.events.length === 1, "both initial results")
    expect(x.events[0]!.kind).toBe("initialResult")

    await handlerBus.emitUpdate(Watch, payloadEquals({ id: "x" }), "value-v1")
    await until(() => x.events.length === 2, "x's update")
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(x.events[1]).toEqual({ kind: "update", value: "value-v1" })
    expect(y.events).toHaveLength(1)

    // A function filter runs against what each subscriber asked.
    await handlerBus.emitUpdate(Watch, (p) => (p as { id: string }).id === "y", "only-y")
    await until(() => y.events.length === 2, "y's update")
    expect(x.events).toHaveLength(2)
  })

  it("answers a subscription for a query it does not handle with an error, not silence", async () => {
    r = await rig()
    const handlerBus = bus(r.another())
    handlerBus.subscribe(Watch, async () => "initial")
    await until(() => r.server.handlerStreams.some((h) => (h.names.has(Watch) && h.credit > 0)), "the handler")
    // Pretend the server routes a name this bus never subscribed (a stale registration).
    const stream = r.server.handlerStreams[0]!
    stream.connection.reply(stream.stream, 0, {
      name: "HandleSubscriptionQuery",
      body: {
        subscription: {
          subscriptionId: "stale", permits: 4n,
          query: { messageId: "q", name: "kronos.test.Other", timestamp: 1, metadata: [], instructions: [], clientId: "", componentName: "", payload: { typeName: "x", revision: "", data: new Uint8Array() } },
        },
      },
    })
    await until(() => stream.connection.received.some((f) => f.frame.name === "SubscriptionQueryHandled"), "the answer")
    const answered = stream.connection.received.find((f) => f.frame.name === "SubscriptionQueryHandled")!.frame
    expect(answered.name === "SubscriptionQueryHandled" && answered.body.event.kind === "initialResult" && answered.body.event.value.errorCode).toBe("KRONOS-5000")
  })

  it("holds updates the subscriber has no credit for, and sends them as credit arrives", async () => {
    r = await rig()
    const handlerBus = await serving()
    const sub = await rawSubscriber(r.client, "slow", { id: "x" }, 2n)
    await until(() => sub.events.length === 1, "the initial result")
    for (let i = 1; i <= 5; i++) await handlerBus.emitUpdate(Watch, () => true, `u${i}`)
    await until(() => sub.events.length === 3, "the first two updates")
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(sub.events.map((e) => e.value)).toEqual([undefined, "u1", "u2"])

    sub.credit(2)
    await until(() => sub.events.length === 5, "two more")
    sub.credit(10)
    await until(() => sub.events.length === 6, "the last")
    expect(sub.events.map((e) => e.value)).toEqual([undefined, "u1", "u2", "u3", "u4", "u5"])
  })

  it("ends a subscription only after the updates held for it", async () => {
    r = await rig()
    const handlerBus = await serving()
    const sub = await rawSubscriber(r.client, "closing", { id: "x" }, 1n)
    await until(() => sub.events.length === 1, "the initial result")
    await handlerBus.emitUpdate(Watch, () => true, "a")
    await handlerBus.emitUpdate(Watch, () => true, "b")
    await handlerBus.completeSubscription(Watch)
    await until(() => sub.events.length === 2, "the first update")
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(sub.events.map((e) => e.kind)).toEqual(["initialResult", "update"])
    sub.credit(5)
    expect(await sub.ended).toBeUndefined()
    expect(sub.events.map((e) => e.kind)).toEqual(["initialResult", "update", "update", "complete"])
  })

  it("fails a subscriber that never grants credit, rather than hold updates without bound", async () => {
    r = await rig()
    const handlerBus = await serving()
    const sub = await rawSubscriber(r.client, "stuck", { id: "x" }, 1n)
    await until(() => sub.events.length === 1, "the initial result")
    for (let i = 0; i < 1030; i++) await handlerBus.emitUpdate(Watch, () => true, i)
    expect(await sub.ended).toBeUndefined()
    const last = sub.events.at(-1)!
    expect(last.kind).toBe("completeExceptionally")
  })

  it("stops serving a subscriber that cancels", async () => {
    r = await rig()
    const handlerBus = await serving()
    const sub = await rawSubscriber(r.client, "going", { id: "x" }, 8n)
    await until(() => sub.events.length === 1, "the initial result")
    sub.cancel()
    await until(() => r.server.connections.some((c) => c.received.some((f) => f.frame.name === "Cancel")), "the cancel")
    await new Promise((resolve) => setTimeout(resolve, 20))
    await handlerBus.emitUpdate(Watch, () => true, "late")
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(sub.events).toHaveLength(1)
  })
})
