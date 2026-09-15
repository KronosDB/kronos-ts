import { afterEach, describe, expect, it } from "bun:test"
import {
  jsonSerializer,
  localCommandBus,
  localQueryBus,
  qn,
  unitOfWork,
  type CommandMessage,
  type QueryMessage,
  type MessagingLimits,
} from "@kronos-ts/core"
import { kronosDbCommandBus, kronosDbQueryBus } from "../kronosdb.js"
import { axonServerCommandBus, axonServerQueryBus } from "../../../axon-server/src/axon-server.js"
import { shutdownLatch as kronosLatch } from "../shutdown-latch.js"
import { shutdownLatch as axonLatch } from "../../../axon-server/src/shutdown-latch.js"
import { outboundStream } from "../outbound-stream.js"

function deferred<T = void>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}
const tick = () => new Promise<void>((r) => setTimeout(r, 0))
const serializer = jsonSerializer()
function message(
  name: string,
  payload: unknown = 0,
  kind: "command" | "query" = "query",
): CommandMessage | QueryMessage {
  return {
    kind,
    identifier: crypto.randomUUID(),
    name: qn("qa", name),
    payload,
    metadata: { tenant: "test" },
    timestamp: 123,
  }
}

// A credit-aware transport double: requests must travel through the adapter's
// outbound RPC and return over its inbound stream. No call reaches `next`
// without being decoded by the adapter. Real-server suites complement this.
function harness(
  backend: "kronos" | "axon",
  kind: "command" | "query",
  shortcut = false,
  codec = serializer,
  limits?: MessagingLimits,
  timeoutMs?: number,
) {
  const incoming = outboundStream<any>()
  const subscription = outboundStream<any>()
  const responses: any[] = []
  const frames: any[] = []
  const subscriptionFrames: any[] = []
  const pending = new Map<string, ReturnType<typeof deferred<any>>>()
  const queued: any[] = []
  let credits = 0n
  let streams = 0
  let completedQueries = 0
  let lastRequestId = ""
  let stallCompletion = false
  let subscriptionSignal: AbortSignal | undefined
  let latch = backend === "kronos" ? kronosLatch() : axonLatch()
  function deliver() {
    while (credits > 0n && queued.length) {
      credits--
      incoming.send({ [kind]: queued.shift() })
    }
  }
  let providerSignal: AbortSignal | undefined
  let disconnect: () => void = () => {}
  const openStream = (outbound: AsyncIterable<any>, options: { signal?: AbortSignal }) => {
    providerSignal = options.signal
    streams++
    void (async () => {
      for await (const frame of outbound) {
        frames.push(frame)
        if (frame.flowControl) {
          credits += frame.flowControl.permits
          deliver()
        }
        const response = frame.commandResponse ?? frame.queryResponse
        if (response) {
          responses.push(response)
          pending.get(response.requestIdentifier)?.resolve(response)
        }
      }
    })()
    return incoming.iterable
  }
  async function request(proto: any, options?: { signal?: AbortSignal }) {
    lastRequestId = proto.messageIdentifier
    const result = deferred<any>()
    pending.set(proto.messageIdentifier, result)
    queued.push(proto)
    deliver()
    let timer: ReturnType<typeof setTimeout> | undefined
    let onAbort: (() => void) | undefined
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => reject(new Error("RPC cancelled by client deadline"))
      if (options?.signal?.aborted) onAbort()
      else options?.signal?.addEventListener("abort", onAbort, { once: true })
    })
    try {
      return await Promise.race([
        result.promise,
        aborted,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error("wire request timed out")), 200)
        }),
      ])
    } finally {
      clearTimeout(timer)
      if (onAbort) options?.signal?.removeEventListener("abort", onAbort)
      pending.delete(proto.messageIdentifier)
    }
  }
  const connection: any = {
    config: { componentName: "qa", clientId: "qa", context: "default", token: "" },
    commands: { openStream, dispatch: request },
    queries: {
      openStream,
      async *query(proto: any, options: any) {
        yield await request(proto, options)
        if (stallCompletion)
          await new Promise<void>((_, reject) => {
            const abort = () => reject(new Error("completion cancelled"))
            if (options.signal.aborted) abort()
            else options.signal.addEventListener("abort", abort, { once: true })
          })
        completedQueries++
      },
      subscription(outbound: AsyncIterable<any>, options: any) {
        subscriptionSignal = options.signal
        void (async () => {
          for await (const frame of outbound) subscriptionFrames.push(frame)
        })()
        return subscription.iterable
      },
    },
    onReconnect() {},
    onDisconnect(callback: () => void) {
      disconnect = callback
    },
  }
  const source = {
    connection,
    serializer: codec,
    shutdown: latch,
    registerShutdownLatch(value: typeof latch) {
      latch = value
    },
  }
  const options = {
    limits,
    timeoutMs,
    flowControl: { permits: 1, refillThreshold: 0 },
    shortcutQueriesToLocalHandlers: shortcut,
  }
  const bus =
    kind === "command"
      ? backend === "kronos"
        ? kronosDbCommandBus(localCommandBus(unitOfWork), source, "default", options)
        : axonServerCommandBus(localCommandBus(unitOfWork), source, options)
      : backend === "kronos"
        ? kronosDbQueryBus(localQueryBus(unitOfWork), source, "default", options)
        : axonServerQueryBus(localQueryBus(unitOfWork), source, options)
  return {
    bus: bus as any,
    frames,
    responses,
    subscriptionFrames,
    incoming,
    subscription,
    disconnect() {
      disconnect()
    },
    get providerSignal() {
      return providerSignal
    },
    get latch() {
      return latch
    },
    get streams() {
      return streams
    },
    stallQueryCompletion() {
      stallCompletion = true
    },
    get lastRequestId() {
      return lastRequestId
    },
    get completedQueries() {
      return completedQueries
    },
    get subscriptionSignal() {
      return subscriptionSignal
    },
    dispatch(name: string, payload: unknown = 0): Promise<unknown> {
      return kind === "command"
        ? (bus as any).dispatch(message(name, payload, kind))
        : (bus as any).query(message(name, payload, kind))
    },
    remote(name: string, extra: Record<string, unknown> = {}) {
      return request({
        ...extra,
        messageIdentifier: crypto.randomUUID(),
        name: `qa.${name}`,
        query: `qa.${name}`,
        payload: serializer.serialize(1, `qa.${name}`),
        metadata: {},
        metaData: {},
        timestamp: 123n,
      })
    },
    close() {
      void latch.initiateShutdown()
      incoming.close()
      subscription.close()
    },
  }
}

for (const backend of ["kronos", "axon"] as const) {
  for (const kind of ["command", "query"] as const) {
    describe(`${backend} ${kind} transport contract`, () => {
      let h: ReturnType<typeof harness>
      afterEach(() => h?.close())
      it("aborts its provider RPC when the connection closes", async () => {
        h = harness(backend, kind)
        h.bus.subscribe("qa.Done", async () => 42)
        expect(h.providerSignal?.aborted).toBe(false)
        h.disconnect()
        expect(h.providerSignal?.aborted).toBe(true)
      })
      it("handles nesting beyond the receive window, preserving response order and fresh units", async () => {
        h = harness(backend, kind)
        const units = new Set()
        h.bus.subscribe("qa.Nested", async (m: any, uow: any) => {
          units.add(uow)
          return m.payload === 0 ? 0 : 1 + Number(await h.dispatch("Nested", m.payload - 1))
        })
        expect(await h.dispatch("Nested", 4)).toBe(4)
        expect(units.size).toBe(5)
        expect(h.responses).toHaveLength(5)
        expect(h.latch.activeCount).toBe(0)
      })
      it("rejects a nested child at saturation and releases the parent's capacity", async () => {
        const snapshots: any[] = []
        h = harness(backend, kind, false, serializer, {
          maxConcurrentHandlers: 1,
          observe: (s) => snapshots.push(s),
        })
        h.bus.subscribe("qa.Nested", async (m: any) => (m.payload ? h.dispatch("Nested", 0) : 42))
        await expect(h.dispatch("Nested", 1)).rejects.toThrow(/overloaded/)
        expect(await h.dispatch("Nested", 0)).toBe(42)
        expect(h.streams).toBe(1)
        expect(h.latch.activeCount).toBe(0)
        expect(
          snapshots.filter((s) => s.name === "inbound handlers").every((s) => s.active <= 1),
        ).toBe(true)
      })
      it("correlates a burst of out-of-order responses without exceeding handler capacity", async () => {
        const snapshots: any[] = []
        h = harness(backend, kind, false, serializer, {
          maxConcurrentHandlers: 8,
          observe: (s) => snapshots.push(s),
        })
        h.bus.subscribe("qa.Burst", async (m: any) => {
          await new Promise((r) => setTimeout(r, 8 - (m.payload % 8)))
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
        expect(
          Math.max(...snapshots.filter((s) => s.name === "inbound handlers").map((s) => s.active)),
        ).toBe(8)
        expect(h.latch.activeCount).toBe(0)
      })
      it("rejects duplicate pending identifiers without stealing the original response", async () => {
        h = harness(backend, kind)
        const entered = deferred(),
          release = deferred()
        h.bus.subscribe("qa.Slow", async () => {
          entered.resolve()
          await release.promise
          return 7
        })
        const m = message("Slow")
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
        h = harness(backend, kind, false, serializer, { maxPendingRequests: 1 })
        const entered = deferred(),
          release = deferred()
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
      it("cancels the RPC at its deadline while retaining capacity for a running handler", async () => {
        h = harness(backend, kind, false, serializer, { maxConcurrentHandlers: 1 }, 20)
        const entered = deferred(),
          release = deferred()
        h.bus.subscribe("qa.Slow", async () => {
          entered.resolve()
          await release.promise
          return 7
        })
        const first = h.dispatch("Slow")
        void first.catch(() => {})
        await entered.promise
        try {
          await expect(first).rejects.toThrow(/cancelled/)
          expect(h.latch.activeCount).toBe(1)
          await expect(h.dispatch("Slow")).rejects.toThrow(/overloaded/)
        } finally {
          release.resolve()
          await tick()
        }
        expect(h.latch.activeCount).toBe(0)
      })
      it("isolates result serialization failure and keeps the stream usable", async () => {
        h = harness(backend, kind)
        h.bus.subscribe("qa.Bad", async () => {
          const cycle: any = {}
          cycle.self = cycle
          return cycle
        })
        h.bus.subscribe("qa.Good", async () => 7)
        await expect(h.dispatch("Bad")).rejects.toThrow(/cyclic|circular/i)
        expect(await h.dispatch("Good")).toBe(7)
        expect(h.streams).toBe(1)
      })
      it("preserves serializer type and revision on both request and response", async () => {
        h = harness(backend, kind, false, {
          ...serializer,
          serialize(value, type) {
            return serializer.serialize(value, type, "v1")
          },
          deserialize<T>(value: any): T {
            expect(value.type).not.toBe("")
            expect(value.revision).toBe("v1")
            return serializer.deserialize<T>(value)
          },
        })
        h.bus.subscribe("qa.Typed", async () => 7)
        expect(await h.dispatch("Typed")).toBe(7)
      })
      it("tracks remote inbound work until completion during shutdown", async () => {
        h = harness(backend, kind)
        const entered = deferred(),
          release = deferred()
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
  describe(`${backend} query lifecycle`, () => {
    let h: ReturnType<typeof harness>
    afterEach(() => h?.close())
    if (backend === "axon")
      it("refills credits consumed by subscription acknowledgements", async () => {
        h = harness(backend, "query")
        h.bus.subscribe("qa.Credited", async () => 42)
        await tick()
        const grants = () => h.frames.filter((frame) => frame.flowControl).length
        const before = grants()
        h.incoming.send({ ack: { instructionId: "subscribed", success: true } })
        await tick()
        expect(grants()).toBe(before + 1)
      })
    if (backend === "axon")
      it("expires orphaned early credits within the bounded table", async () => {
        h = harness(backend, "query", false, serializer, { maxPendingRequests: 1 }, 15)
        h.bus.subscribe("qa.Credited", async () => 42)
        h.incoming.send({
          queryFlowControl: { queryReference: { requestId: "orphan" }, permits: 1n },
        })
        await new Promise((r) => setTimeout(r, 25))
        h.incoming.send({ queryFlowControl: { queryReference: { requestId: "new" }, permits: 1n } })
        h.incoming.send({
          query: {
            messageIdentifier: "new",
            query: "qa.Credited",
            timestamp: 0n,
            processingInstructions: [
              { key: 7, value: { booleanValue: true } },
              { key: 8, value: { booleanValue: true } },
            ],
          },
        })
        await tick()
        expect(h.responses).toHaveLength(1)
        expect(h.responses[0].requestIdentifier).toBe("new")
      })
    if (backend === "axon")
      it("retains response credits delivered before their query", async () => {
        h = harness(backend, "query")
        h.bus.subscribe("qa.Credited", async () => 42)
        h.incoming.send({
          queryFlowControl: { queryReference: { requestId: "early-credit" }, permits: 1n },
        })
        await tick()
        h.incoming.send({
          query: {
            messageIdentifier: "early-credit",
            query: "qa.Credited",
            timestamp: 0n,
            processingInstructions: [
              { key: 7, value: { booleanValue: true } },
              { key: 8, value: { booleanValue: true } },
            ],
          },
        })
        await tick()
        expect(h.responses).toHaveLength(1)
        expect(h.responses[0].requestIdentifier).toBe("early-credit")
        expect(h.responses[0].errorCode).toBe("")
        expect(h.latch.activeCount).toBe(0)
      })
    if (backend === "axon")
      it("drains an admitted reply when its response credit arrives during shutdown", async () => {
        h = harness(backend, "query")
        h.bus.subscribe("qa.Credited", async () => 42)
        const result = h.remote("Credited", {
          processingInstructions: [
            { key: 7, value: { booleanValue: true } },
            { key: 8, value: { booleanValue: true } },
          ],
        })
        await tick()
        const drain = h.latch.initiateShutdown()
        await tick()
        expect(h.latch.activeCount).toBe(1)
        h.incoming.send({
          queryFlowControl: { queryReference: { requestId: h.lastRequestId }, permits: 1n },
        })
        expect((await result).errorCode).toBe("")
        await drain
        expect(h.responses).toHaveLength(1)
        expect(h.latch.activeCount).toBe(0)
      })
    if (backend === "axon")
      it("cancellation releases a completed handler waiting for response credit", async () => {
        h = harness(backend, "query")
        h.bus.subscribe("qa.Credited", async () => 42)
        h.incoming.send({
          query: {
            messageIdentifier: "cancelled",
            query: "qa.Credited",
            timestamp: 0n,
            processingInstructions: [
              { key: 7, value: { booleanValue: true } },
              { key: 8, value: { booleanValue: true } },
            ],
          },
        })
        await tick()
        expect(h.latch.activeCount).toBe(1)
        h.incoming.send({ queryCancel: { requestId: "cancelled" } })
        await tick()
        expect(h.latch.activeCount).toBe(0)
        expect(h.responses).toHaveLength(0)
        expect(await h.dispatch("Credited")).toBe(42)
      })
    if (backend === "axon")
      it("waits for per-query response credit while continuing to receive control frames", async () => {
        h = harness(backend, "query")
        h.bus.subscribe("qa.Credited", async () => 42)
        const result = h.remote("Credited", {
          processingInstructions: [
            { key: 7, value: { booleanValue: true } },
            { key: 8, value: { booleanValue: true } },
          ],
        })
        await tick()
        expect(h.responses).toHaveLength(0)
        // The handler is complete, but its reply still awaits the response credit.
        expect(h.latch.activeCount).toBe(1)
        // Query IDs originate on the wire. Capture the delivered request in the harness.
        const id = h.lastRequestId
        h.incoming.send({ queryFlowControl: { queryReference: { requestId: id }, permits: 1n } })
        expect((await result).errorCode).toBe("")
        await tick()
        expect(h.latch.activeCount).toBe(0)
      })
    it("times out a stream whose end is lost after a response, without replaying", async () => {
      h = harness(backend, "query", false, serializer, undefined, 20)
      let calls = 0
      h.bus.subscribe("qa.Done", async () => {
        calls++
        return 42
      })
      h.stallQueryCompletion()
      const error = await h.dispatch("Done").catch((error: Error) => error)
      expect(error).toBeInstanceOf(Error)
      expect((error as Error).message).toMatch(/completion cancelled/)
      expect(calls).toBe(1)
      expect(h.completedQueries).toBe(0)
      expect(h.latch.activeCount).toBe(0)
    })
    it("consumes query completion instead of cancelling immediately after a response", async () => {
      h = harness(backend, "query")
      h.bus.subscribe("qa.Done", async () => 42)
      expect(await h.dispatch("Done")).toBe(42)
      expect(h.completedQueries).toBe(1)
    })
    it("does not retain a subscription when request serialization fails", () => {
      h = harness(backend, "query", false, {
        ...serializer,
        serialize(value, type, revision) {
          if ((value as any)?.fail) throw new Error("serialization failed")
          return serializer.serialize(value, type, revision)
        },
      })
      const m = message("Watch", { fail: true })
      expect(() => h.bus.subscriptionQuery(m)).toThrow("serialization failed")
      const sub = h.bus.subscriptionQuery({ ...m, payload: {} })
      sub.close()
      expect(h.subscriptionSignal?.aborted).toBe(true)
    })
    it("tracks a local shortcut until its handler settles", async () => {
      h = harness(backend, "query", true)
      const entered = deferred(),
        release = deferred()
      h.bus.subscribe("qa.Slow", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      const result = h.dispatch("Slow")
      await entered.promise
      try {
        expect(h.latch.activeCount).toBe(1)
      } finally {
        release.resolve()
        await result
      }
    })
    it("rejects an initial result if the subscription stream ends before one arrives", async () => {
      h = harness(backend, "query")
      const sub = h.bus.subscriptionQuery(message("Watch"))
      const settled = sub.initialResult.then(
        () => "resolved",
        (e: Error) => e.message,
      )
      h.subscription.close()
      await tick()
      expect(await Promise.race([settled, Promise.resolve("still pending")])).toMatch(
        /before.*initial/i,
      )
      expect(await sub.updates[Symbol.asyncIterator]().next()).toEqual({
        value: undefined,
        done: true,
      })
    })
    it("settles a pending initial result when the caller closes", async () => {
      h = harness(backend, "query")
      const sub = h.bus.subscriptionQuery(message("Watch"))
      const settled = sub.initialResult.then(
        () => "resolved",
        (e: Error) => e.message,
      )
      sub.close()
      await tick()
      expect(await Promise.race([settled, Promise.resolve("still pending")])).toMatch(/closed/i)
    })
    it("reports update overflow instead of silently dropping data", async () => {
      h = harness(backend, "query")
      const sub = h.bus.subscriptionQuery(message("Watch"), 1)
      h.subscription.send({ initialResult: { payload: serializer.serialize(0, "result") } })
      await sub.initialResult
      h.subscription.send({ update: { payload: serializer.serialize(1, "result") } })
      h.subscription.send({ update: { payload: serializer.serialize(2, "result") } })
      await tick()
      const iterator = sub.updates[Symbol.asyncIterator]()
      expect((await iterator.next()).value).toBe(1)
      const error = iterator.next().then(
        () => "resolved",
        (e: Error) => e.message,
      )
      await tick()
      expect(await Promise.race([error, Promise.resolve("still pending")])).toMatch(/overflow/i)
      sub.close()
    })
    it("defers updates until commit and suppresses rolled-back updates", async () => {
      h = harness(backend, "query")
      h.bus.subscribe("qa.Watch", async () => 0)
      const request = {
        subscriptionIdentifier: "sub-commit",
        queryRequest: { messageIdentifier: "initial-commit", query: "qa.Watch", timestamp: 0n },
      }
      h.incoming.send({ subscriptionQueryRequest: { subscribe: request } })
      if (backend === "axon")
        h.incoming.send({ subscriptionQueryRequest: { getInitialResult: request } })
      await tick()
      const updates = () => h.frames.filter((f) => f.subscriptionQueryResponse?.update)
      const committed = unitOfWork()
      await committed.execute(async () => {
        await h.bus.emitUpdate("qa.Watch", () => true, 1, committed)
        await tick()
        expect(updates()).toHaveLength(0)
      })
      await tick()
      expect(updates()).toHaveLength(1)
      const rolledBack = unitOfWork()
      await expect(
        rolledBack.execute(async () => {
          await h.bus.emitUpdate("qa.Watch", () => true, 2, rolledBack)
          throw new Error("rollback")
        }),
      ).rejects.toThrow("rollback")
      await tick()
      expect(updates()).toHaveLength(1)
    })
    it("fails the update stream if transport EOF follows the initial result", async () => {
      h = harness(backend, "query")
      const sub = h.bus.subscriptionQuery(message("Watch"))
      h.subscription.send({ initialResult: { payload: serializer.serialize(0, "result") } })
      await sub.initialResult
      const update = sub.updates[Symbol.asyncIterator]().next()
      h.subscription.close()
      await expect(update).rejects.toThrow(/ended unexpectedly/)
    })
    it("shutdown settles active subscriptions and refuses new registrations", async () => {
      h = harness(backend, "query")
      const sub = h.bus.subscriptionQuery(message("Watch"))
      const update = sub.updates[Symbol.asyncIterator]().next()
      void update.catch(() => {})
      await h.latch.initiateShutdown()
      await expect(sub.initialResult).rejects.toThrow(/shutdown/)
      await expect(update).rejects.toThrow(/shutdown/)
      expect(() => h.bus.subscriptionQuery(message("Watch"))).toThrow(/shutdown/)
    })
    it("processes unsubscribe while the initial handler is still awaiting work", async () => {
      h = harness(backend, "query")
      const entered = deferred(),
        release = deferred()
      h.bus.subscribe("qa.Watch", async () => {
        entered.resolve()
        await release.promise
        return 7
      })
      h.bus.subscribe("qa.Barrier", async () => 1)
      h.incoming.send({
        subscriptionQueryRequest: {
          subscribe: {
            subscriptionIdentifier: "sub-1",
            queryRequest: { messageIdentifier: "initial-1", query: "qa.Watch", timestamp: 0n },
          },
        },
      })
      if (backend === "axon")
        h.incoming.send({
          subscriptionQueryRequest: {
            getInitialResult: {
              subscriptionIdentifier: "sub-1",
              queryRequest: { messageIdentifier: "initial-1", query: "qa.Watch", timestamp: 0n },
            },
          },
        })
      await entered.promise
      h.incoming.send({
        subscriptionQueryRequest: { unsubscribe: { subscriptionIdentifier: "sub-1" } },
      })
      try {
        expect(await h.dispatch("Barrier")).toBe(1)
      } finally {
        release.resolve()
      }
      await tick()
      await h.bus.emitUpdate("qa.Watch", () => true, 9)
      await tick()
      expect(
        h.frames.filter(
          (f) => f.subscriptionQueryResponse || f.queryResponse?.requestIdentifier === "initial-1",
        ),
      ).toHaveLength(0)
      expect(h.latch.activeCount).toBe(0)
    })
  })
  it(`${backend}: concurrent shutdown callers share the same drain`, async () => {
    const latch = backend === "kronos" ? kronosLatch() : axonLatch()
    const activity = latch.registerActivity()
    const a = latch.initiateShutdown(),
      b = latch.initiateShutdown()
    const settled = a.then(() => "drained")
    activity.end()
    await b
    expect(await Promise.race([settled, Promise.resolve("still pending")])).toBe("drained")
  })
}
