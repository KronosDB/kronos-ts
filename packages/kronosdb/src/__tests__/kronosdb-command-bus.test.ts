import { afterEach, describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  command, commandHandler, inMemoryEventStore, jsonSerializer, kronos,
  localCommandBus, localQueryBus, qn, send, unitOfWork, type CommandMessage, type UnitOfWork,
} from "@kronos-ts/core"
import { CommandExecutionError, KronosDbError, KronosDbRequestError } from "../errors.js"
import { kronosDbCommandBus, type KronosDbCommandBusOptions } from "../kronosdb-command-bus.js"
import { FLAG_END, type CommandResult } from "../protocol/kronosdb-protocol.js"
import { deferred, rig, tick, until, type Client, type Rig } from "./bus-test-kit.js"

const Parent = command({ name: qn("test", "Parent"), payload: z.number(), result: z.number() })
const Child = command({ name: qn("test", "Child"), payload: z.number(), result: z.number() })

function wire(r: Rig, options: KronosDbCommandBusOptions = {}, client: Client = r.client) {
  r.server.holdUnhandled = 1000
  const bus = kronosDbCommandBus(localCommandBus(unitOfWork), client.host, "default", options)
  return {
    bus,
    get latch() { return client.latch },
    /** What the handler side answered, in order. */
    responses(): CommandResult[] {
      return client.connection.native.current() === undefined
        ? []
        : r.server.connections.flatMap((c) => c.received.flatMap((f) => (f.frame.name === "CommandHandled" ? [f.frame.body.result] : [])))
    },
    /** A caller on another client: no outbound activity of this bus can cover for a missing inbound one. */
    remote(name: string, value: number, bus = "default") {
      const other = r.another()
      const remoteBus = kronosDbCommandBus(localCommandBus(unitOfWork), other.host, bus)
      const message: CommandMessage = {
        kind: "command", identifier: crypto.randomUUID(), name: qn(...(name.split(".") as [string, string])),
        payload: value, metadata: {}, timestamp: Date.now(),
      }
      return remoteBus.dispatch(message)
    },
  }
}

describe("kronosDbCommandBus — concurrent inbound wire handling", () => {
  let r: Rig
  afterEach(() => r?.close())

  it("cancels a wire dispatch at the client deadline while still tracking its handler", async () => {
    r = await rig()
    const w = wire(r, { timeoutMs: 30, limits: { maxConcurrentHandlers: 1 } })
    const entered = deferred(), release = deferred()
    w.bus.subscribe("test.Parent", async () => { entered.resolve(); await release.promise; return 42 })
    const started = Date.now()
    const result = send(w.bus, Parent, 1)
    void result.catch(() => {})
    await entered.promise
    try {
      await expect(result).rejects.toThrow(/deadline/i)
      expect(Date.now() - started).toBeLessThan(500)
      expect(w.latch.activeCount).toBe(1)
      // The abandoned request is cancelled on its own stream, and the cancel ends it.
      const dispatchStream = r.server.connections[0]!.received.find((f) => f.frame.name === "DispatchCommand")!.stream
      await until(() => r.server.connections[0]!.received.some((f) => f.frame.name === "Cancel" && f.stream === dispatchStream && (f.flags & FLAG_END) !== 0), "the Cancel")
    } finally {
      release.resolve()
      await w.latch.initiateShutdown()
    }
  })

  it("lets A await ctx.send(B) on the same connection with independent units of work", async () => {
    r = await rig()
    const w = wire(r, { flowControl: { permits: 1, refillThreshold: 0 } })
    const units: UnitOfWork[] = []
    const commits: string[] = []
    const parent = commandHandler(Parent, async ({ payload }, ctx) => {
      units.push(ctx.unitOfWork)
      ctx.unitOfWork.onCommit(() => { commits.push("parent") })
      return await ctx.send(Child, payload)
    })
    const child = commandHandler(Child, async ({ payload }, ctx) => {
      units.push(ctx.unitOfWork)
      ctx.unitOfWork.onCommit(() => { commits.push("child") })
      return payload * 2
    })
    const app = kronos({ commandHandlers: [parent, child].map((handler) => ({
      ...handler, commandBus: w.bus, queryBus: localQueryBus(unitOfWork), eventStore: inMemoryEventStore(),
    })) })
    try {
      expect(await send(w.bus, Parent, 21)).toBe(42)
      expect(r.server.handlerStreams).toHaveLength(1)
      expect(r.server.dispatchedCommands.map((c) => c.name)).toEqual(["test.Parent", "test.Child"])
      expect(w.responses().map((x) => x.requestId)).toEqual(r.server.dispatchedCommands.map((c) => c.messageId).reverse())
      expect(units).toHaveLength(2)
      expect(units[0]).not.toBe(units[1])
      expect(commits).toEqual(["child", "parent"])
      expect(w.latch.activeCount).toBe(0)
    } finally { await app.stop() }
  })

  it("refills receive credits while parents wait, including nesting deeper than the permit window", async () => {
    r = await rig()
    const w = wire(r, { flowControl: { permits: 1, refillThreshold: 0 } })
    w.bus.subscribe("test.Parent", async (message) => {
      const depth = message.payload as number
      return depth === 0 ? 0 : 1 + Number(await send(w.bus, Parent, depth - 1))
    })
    expect(await send(w.bus, Parent, 8)).toBe(8)
    expect(r.server.dispatchedCommands).toHaveLength(9)
    expect(w.responses()).toHaveLength(9)
    const credits = r.server.handlerStreams[0]!.credits
    expect(credits.length).toBeGreaterThanOrEqual(9)
    expect(credits.every((grant) => grant === 1)).toBe(true)
  })

  it("grants one window per stream, shared by every command name it handles", async () => {
    r = await rig()
    const w = wire(r, { flowControl: { permits: 4, refillThreshold: 1 } })
    w.bus.subscribe("test.Parent", async () => 1)
    await until(() => r.server.handlerStreams[0]?.credits.length === 1, "the first grant")
    w.bus.subscribe("test.Child", async () => 2)
    await until(() => r.server.handlerStreams[0]?.names.size === 2, "both names")
    const stream = r.server.handlerStreams[0]!
    // A second name needs no grant of its own: the window is the stream's.
    expect(stream.credits).toEqual([4])
    expect(stream.credit).toBe(4)

    // Commands of either name spend the one window, which is refilled when
    // it runs low.
    expect(await send(w.bus, Parent, 0)).toBe(1)
    expect(await send(w.bus, Child, 1)).toBe(2)
    expect(await send(w.bus, Child, 2)).toBe(2)
    await until(() => stream.credits.length === 2, "the refill")
    expect(stream.credits).toEqual([4, 4])
    expect(stream.credit).toBe(5)
  })

  it("propagates child failures to the parent and keeps unrelated work running", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Parent", async ({ payload }) => await send(w.bus, Child, payload as number))
    w.bus.subscribe("test.Child", async ({ payload }) => {
      if (payload === 0) throw new Error("child failed")
      return payload
    })
    const results = await Promise.allSettled([send(w.bus, Parent, 0), send(w.bus, Parent, 7)])
    expect(results[0].status).toBe("rejected")
    if (results[0].status === "rejected") expect(results[0].reason.message).toBe("child failed")
    expect(results[1]).toEqual({ status: "fulfilled", value: 7 })
    expect(w.responses().filter((x) => x.errorCode === "KRONOS-4002")).toHaveLength(2)
    expect(r.server.handlerStreams).toHaveLength(1)
    expect(w.latch.activeCount).toBe(0)
  })

  it("isolates response serialization failures without restarting the stream", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Parent", async () => { const value: any = {}; value.self = value; return value })
    w.bus.subscribe("test.Child", async ({ payload }) => payload)
    await expect(send(w.bus, Parent, 0)).rejects.toThrow()
    expect(await send(w.bus, Child, 7)).toBe(7)
    expect(w.responses()[0]!.errorCode).toBe("KRONOS-4002")
    expect(r.server.handlerStreams).toHaveLength(1)
    expect(w.latch.activeCount).toBe(0)
  })

  it("answers with the failure when the response is too big for a frame, instead of leaving the caller to time out", async () => {
    r = await rig({ server: { maxFrame: 4096 } })
    const w = wire(r)
    w.bus.subscribe("test.Parent", async () => "x".repeat(10_000))
    w.bus.subscribe("test.Child", async ({ payload }) => payload)
    const failure = await send(w.bus, Parent, 0).then(() => undefined, (e) => e)
    expect(failure).toBeInstanceOf(CommandExecutionError)
    expect(failure.message).toMatch(/exceeds the server's limit of 4096/)
    expect(await send(w.bus, Child, 3)).toBe(3)
    expect(r.server.handlerStreams).toHaveLength(1)
  })

  it("drains remote inbound handlers and rejects new work during shutdown", async () => {
    r = await rig()
    const w = wire(r)
    const entered = deferred()
    const release = deferred()
    let calls = 0
    w.bus.subscribe("test.Parent", async () => {
      calls++
      entered.resolve()
      await release.promise
      return 42
    })
    const result = w.remote("test.Parent", 0)
    await entered.promise
    expect(w.latch.activeCount).toBe(1)
    let drained = false
    const drain = w.latch.initiateShutdown().then(() => { drained = true })
    try {
      await expect(w.remote("test.Parent", 1)).rejects.toThrow("Shutdown in progress")
      expect(calls).toBe(1)
      expect(drained).toBe(false)
    } finally { release.resolve() }
    await drain
    expect(await result).toBe(42)
    expect(w.latch.activeCount).toBe(0)
    expect(r.server.handlerStreams.filter((h) => h.clientId === r.client.connection.config.clientId)).toHaveLength(1)
    await expect(send(w.bus, Parent, 0)).rejects.toThrow("Shutdown in progress")
  })

  it("drains a running parent and child, including the child's outbound dispatch", async () => {
    r = await rig()
    const w = wire(r)
    const entered = deferred()
    const release = deferred()
    w.bus.subscribe("test.Parent", async ({ payload }) => await send(w.bus, Child, payload as number))
    w.bus.subscribe("test.Child", async ({ payload }) => {
      entered.resolve()
      await release.promise
      return payload
    })
    const result = w.remote("test.Parent", 42)
    await entered.promise
    let drained = false
    const drain = w.latch.initiateShutdown().then(() => { drained = true })
    try {
      // Remote parent inbound + child outbound + child inbound.
      expect(w.latch.activeCount).toBe(3)
      await Promise.resolve()
      expect(drained).toBe(false)
    } finally { release.resolve() }
    await drain
    expect(await result).toBe(42)
    expect(w.latch.activeCount).toBe(0)
    expect(w.responses().map((x) => x.requestId)).toEqual(r.server.dispatchedCommands.map((c) => c.messageId).reverse())
  })
})

describe("kronosDbCommandBus — what goes on the wire", () => {
  let r: Rig
  afterEach(() => r?.close())

  it("binds the bus by name and addresses every request by its id", async () => {
    r = await rig()
    const w = wire(r)
    const orders = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host, "orders")
    orders.subscribe("test.Child", async ({ payload }) => payload)
    await until(() => r.server.handlerStreams.length === 1, "the handler stream")
    expect(await orders.dispatch({ kind: "command", identifier: "o-1", name: qn("test", "Child"), payload: 5, metadata: {}, timestamp: 1 })).toBe(5)
    const received = r.server.connections[0]!.received
    const bind = received.find((f) => f.frame.name === "BindBus")!.frame
    expect(bind.name === "BindBus" && bind.body).toEqual({ id: 1, name: "orders" })
    expect(r.server.handlerStreams[0]!.bus).toBe("orders")
    // Neither a context nor the default bus leaks onto it.
    expect(received.some((f) => f.frame.name === "BindContext")).toBe(false)
    expect(w.bus).toBeDefined()
  })

  it("uses the default bus when none is named, without binding anything", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Child", async () => 1)
    await send(w.bus, Child, 1)
    expect(r.server.connections[0]!.received.some((f) => f.frame.name === "BindBus")).toBe(false)
    expect(r.server.handlerStreams[0]!.bus).toBe("default")
  })

  it("sends the routing key, priority and timeout as processing instructions, and nothing when there are none", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Child", async () => 1)
    await w.bus.dispatch({ kind: "command", identifier: "k-1", name: qn("test", "Child"), payload: 1, metadata: { processingInstructions: { routingKey: "order-7", priority: 3, timeoutMs: 900 } }, timestamp: 1 })
    await w.bus.dispatch({ kind: "command", identifier: "k-2", name: qn("test", "Child"), payload: 1, metadata: {}, timestamp: 1 })
    const [keyed, plain] = r.server.dispatchedCommands
    expect(keyed!.instructions.map((i) => [i.key, i.value?.kind === "number" ? Number(i.value.value) : (i.value as { value: string }).value])).toEqual([[0, "order-7"], [1, 3], [2, 900]])
    expect(plain!.instructions).toEqual([])
    expect(keyed!.clientId).toBe(r.client.connection.config.clientId)
    expect(keyed!.componentName).toBe("bus-test")
  })

  it("stamps a message that has no instant yet, and keeps one that has", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Child", async () => 1)
    const before = Date.now()
    await w.bus.dispatch({ kind: "command", identifier: "t-1", name: qn("test", "Child"), payload: 1, metadata: {} } as CommandMessage)
    await w.bus.dispatch({ kind: "command", identifier: "t-2", name: qn("test", "Child"), payload: 1, metadata: {}, timestamp: 123 })
    const [stamped, kept] = r.server.dispatchedCommands
    expect(stamped!.timestamp).toBeGreaterThanOrEqual(before)
    expect(kept!.timestamp).toBe(123)
  })

  it("fails a command nobody handles with the server's own reason", async () => {
    r = await rig()
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host)
    const failure = await send(bus, Child, 1).then(() => undefined, (e) => e)
    expect(failure).toBeInstanceOf(KronosDbRequestError)
    expect(failure.status).toBe("unavailable")
    expect(failure.transient).toBe(true)
    expect(failure.message).toMatch(/no handler available/)
  })

  it("surfaces the server's in-band timeout as an error with its code", async () => {
    r = await rig({ server: { commandTimeoutMs: 20 } })
    const w = wire(r)
    w.bus.subscribe("test.Parent", async () => { await new Promise(() => {}) })
    const timedOut = await send(w.bus, Parent, 1).then(() => undefined, (e) => e)
    expect(timedOut).toBeInstanceOf(KronosDbError)
    expect(timedOut.errorCode).toBe("KRONOSDB-4005")
    expect(timedOut.transient).toBe(false)
  })

  it("surfaces the loss of the handler's connection as an error with the server's code", async () => {
    r = await rig()
    const w = wire(r)
    const other = r.another()
    const handlerBus = kronosDbCommandBus(localCommandBus(unitOfWork), other.host)
    const received = deferred()
    handlerBus.subscribe("test.Child", async () => { received.resolve(); await new Promise(() => {}) })
    await until(() => r.server.handlerStreams.some((h) => h.names.has("test.Child") && h.credit > 0), "the handler")
    const call = send(w.bus, Child, 1).then(() => undefined, (e) => e)
    await received.promise
    other.connection.close()
    const lost = await call
    expect(lost).toBeInstanceOf(KronosDbError)
    expect(lost.errorCode).toBe("KRONOSDB-4006")
  })

  it("keeps the handler's own error code and message, as an error with that code", async () => {
    r = await rig()
    const w = wire(r)
    const other = r.another()
    const handlerBus = kronosDbCommandBus(localCommandBus(unitOfWork), other.host)
    handlerBus.subscribe("test.Child", async () => { throw new Error("no stock") })
    await until(() => r.server.handlerStreams.some((h) => h.names.has("test.Child")), "the handler")
    const failure = await send(w.bus, Child, 1).then(() => undefined, (e) => e)
    expect(failure).toBeInstanceOf(CommandExecutionError)
    expect(failure.message).toBe("no stock")
    expect(failure.errorCode).toBe("KRONOS-4002")
  })

  it("rejects an oversized command at the caller, before it leaves", async () => {
    r = await rig({ server: { maxFrame: 4096 } })
    const w = wire(r)
    w.bus.subscribe("test.Child", async () => 1)
    const big = await w.bus.dispatch({ kind: "command", identifier: "big", name: qn("test", "Child"), payload: "x".repeat(10_000), metadata: {}, timestamp: 1 }).then(() => undefined, (e) => e)
    expect(big).toBeInstanceOf(KronosDbError)
    expect(big.message).toMatch(/exceeds the server's limit/)
    expect(w.latch.activeCount).toBe(0)
  })

  it("holds a subscription's readiness until the server has acknowledged it", async () => {
    r = await rig({ server: { ackDelayMs: 60 } })
    const w = wire(r)
    w.bus.subscribe("test.Child", async () => 1)
    expect(r.client.readiness).toHaveLength(1)
    let ready = false
    const waiting = r.client.readiness[0]!().then(() => { ready = true })
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(ready).toBe(false)
    await waiting
    expect(r.server.handlerStreams[0]!.names.has("test.Child")).toBe(true)
  })

  it("has nothing to wait for before the first subscription", async () => {
    r = await rig()
    wire(r)
    await r.client.readiness[0]!()
    expect(r.server.handlerStreams).toHaveLength(0)
    expect(r.server.connections).toHaveLength(0)
  })
})

describe("kronosDbCommandBus — the handler stream comes back", () => {
  let r: Rig
  afterEach(() => r?.close())

  it("announces every name again, with credit, on a new connection after the old one is lost", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Parent", async ({ payload }) => payload)
    w.bus.subscribe("test.Child", async ({ payload }) => payload)
    await until(() => r.server.handlerStreams[0]?.names.size === 2, "the first announcement")

    r.server.connections[0]!.destroy()
    await until(() => r.server.handlerStreams.length === 2 && r.server.handlerStreams[1]!.names.size === 2, "the second announcement")
    const again = r.server.handlerStreams[1]!
    expect(again.connection).not.toBe(r.server.handlerStreams[0]!.connection)
    expect([...again.names.keys()].sort()).toEqual(["test.Child", "test.Parent"])
    expect(again.credits).toEqual([5000])
    expect(await send(w.bus, Child, 9)).toBe(9)
  })

  it("moves to a fresh stream when the connection is reset, leaving nothing registered on the old one", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Child", async ({ payload }) => payload)
    await until(() => r.server.handlerStreams[0]?.names.size === 1, "the announcement")
    await r.client.connection.reconnect()
    await until(() => r.server.handlerStreams.length === 2 && r.server.handlerStreams[1]!.names.size === 1, "the new stream")
    expect(r.server.handlerStreams[0]!.ended).toBe(true)
    expect(r.server.handlerStreams[1]!.ended).toBe(false)
    expect(await send(w.bus, Child, 4)).toBe(4)
  })

  it("does not reopen after shutdown has begun, but keeps answering on the stream it has", async () => {
    r = await rig()
    const w = wire(r)
    const entered = deferred(), release = deferred()
    w.bus.subscribe("test.Child", async ({ payload }) => { entered.resolve(); await release.promise; return payload })
    const result = w.remote("test.Child", 5)
    await entered.promise
    const drain = w.latch.initiateShutdown()
    await tick()
    release.resolve()
    expect(await result).toBe(5)
    await drain
    expect(r.server.handlerStreams.filter((h) => h.clientId === r.client.connection.config.clientId)).toHaveLength(1)
  })

  it("lets two buses for the same bus name coexist instead of replacing each other", async () => {
    r = await rig()
    const first = wire(r)
    const second = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host)
    first.bus.subscribe("test.Parent", async ({ payload }) => payload)
    second.subscribe("test.Child", async ({ payload }) => payload)
    await until(() => r.server.handlerStreams.length === 2 && r.server.handlerStreams.every((h) => h.names.size === 1), "both streams")
    const id = r.client.connection.config.clientId
    expect(r.server.handlerStreams.map((h) => h.clientId)).toEqual([id, `${id}#2`])
    expect(await send(first.bus, Parent, 1)).toBe(1)
    expect(await send(second, Child, 2)).toBe(2)
    await tick()
    expect(r.server.handlerStreams.some((h) => h.ended)).toBe(false)
  })

  it("is replaced, not duplicated, when the same client opens a second stream", async () => {
    r = await rig()
    const w = wire(r)
    w.bus.subscribe("test.Child", async ({ payload }) => payload)
    await until(() => r.server.handlerStreams[0]?.names.size === 1, "the announcement")
    // A restarted process with the same client id takes over from the stale stream.
    const sameId = r.another({ clientId: r.client.connection.config.clientId })
    const takeover = kronosDbCommandBus(localCommandBus(unitOfWork), sameId.host)
    takeover.subscribe("test.Child", async ({ payload }) => (payload as number) * 10)
    await until(() => r.server.handlerStreams.some((h) => h.connection !== r.server.handlerStreams[0]!.connection && h.names.size === 1), "the takeover")
    expect(r.server.handlerStreams[0]!.ended).toBe(true)
  })
})
