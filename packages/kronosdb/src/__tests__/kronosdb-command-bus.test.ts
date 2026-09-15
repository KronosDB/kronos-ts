import { afterEach, describe, expect, it } from "bun:test"
import { createServer, ServerError, Status } from "nice-grpc"
import { z } from "zod"
import {
  command, commandHandler, inMemoryEventStore, jsonSerializer, kronos,
  localCommandBus, localQueryBus, qn, send, unitOfWork, type UnitOfWork,
} from "@kronos-ts/core"
import { connectToKronosDb } from "../connection.js"
import { kronosDbCommandBus, type KronosDbCommandBusOptions } from "../kronosdb.js"
import { CommandServiceDefinition, type Command, type CommandResponse, type CommandHandlerInbound } from "../generated/command.js"
import { outboundStream } from "../outbound-stream.js"
import type { ShutdownLatch } from "../shutdown-latch.js"

const Parent = command({ name: qn("test", "Parent"), payload: z.number(), result: z.number() })
const Child = command({ name: qn("test", "Child"), payload: z.number(), result: z.number() })

function deferred<T = void>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((r) => { resolve = r })
  return { promise, resolve }
}

// A loopback gRPC router: every dispatch crosses the protobuf wire, consumes
// a stream credit, and waits for its own requestIdentifier on that SAME stream.
// There is no local dispatch shortcut and no external KronosDB dependency.
async function wireBus(options: KronosDbCommandBusOptions = {}) {
  const server = createServer()
  const inbound = outboundStream<CommandHandlerInbound>()
  const pending = new Map<string, ReturnType<typeof deferred<CommandResponse>>>()
  const queued: Command[] = []
  const requests: Command[] = []
  const responses: CommandResponse[] = []
  const grants: bigint[] = []
  const subscribed = new Set<string>()
  let credits = 0n
  let streams = 0
  let latch!: ShutdownLatch

  function route() {
    while (credits > 0n && queued.length && subscribed.has(queued[0]!.name)) {
      credits--
      inbound.send({ command: queued.shift()!, instructionId: "" })
    }
  }

  server.add(CommandServiceDefinition, {
    async *openStream(outbound, ctx) {
      streams++
      ctx.signal.addEventListener("abort", () => inbound.close(), { once: true })
      const consume = (async () => {
        try {
          for await (const frame of outbound) {
            if (frame.subscribe) subscribed.add(frame.subscribe.command)
            if (frame.flowControl) {
              grants.push(frame.flowControl.permits)
              credits += frame.flowControl.permits
            }
            if (frame.commandResponse) {
              responses.push(frame.commandResponse)
              pending.get(frame.commandResponse.requestIdentifier)?.resolve(frame.commandResponse)
            }
            route()
          }
        } catch (error) {
          if (!ctx.signal.aborted) throw error
        } finally {
          inbound.close()
        }
      })()
      try { yield* inbound.iterable } finally { await consume }
    },
    async dispatch(request) {
      requests.push(request)
      const result = deferred<CommandResponse>()
      pending.set(request.messageIdentifier, result)
      queued.push(request)
      route()
      let timer: ReturnType<typeof setTimeout> | undefined
      try {
        return await Promise.race([
          result.promise,
          new Promise<never>((_, reject) => {
            timer = setTimeout(() => reject(new ServerError(Status.DEADLINE_EXCEEDED, "command dispatch timed out")), 1000)
          }),
        ])
      } finally {
        clearTimeout(timer)
        pending.delete(request.messageIdentifier)
      }
    },
  })
  const port = await server.listen("127.0.0.1:0")
  const connection = connectToKronosDb({ host: "127.0.0.1", port, componentName: "nested-test" })
  const bus = kronosDbCommandBus(localCommandBus(unitOfWork), {
    connection,
    serializer: jsonSerializer(),
    registerShutdownLatch(value) { latch = value },
  }, "default", options)
  return {
    bus, connection, requests, responses, grants,
    get streams() { return streams },
    get latch() { return latch },
    // Use the raw client to model a remote caller: no outbound adapter activity
    // can accidentally cover for a missing inbound activity in the drain test.
    remote(name: string, value: number) {
      return connection.commands.dispatch({
        messageIdentifier: crypto.randomUUID(), name,
        payload: jsonSerializer().serialize(value, name),
      })
    },
    close() {
      void latch.initiateShutdown()
      connection.close()
      inbound.close()
      server.forceShutdown()
    },
  }
}

describe("kronosDbCommandBus — concurrent inbound wire handling", () => {
  let wire: Awaited<ReturnType<typeof wireBus>>
  afterEach(() => wire?.close())

  it("cancels a wire dispatch at the client deadline while still tracking its handler", async () => {
    wire = await wireBus({ timeoutMs: 30, limits: { maxConcurrentHandlers: 1 } })
    const entered = deferred(), release = deferred()
    wire.bus.subscribe("test.Parent", async () => { entered.resolve(); await release.promise; return 42 })
    const started = Date.now()
    const result = send(wire.bus, Parent, 1)
    void result.catch(() => {})
    await entered.promise
    try {
      await expect(result).rejects.toThrow(/aborted|deadline|cancel/i)
      expect(Date.now() - started).toBeLessThan(500)
      expect(wire.latch.activeCount).toBe(1)
    } finally {
      release.resolve()
      await wire.latch.initiateShutdown()
    }
  })

  it("lets A await ctx.send(B) on the same connection with independent units of work", async () => {
    wire = await wireBus({ flowControl: { permits: 1, refillThreshold: 0 } })
    const units: UnitOfWork[] = []
    const commits: string[] = []
    const parent = commandHandler(Parent, async ({ payload }, ctx) => {
      units.push(ctx.unitOfWork)
      ctx.unitOfWork.onCommit(() => { commits.push("parent") })
      return await ctx.send(Child, payload) as number
    })
    const child = commandHandler(Child, async ({ payload }, ctx) => {
      units.push(ctx.unitOfWork)
      ctx.unitOfWork.onCommit(() => { commits.push("child") })
      return payload * 2
    })
    const app = kronos({ commandHandlers: [parent, child].map((handler) => ({
      ...handler, commandBus: wire.bus, queryBus: localQueryBus(unitOfWork), eventStore: inMemoryEventStore(),
    })) })
    try {
      expect(await send(wire.bus, Parent, 21)).toBe(42)
      expect(wire.streams).toBe(1)
      expect(wire.requests.map((r) => r.name)).toEqual(["test.Parent", "test.Child"])
      expect(wire.responses.map((r) => r.requestIdentifier)).toEqual(wire.requests.map((r) => r.messageIdentifier).reverse())
      expect(units).toHaveLength(2)
      expect(units[0]).not.toBe(units[1])
      expect(commits).toEqual(["child", "parent"])
      expect(wire.latch.activeCount).toBe(0)
    } finally { await app.stop() }
  })

  it("refills receive credits while parents wait, including nesting deeper than the permit window", async () => {
    wire = await wireBus({ flowControl: { permits: 1, refillThreshold: 0 } })
    wire.bus.subscribe("test.Parent", async (message) => {
      const depth = message.payload as number
      return depth === 0 ? 0 : 1 + Number(await send(wire.bus, Parent, depth - 1))
    })
    expect(await send(wire.bus, Parent, 8)).toBe(8)
    expect(wire.requests).toHaveLength(9)
    expect(wire.responses).toHaveLength(9)
    expect(wire.grants.length).toBeGreaterThanOrEqual(9)
    expect(wire.grants.every((grant) => grant === 1n)).toBe(true)
  })

  it("propagates child failures to the parent and keeps unrelated work running", async () => {
    wire = await wireBus()
    wire.bus.subscribe("test.Parent", async ({ payload }) => await send(wire.bus, Child, payload as number))
    wire.bus.subscribe("test.Child", async ({ payload }) => {
      if (payload === 0) throw new Error("child failed")
      return payload
    })
    const results = await Promise.allSettled([send(wire.bus, Parent, 0), send(wire.bus, Parent, 7)])
    expect(results[0].status).toBe("rejected")
    if (results[0].status === "rejected") expect(results[0].reason.message).toBe("child failed")
    expect(results[1]).toEqual({ status: "fulfilled", value: 7 })
    expect(wire.responses.filter((r) => r.errorCode === "KRONOS-4002")).toHaveLength(2)
    expect(wire.streams).toBe(1)
    expect(wire.latch.activeCount).toBe(0)
  })

  it("isolates response serialization failures without restarting the stream", async () => {
    wire = await wireBus()
    wire.bus.subscribe("test.Parent", async () => { const value: any = {}; value.self = value; return value })
    wire.bus.subscribe("test.Child", async ({ payload }) => payload)
    await expect(send(wire.bus, Parent, 0)).rejects.toThrow()
    expect(await send(wire.bus, Child, 7)).toBe(7)
    expect(wire.responses[0]!.errorCode).toBe("KRONOS-4002")
    expect(wire.streams).toBe(1)
    expect(wire.latch.activeCount).toBe(0)
  })

  it("drains remote inbound handlers and rejects new work during shutdown", async () => {
    wire = await wireBus()
    const entered = deferred()
    const release = deferred()
    let calls = 0
    wire.bus.subscribe("test.Parent", async () => {
      calls++
      entered.resolve()
      await release.promise
      return 42
    })
    const result = wire.remote("test.Parent", 0)
    await entered.promise
    expect(wire.latch.activeCount).toBe(1)
    let drained = false
    const drain = wire.latch.initiateShutdown().then(() => { drained = true })
    try {
      const rejected = await wire.remote("test.Parent", 1)
      expect(rejected.errorMessage?.message).toBe("Shutdown in progress")
      expect(calls).toBe(1)
      expect(drained).toBe(false)
    } finally { release.resolve() }
    await drain
    expect(jsonSerializer().deserialize<number>((await result).payload!)).toBe(42)
    expect(wire.latch.activeCount).toBe(0)
    expect(wire.streams).toBe(1)
    await expect(send(wire.bus, Parent, 0)).rejects.toThrow("Shutdown in progress")
  })

  it("drains a running parent and child, including the child's outbound dispatch", async () => {
    wire = await wireBus()
    const entered = deferred()
    const release = deferred()
    wire.bus.subscribe("test.Parent", async ({ payload }) => await send(wire.bus, Child, payload as number))
    wire.bus.subscribe("test.Child", async ({ payload }) => {
      entered.resolve()
      await release.promise
      return payload
    })
    const result = wire.remote("test.Parent", 42)
    await entered.promise
    let drained = false
    const drain = wire.latch.initiateShutdown().then(() => { drained = true })
    try {
      // Remote parent inbound + child outbound + child inbound.
      expect(wire.latch.activeCount).toBe(3)
      await Promise.resolve()
      expect(drained).toBe(false)
    } finally { release.resolve() }
    await drain
    expect(jsonSerializer().deserialize<number>((await result).payload!)).toBe(42)
    expect(wire.latch.activeCount).toBe(0)
    expect(wire.responses.map((r) => r.requestIdentifier)).toEqual(wire.requests.map((r) => r.messageIdentifier).reverse())
  })
})
