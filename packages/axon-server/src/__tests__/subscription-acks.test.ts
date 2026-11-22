import { afterEach, describe, expect, it } from "bun:test"
import { createServer } from "nice-grpc"
import { isUnavailable, jsonSerializer, localCommandBus, localQueryBus, unitOfWork } from "@kronos-ts/core"
import { axonServerCommandBus, axonServerQueryBus } from "../axon-server.js"
import { axonServerConnection, type AxonServerConnectionHandle } from "../connection.js"
import { AxonServerError } from "../errors.js"
import { CommandServiceDefinition } from "../generated/command.js"
import type { InstructionAck } from "../generated/common.js"
import { PlatformServiceDefinition } from "../generated/control.js"
import { QueryServiceDefinition } from "../generated/query.js"
import { subscriptionAcks } from "../subscription-acks.js"

/**
 * A gRPC server that speaks the three streams `start()` touches. `answer`
 * decides what each subscribe instruction is answered with, and when; `undefined`
 * leaves it unanswered.
 */
async function serverAnswering(answer: (instructionId: string) => Promise<InstructionAck | undefined>) {
  const server = createServer()
  const subscribed: string[] = []
  async function* provider(request: AsyncIterable<any>) {
    for await (const frame of request) {
      if (!frame.subscribe) continue
      subscribed.push(frame.subscribe.command ?? frame.subscribe.query)
      const ack = await answer(frame.instructionId)
      if (ack) yield { ack, instructionId: "" }
    }
  }
  async function* silent(request: AsyncIterable<any>) {
    for await (const _ of request) {
      /* Registration and heartbeats need no answer. */
    }
  }
  server.add(CommandServiceDefinition, { openStream: provider, dispatch: async () => ({}) } as any)
  server.add(QueryServiceDefinition, {
    openStream: provider,
    async *query() {},
    async *subscription() {},
  } as any)
  server.add(PlatformServiceDefinition, { openStream: silent, getPlatformServer: async () => ({}) } as any)
  const port = await server.listen("127.0.0.1:0")
  return { port, subscribed, stop: () => server.forceShutdown() }
}

const accepted = async (instructionId: string): Promise<InstructionAck> => ({
  instructionId,
  success: true,
  error: undefined,
})

describe("axonServerConnection start() and handler subscriptions", () => {
  let server: Awaited<ReturnType<typeof serverAnswering>> | undefined
  let axon: AxonServerConnectionHandle | undefined

  afterEach(async () => {
    await axon?.close().catch(() => {})
    server?.stop()
    axon = undefined
    server = undefined
  })

  async function connectAndSubscribe(
    answer: (instructionId: string) => Promise<InstructionAck | undefined>,
    startTimeoutMs?: number,
  ) {
    server = await serverAnswering(answer)
    axon = await axonServerConnection({
      host: "127.0.0.1",
      port: server.port,
      componentName: "acks",
      serializer: jsonSerializer(),
      startTimeoutMs,
    })
    axonServerCommandBus(localCommandBus(unitOfWork), axon).subscribe("acks.Command", async () => undefined)
    axonServerQueryBus(localQueryBus(unitOfWork), axon).subscribe("acks.Query", async () => undefined)
    return axon
  }

  it("resolves once Axon Server has acknowledged every subscription", async () => {
    const handle = await connectAndSubscribe(accepted)
    const began = Date.now()
    await handle.start()
    expect(Date.now() - began).toBeLessThan(500)
    expect(server!.subscribed.sort()).toEqual(["acks.Command", "acks.Query"])
  })

  it("waits for an acknowledgement and resolves when it arrives", async () => {
    let release!: () => void
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    const handle = await connectAndSubscribe(async (instructionId) => {
      await held
      return accepted(instructionId)
    })
    let started = false
    const starting = handle.start().then(() => {
      started = true
    })
    await new Promise((r) => setTimeout(r, 100))
    expect(started).toBe(false)
    const released = Date.now()
    release()
    await starting
    expect(Date.now() - released).toBeLessThan(500)
  })

  it("rejects with the server's error when a subscription is refused", async () => {
    const handle = await connectAndSubscribe(async (instructionId) => ({
      instructionId,
      success: false,
      error: { message: "not allowed here", location: "server", details: [], errorCode: "AXONIQ-9999" },
    }))
    const failure = await handle.start().then(
      () => undefined,
      (error: unknown) => error,
    )
    expect(failure).toBeInstanceOf(AxonServerError)
    expect((failure as AxonServerError).message).toContain("not allowed here")
    expect((failure as AxonServerError).errorCode).toBe("AXONIQ-9999")
  })

  it("rejects as unavailable when no acknowledgement arrives within startTimeoutMs", async () => {
    const handle = await connectAndSubscribe(async () => undefined, 50)
    const failure = await handle.start().then(
      () => undefined,
      (error: unknown) => error,
    )
    expect(isUnavailable(failure)).toBe(true)
    expect((failure as Error).message).toBe("Handler registration timed out after 50ms")
  })

  it("resolves at once when no handler is subscribed", async () => {
    server = await serverAnswering(async () => undefined)
    axon = await axonServerConnection({
      host: "127.0.0.1",
      port: server.port,
      componentName: "acks",
      serializer: jsonSerializer(),
    })
    const began = Date.now()
    await axon.start()
    expect(Date.now() - began).toBeLessThan(500)
  })
})

describe("subscriptionAcks", () => {
  const ack = (instructionId: string): InstructionAck => ({ instructionId, success: true, error: undefined })

  async function settled(promise: Promise<void>): Promise<boolean> {
    let done = false
    void promise.then(() => {
      done = true
    })
    await Promise.resolve()
    await Promise.resolve()
    return done
  }

  it("is acknowledged while nothing is pending", async () => {
    expect(await settled(subscriptionAcks().acknowledged())).toBe(true)
  })

  it("is acknowledged only when every instruction sent has been answered", async () => {
    const acks = subscriptionAcks()
    acks.sent("a")
    acks.sent("b")
    const waiting = acks.acknowledged()
    acks.received(ack("a"))
    expect(await settled(waiting)).toBe(false)
    acks.received(ack("unknown"))
    expect(await settled(waiting)).toBe(false)
    acks.received(ack("b"))
    expect(await settled(waiting)).toBe(true)
  })

  it("after a reset, waits for the instructions the new stream sends", async () => {
    const acks = subscriptionAcks()
    acks.sent("old")
    const waiting = acks.acknowledged()
    acks.reset()
    acks.sent("new")
    acks.received(ack("old"))
    expect(await settled(waiting)).toBe(false)
    acks.received(ack("new"))
    expect(await settled(waiting)).toBe(true)
  })
})
