import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, qn } from "@kronos-ts/core"
import { query, payloadEquals, unitOfWork } from "@kronos-ts/core"
import {
  correlation,
  interceptingQueryBus,
  localQueryBus,
  type QueryBus,
} from "@kronos-ts/core"
import {
  rabbitMqQueryBus,
  type RabbitMqQueryEnvelope,
  type RabbitMqQueryTransport,
} from "../query-bus.js"
import type { RabbitMqBusOptions } from "../command-bus.js"
import type {
  DeliverEnvelope,
  DistributedSubscriberRegistry,
  SubscriberRecord,
} from "../distributed-subscriber-registry.js"
import { resolveRabbitMqConfig, type RabbitMqConfig } from "../rabbitmq.js"
import type { RabbitMqIdentity } from "../topology.js"

const GetThing = query({
  name: qn("test", "GetThing"),
  payload: z.object({ id: z.string() }),
})

const DEFAULT_IDENTITY: RabbitMqIdentity = { serviceName: "svc", instanceId: "inst" }

function rabbitConfig(
  config: Omit<RabbitMqConfig, "identity">,
  identity: RabbitMqIdentity = DEFAULT_IDENTITY,
) {
  return resolveRabbitMqConfig({ identity, ...config })
}

function recordingTransport() {
  const subscriptions = new Map<string, (envelope: RabbitMqQueryEnvelope) => Promise<any>>()
  const dispatched: RabbitMqQueryEnvelope[] = []
  const transport: RabbitMqQueryTransport = {
    async dispatch(envelope) {
      dispatched.push(envelope)
      return { requestId: envelope.requestId, ok: true, result: "remote-ok" }
    },
    subscribe(name, handler) {
      subscriptions.set(name, handler)
    },
  }
  return { transport, dispatched, subscriptions }
}

/**
 * The composition under test: the transport owns its own client-side routing,
 * over the caller's local segment, with correlation OUTSIDE the fork.
 */
function busOver(params: {
  transport: RabbitMqQueryTransport
  localSegment?: QueryBus
  subscriberRegistry?: DistributedSubscriberRegistry
  identity?: RabbitMqIdentity
  options?: RabbitMqBusOptions
}): QueryBus {
  return interceptingQueryBus(
    rabbitMqQueryBus(
      params.localSegment ?? localQueryBus(unitOfWork),
      {
        config: rabbitConfig({ url: "amqp://test" }, params.identity),
        queryTransport: params.transport,
        subscriberRegistry: params.subscriberRegistry,
      },
      params.options,
    ),
    correlation,
  )
}

describe("RabbitMQ query bus", () => {
  it("prefers local handlers by default", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver({ transport })

    bus.subscribe("test.GetThing", async () => "local-ok")

    const result = await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("local-ok")
    expect(dispatched).toHaveLength(0)
  })

  it("routes through transport when distributed routing is forced", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver({ transport, options: { preferLocal: false } })

    bus.subscribe("test.GetThing", async () => "local-ok")

    const result = await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(result).toBe("remote-ok")
    expect(dispatched).toHaveLength(1)
  })

  it("carries query metadata across the transport envelope", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver({ transport, options: { preferLocal: false } })

    await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: { correlationId: "corr-1", causationId: "cause-1" },
      timestamp: Date.now(),
    })

    // `correlation` SEEDS and never clobbers: a query that already carries a cause
    // keeps it across the hop, so the causal chain survives more than one leg.
    expect(dispatched[0]!.message.metadata).toEqual({
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("stamps the connection's timeout on the envelope when routing names none", async () => {
    const { transport, dispatched } = recordingTransport()
    const bus = busOver({ transport, options: { preferLocal: false } })

    await bus.query({
      identifier: "qry-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(dispatched[0]!.timeoutMs).toBe(30_000)
  })

  it("propagates a remote query failure as a thrown error", async () => {
    const transport: RabbitMqQueryTransport = {
      async dispatch(envelope) {
        return {
          requestId: envelope.requestId,
          ok: false,
          error: { name: "LookupError", message: "no such thing" },
        }
      },
      subscribe() {},
    }
    const bus = busOver({ transport, options: { preferLocal: false } })

    await expect(
      bus.query({
        identifier: "qry-1",
        name: GetThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      }),
    ).rejects.toThrow("no such thing")
  })

  it("handles an inbound query in its own UnitOfWork", async () => {
    const { transport, subscriptions } = recordingTransport()
    const bus = busOver({ transport })

    bus.subscribe("test.GetThing", async (message) => `answered:${(message.payload as { id: string }).id}`)
    const subscribed = subscriptions.get("test.GetThing")!
    const reply = await subscribed({
      kind: "query",
      requestId: "qry-1",
      timeoutMs: 1000,
      message: {
        identifier: "qry-1",
        name: GetThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      },
    })

    expect(reply.ok).toBe(true)
    expect(reply.result).toBe("answered:1")
  })

  it("reports a handler failure as an ok:false reply rather than a rejection", async () => {
    const { transport, subscriptions } = recordingTransport()
    const bus = busOver({ transport })

    bus.subscribe("test.GetThing", async () => {
      throw new Error("read model unavailable")
    })
    const reply = await subscriptions.get("test.GetThing")!({
      kind: "query",
      requestId: "qry-1",
      timeoutMs: 1000,
      message: {
        identifier: "qry-1",
        name: GetThing.name,
        payload: { id: "1" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
      },
    })

    expect(reply.ok).toBe(false)
    expect(reply.error?.message).toBe("read model unavailable")
  })

  it("returns initial result via the existing query transport when starting a subscription", async () => {
    const { transport } = recordingTransport()
    const bus = busOver({ transport })

    bus.subscribe("test.GetThing", async () => "initial")

    const sub = bus.subscriptionQuery({
      identifier: "sub-1",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })

    expect(await sub.initialResult).toBe("initial")
    sub.close()
  })

  it("delivers a local emit to a local subscriber via the registry mirror", async () => {
    const mesh = createInProcessRegistryMesh()
    const { transport } = recordingTransport()
    const bus = busOver({ transport, subscriberRegistry: mesh.join("inst-A") })

    bus.subscribe("test.GetThing", async () => "initial")
    const sub = bus.subscriptionQuery({
      identifier: "sub-local",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await sub.initialResult

    void bus.emitUpdate("test.GetThing", payloadEquals({ id: "1" }), { value: "v1" })

    const updates = await readN(sub.updates, 1)
    expect(updates).toEqual([{ value: "v1" }])

    sub.close()
  })

  it("delivers across instances — emitUpdate on one instance routes to the subscriber on another", async () => {
    const mesh = createInProcessRegistryMesh()

    // Instance B owns the subscriber.
    const busB = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-B"),
      identity: { serviceName: "svc", instanceId: "inst-B" },
    })
    busB.subscribe("test.GetThing", async () => "initial-B")
    const sub = busB.subscriptionQuery({
      identifier: "sub-cross",
      name: GetThing.name,
      payload: { id: "x" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await sub.initialResult

    // Instance C emits — it has no local subscriber but should route to B.
    const busC = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-C"),
      identity: { serviceName: "svc", instanceId: "inst-C" },
    })

    void busC.emitUpdate("test.GetThing", payloadEquals({ id: "x" }), "from-C")

    const updates = await readN(sub.updates, 1)
    expect(updates).toEqual(["from-C"])

    sub.close()
  })

  it("function predicates work across instances because they execute on the emitter's mirror", async () => {
    const mesh = createInProcessRegistryMesh()

    // Two subscribers on instance B with different payloads.
    const busB = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-B"),
      identity: { serviceName: "svc", instanceId: "inst-B" },
    })
    busB.subscribe("test.GetThing", async () => "initial")
    const sub1 = busB.subscriptionQuery({
      identifier: "sub-lo",
      name: GetThing.name,
      payload: { id: "1" } as any,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    const sub2 = busB.subscriptionQuery({
      identifier: "sub-hi",
      name: GetThing.name,
      payload: { id: "9" } as any,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await Promise.all([sub1.initialResult, sub2.initialResult])

    // Emit from C with a function filter — only "9" matches.
    const busC = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-C"),
      identity: { serviceName: "svc", instanceId: "inst-C" },
    })

    void busC.emitUpdate(
      "test.GetThing",
      (payload) => (payload as { id: string }).id === "9",
      "match-hi",
    )

    // sub2 receives; sub1 closes without receiving.
    const matched = await readN(sub2.updates, 1)
    expect(matched).toEqual(["match-hi"])

    sub1.close()
    sub2.close()
  })

  it("a late-joining instance learns existing claims via syncRequest", async () => {
    const mesh = createInProcessRegistryMesh()

    // B subscribes first.
    const busB = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-B"),
      identity: { serviceName: "svc", instanceId: "inst-B" },
    })
    busB.subscribe("test.GetThing", async () => "initial")
    const sub = busB.subscriptionQuery({
      identifier: "sub-late",
      name: GetThing.name,
      payload: { id: "z" } as any,
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await sub.initialResult

    // C joins later — its mirror starts empty, then fills via sync reply.
    const busC = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-C"),
      identity: { serviceName: "svc", instanceId: "inst-C" },
    })

    void busC.emitUpdate("test.GetThing", payloadEquals({ id: "z" }), "late-update")

    const updates = await readN(sub.updates, 1)
    expect(updates).toEqual(["late-update"])

    sub.close()
  })

  it("release on unsubscribe removes the record from peer mirrors", async () => {
    const mesh = createInProcessRegistryMesh()
    const busB = busOver({
      transport: recordingTransport().transport,
      subscriberRegistry: mesh.join("inst-B"),
      identity: { serviceName: "svc", instanceId: "inst-B" },
    })
    busB.subscribe("test.GetThing", async () => "initial")
    const sub = busB.subscriptionQuery({
      identifier: "sub-rm",
      name: GetThing.name,
      payload: { id: "1" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    })
    await sub.initialResult

    const registryC = mesh.join("inst-C")
    expect([...registryC.records()].some((r) => r.subId === "sub-rm")).toBe(true)

    sub.close()
    expect([...registryC.records()].some((r) => r.subId === "sub-rm")).toBe(false)
  })
})

// ---------------------------------------------------------------------------
// In-process registry mesh — wires N AmqpDistributedSubscriberRegistry stand-
// ins together over a synchronous in-memory bus that mimics the gossip-fanout +
// direct-routed topology.
// ---------------------------------------------------------------------------

type MeshRegistry = DistributedSubscriberRegistry & {
  readonly instanceId: string
}

type InProcessMesh = {
  join(instanceId: string): MeshRegistry
}

function createInProcessRegistryMesh(): InProcessMesh {
  const registries: MeshRegistry[] = []

  function broadcast(envelope:
    | { kind: "claim"; ownerInstanceId: string; subId: string; queryName: string; payload: unknown }
    | { kind: "release"; ownerInstanceId: string; subId: string }
    | { kind: "syncRequest"; requesterId: string },
  ) {
    for (const r of registries) (r as any).__handleGossip(envelope)
  }

  function deliverDirect(targetInstanceId: string, envelope: DeliverEnvelope) {
    const target = registries.find((r) => r.instanceId === targetInstanceId)
    if (!target) return
    ;(target as any).__handleDirect(envelope)
  }

  return {
    join(instanceId) {
      const mirror = new Map<string, SubscriberRecord>()
      const locallyOwned = new Set<string>()
      let deliverHandler: ((envelope: DeliverEnvelope) => void) | undefined

      const reg: MeshRegistry = {
        instanceId,
        async claim(record) {
          const full: SubscriberRecord = { ...record, ownerInstanceId: instanceId }
          mirror.set(full.subId, full)
          locallyOwned.add(full.subId)
          broadcast({
            kind: "claim",
            ownerInstanceId: instanceId,
            subId: full.subId,
            queryName: full.queryName,
            payload: full.payload,
          })
        },
        async release(subId) {
          mirror.delete(subId)
          locallyOwned.delete(subId)
          broadcast({ kind: "release", ownerInstanceId: instanceId, subId })
        },
        *records() {
          for (const r of mirror.values()) yield r
        },
        async deliver(envelope) {
          const record = mirror.get(envelope.subId)
          if (!record) return
          if (record.ownerInstanceId === instanceId) {
            deliverHandler?.(envelope)
            return
          }
          deliverDirect(record.ownerInstanceId, envelope)
        },
        setDeliverHandler(handler) {
          deliverHandler = handler
        },
        async connect() {},
        async close() {},
      }

      // Backdoor hooks for the mesh — intentionally not part of the public
      // interface; the in-process mesh stands in for the AMQP transport.
      ;(reg as any).__handleGossip = (env: any) => {
        if (env.kind === "claim") {
          if (env.ownerInstanceId === instanceId) return
          mirror.set(env.subId, {
            subId: env.subId,
            queryName: env.queryName,
            payload: env.payload,
            ownerInstanceId: env.ownerInstanceId,
          })
        } else if (env.kind === "release") {
          if (env.ownerInstanceId === instanceId) return
          mirror.delete(env.subId)
        } else if (env.kind === "syncRequest") {
          if (env.requesterId === instanceId) return
          for (const subId of locallyOwned) {
            const record = mirror.get(subId)
            if (!record) continue
            broadcast({
              kind: "claim",
              ownerInstanceId: instanceId,
              subId: record.subId,
              queryName: record.queryName,
              payload: record.payload,
            })
          }
        }
      }
      ;(reg as any).__handleDirect = (env: DeliverEnvelope) => {
        deliverHandler?.(env)
      }

      registries.push(reg)
      // Joiner publishes its own syncRequest so existing peers re-broadcast
      // their owned claims into the new mirror.
      broadcast({ kind: "syncRequest", requesterId: instanceId })
      return reg
    },
  }
}

async function readN<T>(iter: AsyncIterable<T>, n: number): Promise<T[]> {
  const results: T[] = []
  for await (const v of iter) {
    results.push(v)
    if (results.length >= n) break
  }
  return results
}
