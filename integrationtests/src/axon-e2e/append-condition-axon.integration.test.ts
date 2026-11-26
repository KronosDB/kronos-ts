/**
 * A decision over two states against Axon Server — the scenarios in
 * `../__tests__/append-condition-scenarios.ts`. Axon's append condition takes
 * one marker, so it checks the whole condition from the earliest read.
 */
import { afterAll, beforeAll, describe } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { emptyMetadata, jsonSerializer, qn, type EventStore } from "@kronos-ts/core"
import { axonServerConnection, axonServerEventStore } from "@kronos-ts/axon-server"
import { appendConditionScenarios } from "../__tests__/append-condition-scenarios.js"
import { eventStoreContract } from "../__tests__/event-store-contract.js"

async function initClusterWithDcb(host: string, httpPort: number): Promise<void> {
  const init = await fetch(`http://${host}:${httpPort}/v2/cluster/init?dcb=true`, { method: "POST" })
  if (!init.ok) throw new Error(`Axon Server refused cluster init: ${init.status} ${await init.text()}`)
  const start = Date.now()
  while (Date.now() - start < 15000) {
    try {
      const res = await fetch(`http://${host}:${httpPort}/v1/public/context`)
      const contexts = (await res.json()) as Array<{ context: string }>
      if (contexts.some((c) => c.context === "default")) return
    } catch {
      /* not ready */
    }
    await new Promise((r) => setTimeout(r, 500))
  }
  throw new Error("Timed out waiting for default context")
}

async function startAxonServer(): Promise<StartedTestContainer> {
  const container = await new GenericContainer("axoniq/axonserver:2026.1.8")
    .withExposedPorts(8024, 8124)
    .withEnvironment({ AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true" })
    .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
    .start()
  await initClusterWithDcb(container.getHost(), container.getMappedPort(8024))
  return container
}

/**
 * Wait until a freshly initialised context has taken an append and answered a
 * read. It refuses both for a moment, and its first append takes seconds where
 * the ones after it take milliseconds, which is longer than a test may take on
 * a slow machine.
 */
async function served(store: EventStore): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await store.append([
        {
          event: {
            kind: "event",
            identifier: crypto.randomUUID(),
            name: qn("markers", "WarmedUp"),
            version: "1.0",
            payload: {},
            metadata: emptyMetadata(),
            timestamp: Date.now(),
          },
          tags: [{ key: "subject", value: "warm-up" }],
        },
      ])
      await store.source({ query: { tags: { subject: "warm-up" } } })
      return
    } catch (error) {
      if (attempt >= 60) throw error
      await new Promise((r) => setTimeout(r, 500))
    }
  }
}

describe("append condition — axon server", () => {
  let container: StartedTestContainer
  let axon: Awaited<ReturnType<typeof axonServerConnection>>
  let eventStore: EventStore
  const connections: Array<{ close(): Promise<void> }> = []
  const containers: StartedTestContainer[] = []

  const connect = async (host: string, port: number) => {
    const opened = await axonServerConnection({
      componentName: "append-condition-test",
      host,
      port,
      context: "default",
      serializer: jsonSerializer(),
      readTimeoutMs: 5_000,
    })
    connections.push(opened)
    return opened
  }

  beforeAll(async () => {
    container = await startAxonServer()
    axon = await connect(container.getHost(), container.getMappedPort(8124))
    await axon.start()
    eventStore = axonServerEventStore(axon, "default")
    await served(eventStore)
  }, 180_000)

  afterAll(async () => {
    for (const opened of connections) await opened.close().catch(() => {})
    for (const started of containers) await started.stop().catch(() => {})
    await container?.stop()
  })

  appendConditionScenarios({ eventStore: () => eventStore, exact: false })
  eventStoreContract({
    eventStore: () => eventStore,
    unreachable: async (port) => axonServerEventStore(await connect("127.0.0.1", port), "default"),
    stopped: async () => {
      const own = await startAxonServer()
      containers.push(own)
      const store = axonServerEventStore(await connect(own.getHost(), own.getMappedPort(8124)), "default")
      await served(store)
      await own.stop()
      return store
    },
  })
})
