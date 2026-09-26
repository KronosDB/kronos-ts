/**
 * A decision over two states against Axon Server — the scenarios in
 * `../__tests__/append-condition-scenarios.ts`. Axon's append condition takes
 * one marker, so it checks the whole condition from the earliest read.
 */
import { afterAll, beforeAll, describe } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { jsonSerializer, type EventStore } from "@kronos-ts/core"
import { axonServerConnection, axonServerEventStore } from "@kronos-ts/axon-server"
import { appendConditionScenarios } from "../__tests__/append-condition-scenarios.js"

async function initClusterWithDcb(host: string, httpPort: number): Promise<void> {
  await fetch(`http://${host}:${httpPort}/v2/cluster/init?dcb=true`, { method: "POST" })
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

describe("append condition — axon server", () => {
  let container: StartedTestContainer
  let axon: Awaited<ReturnType<typeof axonServerConnection>>
  let eventStore: EventStore

  beforeAll(async () => {
    container = await new GenericContainer("axoniq/axonserver:2025.2.5")
      .withExposedPorts(8024, 8124)
      .withEnvironment({ AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true" })
      .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
      .start()
    await initClusterWithDcb(container.getHost(), container.getMappedPort(8024))
    axon = await axonServerConnection({
      componentName: "append-condition-test",
      host: container.getHost(),
      port: container.getMappedPort(8124),
      context: "default",
      serializer: jsonSerializer(),
    })
    await axon.start()
    eventStore = axonServerEventStore(axon, "default")
  }, 180_000)

  afterAll(async () => {
    await axon?.close()
    await container?.stop()
  })

  appendConditionScenarios({ eventStore: () => eventStore, exact: false })
})
