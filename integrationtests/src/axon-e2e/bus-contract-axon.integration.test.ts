/**
 * The error contract of a remote bus (`../__tests__/bus-contract.ts`) against
 * Axon Server: two instances on separate connections.
 */
import { afterAll, describe } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { jsonSerializer, localCommandBus, localQueryBus, unitOfWork } from "@kronos-ts/core"
import { axonServerCommandBus, axonServerConnection, axonServerQueryBus } from "@kronos-ts/axon-server"
import { busContract } from "../__tests__/bus-contract.js"

describe("bus contract — axon server", () => {
  let container: StartedTestContainer | undefined
  const connections: Array<{ close(): Promise<void> }> = []

  afterAll(async () => {
    for (const axon of connections) await axon.close()
    await container?.stop()
  })

  busContract({
    instances: async () => {
      const started = await new GenericContainer("axoniq/axonserver:2025.2.5")
        .withExposedPorts(8024, 8124)
        .withEnvironment({ AXONIQ_AXONSERVER_DEVMODE_ENABLED: "true" })
        .withWaitStrategy(Wait.forHttp("/actuator/health", 8024).forStatusCode(200))
        .start()
      container = started
      const host = started.getHost()
      const http = started.getMappedPort(8024)
      await fetch(`http://${host}:${http}/v2/cluster/init`, { method: "POST" })
      const instance = async (componentName: string) => {
        const axon = await axonServerConnection({
          componentName,
          host,
          port: started.getMappedPort(8124),
          context: "default",
          serializer: jsonSerializer(),
        })
        connections.push(axon)
        await axon.start()
        return {
          commandBus: axonServerCommandBus(localCommandBus(unitOfWork), axon),
          queryBus: axonServerQueryBus(localQueryBus(unitOfWork), axon),
        }
      }
      return { handling: await instance("bus-contract-handling"), calling: await instance("bus-contract-calling") }
    },
  })
})
