/**
 * The error contract of a remote bus (`./bus-contract.ts`) against KronosDB and
 * RabbitMQ: two instances each, on separate connections. Axon Server runs it
 * from `../axon-e2e`.
 */
import { afterAll, describe } from "bun:test"
import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"
import { jsonSerializer, localCommandBus, localQueryBus, unitOfWork } from "@kronos-ts/core"
import {
  kronosDbCommandBus,
  kronosDbConnection,
  kronosDbQueryBus,
  type KronosDbConnectionHandle,
} from "@kronos-ts/kronosdb"
import { rabbitMqCommandBus, rabbitMqConnection, rabbitMqQueryBus } from "@kronos-ts/rabbitmq"
import { startKronosDb, type KronosDbServer } from "../kronosdb-server.js"
import { busContract } from "./bus-contract.js"

describe("bus contract — kronosdb", () => {
  let server: KronosDbServer | undefined
  const connections: KronosDbConnectionHandle[] = []

  afterAll(async () => {
    for (const kdb of connections) await kdb.close()
    await server?.stop()
  })

  busContract({
    instances: async () => {
      const started = await startKronosDb()
      server = started
      const instance = async (componentName: string) => {
        const kdb = await kronosDbConnection({
          componentName,
          host: started.host,
          port: started.port,
          serializer: jsonSerializer(),
          shutdownTimeoutMs: 200,
        })
        connections.push(kdb)
        return {
          commandBus: kronosDbCommandBus(localCommandBus(unitOfWork), kdb),
          queryBus: kronosDbQueryBus(localQueryBus(unitOfWork), kdb),
        }
      }
      return { handling: await instance("bus-contract-handling"), calling: await instance("bus-contract-calling") }
    },
  })
})

describe("bus contract — rabbitmq", () => {
  let container: StartedTestContainer | undefined
  const connections: Array<{ close(): Promise<void> }> = []

  afterAll(async () => {
    for (const rabbit of connections) await rabbit.close()
    await container?.stop()
  })

  busContract({
    instances: async () => {
      container = await new GenericContainer("rabbitmq:3.13-alpine")
        .withExposedPorts(5672)
        .withWaitStrategy(Wait.forLogMessage("Server startup complete"))
        .start()
      const url = `amqp://guest:guest@${container.getHost()}:${container.getMappedPort(5672)}`
      const prefix = `bus-contract-${Date.now().toString(36)}`
      const instance = async (serviceName: string) => {
        const rabbit = await rabbitMqConnection(url, { serviceName, instanceId: `${prefix}-${serviceName}`, topology: { prefix } })
        connections.push(rabbit)
        const buses = {
          commandBus: rabbitMqCommandBus(localCommandBus(unitOfWork), rabbit),
          queryBus: rabbitMqQueryBus(localQueryBus(unitOfWork), rabbit),
        }
        await rabbit.start()
        return buses
      }
      return { handling: await instance("handling"), calling: await instance("calling") }
    },
  })
})
