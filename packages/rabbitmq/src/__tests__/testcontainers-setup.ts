import { GenericContainer, Wait, type StartedTestContainer } from "testcontainers"

export type RunningRabbitMq = {
  readonly url: string
  readonly host: string
  readonly port: number
  stop(): Promise<void>
}

export async function startRabbitMqContainer(): Promise<RunningRabbitMq> {
  const reuse = process.env.TESTCONTAINERS_REUSE === "1"
  let builder = new GenericContainer("rabbitmq:3.13-alpine")
    .withExposedPorts(5672)
    .withWaitStrategy(Wait.forLogMessage("Server startup complete"))

  if (reuse) builder = builder.withReuse()

  const container: StartedTestContainer = await builder.start()
  const host = container.getHost()
  const port = container.getMappedPort(5672)
  const url = `amqp://guest:guest@${host}:${port}`

  let stopped = false
  return {
    url,
    host,
    port,
    async stop() {
      if (stopped) return
      stopped = true
      await container.stop()
    },
  }
}
