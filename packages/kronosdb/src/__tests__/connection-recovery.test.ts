import { afterEach, describe, expect, it } from "bun:test"
import { jsonSerializer } from "@kronos-ts/core"
import { connectToKronosDb, contextView, type KronosDbConnection } from "../connection.js"
import { kronosDbConnection } from "../kronosdb.js"
import { shutdownLatch } from "../shutdown-latch.js"
import { streamRecovery } from "../stream-recovery.js"
import { fakeNativeServer, type FakeNativeServer } from "./fake-native-server.js"

const servers: FakeNativeServer[] = []
const connections: KronosDbConnection[] = []

afterEach(async () => {
  for (const c of connections.splice(0)) c.close()
  await Promise.all(servers.splice(0).map((s) => s.close()))
})

async function fake(options?: Parameters<typeof fakeNativeServer>[0]) {
  const server = await fakeNativeServer(options)
  servers.push(server)
  return server
}

function connect(port: number, extra: Partial<Parameters<typeof connectToKronosDb>[0]> = {}) {
  const connection = connectToKronosDb({ host: "127.0.0.1", port, componentName: "recovery", connectTimeoutMs: 1000, reconnectIntervalMs: 5, ...extra })
  connections.push(connection)
  return connection
}

describe("kronos connection recovery", () => {
  it("keeps a healthy socket through a reconnect, and coalesces concurrent reconnects", async () => {
    const server = await fake()
    const connection = connect(server.port)
    const socket = await connection.native.ready()
    let disconnects = 0,
      reconnects = 0
    connection.onDisconnect(() => void disconnects++)
    connection.onReconnect(() => void reconnects++)

    await Promise.all([connection.reconnect(), connection.reconnect()])
    expect(connection.state).toBe("connected")
    expect(disconnects).toBe(1)
    expect(reconnects).toBe(1)
    // A socket that answers its liveness checks is not replaced.
    expect(connection.native.current()).toBe(socket)
    expect(server.connections).toHaveLength(1)
  })

  it("opens a new socket when the old one is gone, and only then reports recovery", async () => {
    const server = await fake()
    const connection = connect(server.port)
    await connection.native.ready()
    server.connections[0]!.destroy()
    await new Promise((resolve) => setTimeout(resolve, 20))
    let reconnects = 0
    connection.onReconnect(() => void reconnects++)
    await connection.reconnect()
    expect(connection.native.current()).toBeDefined()
    expect(server.connections).toHaveLength(2)
    expect(reconnects).toBe(1)
  })

  it("refuses to reconnect once closed", async () => {
    const server = await fake()
    const connection = connect(server.port)
    connection.close()
    expect(connection.state).toBe("closed")
    await expect(connection.reconnect()).rejects.toThrow(/permanently closed/)
  })

  it("reports failed recovery rather than success from a lazily opened socket", async () => {
    const dead = await fakeNativeServer()
    const port = dead.port
    await dead.close()
    const connection = connect(port, { maxReconnectAttempts: 2, connectTimeoutMs: 100 })
    let recovered = false
    connection.onReconnect(() => {
      recovered = true
    })
    await expect(connection.reconnect()).rejects.toThrow(/Failed to reconnect after 2 attempts/)
    expect(recovered).toBe(false)
    expect(connection.state).toBe("disconnected")
  })

  it("keeps trying while the server is down, and recovers when it is back", async () => {
    const first = await fakeNativeServer()
    const port = first.port
    await first.close()
    const connection = connect(port, { reconnectIntervalMs: 10, connectTimeoutMs: 100 })
    let reconnects = 0
    connection.onReconnect(() => void reconnects++)
    const recovering = connection.reconnect()
    await new Promise((resolve) => setTimeout(resolve, 60))
    expect(connection.state).toBe("reconnecting")
    expect(reconnects).toBe(0)

    servers.push(await fakeNativeServer({ port }))
    await recovering
    expect(connection.state).toBe("connected")
    expect(reconnects).toBe(1)
  })

  it("fails over to the next server in the list", async () => {
    const down = await fakeNativeServer()
    const downPort = down.port
    await down.close()
    const up = await fake()
    const connection = connect(up.port, { servers: [`127.0.0.1:${downPort}`, `127.0.0.1:${up.port}`] })
    await connection.reconnect()
    expect(up.connections).toHaveLength(1)
  })

  it("shares its socket among context views and does not let one close it", async () => {
    const server = await fake()
    const connection = connect(server.port)
    const billing = contextView(connection, "billing")
    expect(billing.config.context).toBe("billing")
    expect(connection.config.context).toBe("default")
    expect(billing.native).toBe(connection.native)
    expect(contextView(connection, "default")).toBe(connection)
    billing.close()
    expect(connection.state).toBe("connected")
    await billing.reconnect()
    expect(server.connections).toHaveLength(1)
  })
})

describe("kronos connection handle", () => {
  it("still closes the socket when a bus does not drain in time", async () => {
    const server = await fake()
    const kdb = await kronosDbConnection({
      componentName: "shutdown",
      host: "127.0.0.1",
      port: server.port,
      serializer: jsonSerializer(),
      shutdownTimeoutMs: 10,
    })
    const latch = shutdownLatch()
    kdb.registerShutdownLatch(latch)
    const activity = latch.registerActivity()
    try {
      await expect(kdb.close()).rejects.toThrow(/shutdown.*timed out/i)
      expect(kdb.connection.state).toBe("closed")
      expect(latch.activeCount).toBe(1)
    } finally {
      activity.end()
      await kdb.close()
    }
  })

  it("opens nothing until something needs it", async () => {
    const server = await fake()
    const kdb = await kronosDbConnection({ componentName: "lazy", host: "127.0.0.1", port: server.port, serializer: jsonSerializer() })
    await new Promise((resolve) => setTimeout(resolve, 30))
    expect(server.connections).toHaveLength(0)
    await kdb.close()
  })
})

describe("kronos stream recovery", () => {
  it("async EOF recovery backs off, coalesces, caps attempts and stops", async () => {
    let opened = 0
    const recovery = streamRecovery(
      () => {
        opened++
        queueMicrotask(() => recovery.failed(new Error("EOF")))
      },
      () => true,
      { initialDelayMs: 2, maxDelayMs: 4, maxAttempts: 3, log: () => {} },
    )
    try {
      recovery.failed(new Error("EOF"))
      recovery.failed(new Error("duplicate failure"))
      expect(opened).toBe(0)
      await new Promise((r) => setTimeout(r, 30))
      expect(opened).toBe(3)
      recovery.restart()
      expect(opened).toBe(4)
      await Promise.resolve()
      recovery.stop()
      await new Promise((r) => setTimeout(r, 10))
      expect(opened).toBe(4)
    } finally {
      recovery.stop()
    }
  })
})
