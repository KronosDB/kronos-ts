import { describe, expect, it } from "bun:test"
import { createServer } from "nice-grpc"
import { connectToKronosDb } from "../connection.js"
import { connectToAxonServer } from "../../../axon-server/src/connection.js"
import { CommandServiceDefinition } from "../generated/command.js"

for (const [backend, connect] of [
  ["kronos", connectToKronosDb],
  ["axon", connectToAxonServer],
] as const) {
  describe(`${backend} channel recovery`, () => {
    it("replaces a nominally connected channel and coalesces concurrent reconnects", async () => {
      const server = createServer()
      server.add(CommandServiceDefinition, {
        async dispatch() {
          return {}
        },
        async *openStream() {},
      })
      const port = await server.listen("127.0.0.1:0")
      const connection = connect({
        host: "127.0.0.1",
        port,
        componentName: "recovery",
        keepAliveTimeoutMs: 1000,
      })
      let disconnects = 0,
        reconnects = 0
      connection.onDisconnect(() => {
        disconnects++
      })
      connection.onReconnect(() => {
        reconnects++
      })
      const original = connection.channel
      try {
        await Promise.all([connection.reconnect(), connection.reconnect()])
        expect(connection.channel).not.toBe(original)
        expect(connection.state).toBe("connected")
        expect(disconnects).toBe(1)
        expect(reconnects).toBe(1)
      } finally {
        connection.close()
        server.forceShutdown()
      }
      await expect(connection.reconnect()).rejects.toThrow(/permanently closed/)
    })
    it("reports failed recovery rather than success from lazy channel construction", async () => {
      const server = createServer()
      server.add(CommandServiceDefinition, {
        async dispatch() {
          return {}
        },
        async *openStream() {},
      })
      const port = await server.listen("127.0.0.1:0")
      server.forceShutdown()
      const connection = connect({
        host: "127.0.0.1",
        port,
        componentName: "recovery",
        keepAliveTimeoutMs: 20,
        maxReconnectAttempts: 1,
      })
      let recovered = false
      connection.onReconnect(() => {
        recovered = true
      })
      try {
        await expect(connection.reconnect()).rejects.toThrow(/Failed to reconnect/)
        expect(recovered).toBe(false)
        expect(connection.state).toBe("disconnected")
      } finally {
        connection.close()
      }
    })
  })
}

import { platformConnection as kronosPlatform } from "../platform-service.js"
import { platformConnection as axonPlatform } from "../../../axon-server/src/platform-service.js"
import { outboundStream } from "../outbound-stream.js"

for (const [backend, create] of [
  ["kronos", kronosPlatform],
  ["axon", axonPlatform],
] as const) {
  it(`${backend}: platform EOF triggers recovery and reopens monitoring`, async () => {
    const streams: ReturnType<typeof outboundStream<any>>[] = []
    let onReconnect!: () => void
    let reconnects = 0
    const connection: any = {
      config: {
        componentName: "qa",
        clientId: "qa",
        context: "default",
        token: "",
        reconnectIntervalMs: 1,
      },
      state: "connected",
      onReconnect(callback: () => void) {
        onReconnect = callback
      },
      async reconnect() {
        reconnects++
        onReconnect()
      },
      platform: {
        openStream(outbound: AsyncIterable<any>) {
          void (async () => {
            for await (const _ of outbound) {
              /* Drain client registration and heartbeat. */
            }
          })()
          const inbound = outboundStream<any>()
          streams.push(inbound)
          return inbound.iterable
        },
      },
    }
    const platform = create(connection, {
      heartbeatIntervalMs: 1000,
      processorsNotificationInitialDelayMs: 1000,
    })
    try {
      await platform.start()
      streams[0]!.close()
      await new Promise((r) => setTimeout(r, 15))
      expect(reconnects).toBe(1)
      expect(streams).toHaveLength(2)
      expect(platform.connected).toBe(true)
      platform.stop()
      streams[1]!.close()
      await new Promise((r) => setTimeout(r, 15))
      expect(reconnects).toBe(1)
      expect(platform.connected).toBe(false)
    } finally {
      platform.stop()
      for (const stream of streams) stream.close()
    }
  })
}

import { kronosDbConnection } from "../kronosdb.js"
import { axonServerConnection } from "../../../axon-server/src/connection.js"
import { shutdownLatch } from "../shutdown-latch.js"
import { jsonSerializer } from "@kronos-ts/core"

for (const backend of ["kronos", "axon"] as const) {
  it(`${backend}: shutdown expiry still closes the underlying transport`, async () => {
    const options = {
      componentName: "shutdown",
      serializer: jsonSerializer(),
      shutdownTimeoutMs: 10,
    }
    const connection =
      backend === "kronos" ? await kronosDbConnection(options) : await axonServerConnection(options)
    const latch = "shutdown" in connection ? connection.shutdown : shutdownLatch()
    if ("registerShutdownLatch" in connection) connection.registerShutdownLatch(latch)
    const activity = latch.registerActivity()
    try {
      await expect(connection.close()).rejects.toThrow(/shutdown.*timed out/i)
      expect(connection.connection.state).toBe("closed")
      expect(latch.activeCount).toBe(1)
    } finally {
      activity.end()
      await connection.close()
    }
  })
}

import { streamRecovery as kronosRecovery } from "../stream-recovery.js"
import { streamRecovery as axonRecovery } from "../../../axon-server/src/stream-recovery.js"

for (const [backend, create] of [
  ["kronos", kronosRecovery],
  ["axon", axonRecovery],
] as const) {
  it(`${backend}: async EOF recovery backs off, coalesces, caps attempts and stops`, async () => {
    let opened = 0
    const recovery = create(
      () => {
        opened++
        queueMicrotask(() => recovery.failed(new Error("EOF")))
      },
      () => true,
      { initialDelayMs: 2, maxDelayMs: 4, maxAttempts: 3 },
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
}
