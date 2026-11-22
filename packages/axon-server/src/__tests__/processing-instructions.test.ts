/**
 * A command's `metadata.processingInstructions` on the Axon Server data path.
 * The assertions are on the proto request handed to the gRPC client.
 */
import { describe, expect, it } from "bun:test"
import { localCommandBus, qn, unitOfWork, type SerializedObject, type Serializer } from "@kronos-ts/core"
import { axonServerCommandBus } from "../axon-server.js"
import { shutdownLatch } from "../shutdown-latch.js"
import type { AxonServerBusSource, AxonServerConnection } from "../connection.js"

const jsonSerializer: Serializer = {
  serialize(value, type, revision = ""): SerializedObject {
    return { type, revision, data: new TextEncoder().encode(JSON.stringify(value)) }
  },
  deserialize<T>({ data }: SerializedObject): T {
    return JSON.parse(new TextDecoder().decode(data)) as T
  },
  canConvert() {
    return true
  },
}

/** Captures the command requests that reach the wire. */
function capturingSource() {
  const requests: any[] = []
  const connection = {
    commands: {
      openStream() {
        return (async function* () {})()
      },
      async dispatch(request: any) {
        requests.push(request)
        return { errorCode: "", payload: jsonSerializer.serialize(null, "null", "") }
      },
    },
    config: { context: "default", componentName: "instructions-test", clientId: "instructions-client" },
    state: "connected",
    onReconnect() {},
    onDisconnect() {},
  } as unknown as AxonServerConnection
  const source: AxonServerBusSource = { connection, serializer: jsonSerializer, shutdown: shutdownLatch() }
  return { source, requests }
}

describe("Axon Server command bus — processing instructions", () => {
  it("sends the metadata's routing key, priority and timeout as processing instructions", async () => {
    const { source, requests } = capturingSource()
    const bus = axonServerCommandBus(localCommandBus(unitOfWork), source)

    await bus.dispatch({
      kind: "command",
      identifier: "c-1",
      name: qn("instructions", "Enroll"),
      payload: { courseId: "cs-101" },
      metadata: { processingInstructions: { routingKey: "cs-101", priority: 3, timeoutMs: 900 } },
      timestamp: 1,
    })

    expect(requests).toHaveLength(1)
    expect(requests[0]!.processingInstructions).toEqual([
      { key: 0, value: { textValue: "cs-101" } },
      { key: 1, value: { numberValue: 3n } },
      { key: 2, value: { numberValue: 900n } },
    ])
  })

  it("sends no processing instructions for a command that carries none", async () => {
    const { source, requests } = capturingSource()
    const bus = axonServerCommandBus(localCommandBus(unitOfWork), source)

    await bus.dispatch({
      kind: "command",
      identifier: "c-2",
      name: qn("instructions", "Enroll"),
      payload: { courseId: "cs-101" },
      metadata: {},
      timestamp: 1,
    })

    expect(requests[0]!.processingInstructions).toEqual([])
  })
})
