import assert from "node:assert/strict"
import { createServer } from "node:http"
import { test } from "node:test"
import { inMemoryEventStore, kronos, localCommandBus, localQueryBus, send, unitOfWork } from "@kronos-ts/core"
import { otlpExporter, otlpHandler } from "@kronos-ts/otlp"
import { AddTool, addTool } from "./toolstore.mjs"

test("otlpHandler records a span per handling and the exporter posts it over HTTP", async () => {
  const received = []
  const server = createServer((request, response) => {
    let body = ""
    request.on("data", (chunk) => (body += chunk))
    request.on("end", () => {
      received.push({ path: request.url, body })
      response.writeHead(200, { "content-type": "application/json" }).end("{}")
    })
  })
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve))
  const exporter = otlpExporter({ endpoint: `http://127.0.0.1:${server.address().port}`, serviceName: "node-tests", metrics: false })

  const eventStore = inMemoryEventStore()
  const commandBus = localCommandBus(unitOfWork)
  const queryBus = localQueryBus(unitOfWork)
  const app = kronos({
    commandHandlers: [{ ...addTool, handler: otlpHandler(addTool.handler, exporter), eventStore, commandBus, queryBus }],
  })
  try {
    await send(commandBus, AddTool, { toolId: "t-1", name: "Cordless drill" })
    await exporter.flush()
    const traces = received.filter((request) => request.path === "/v1/traces")
    assert.equal(traces.length, 1)
    assert.ok(traces[0].body.includes("AddTool"))
  } finally {
    await app.stop()
    await exporter.close()
    server.close()
  }
})
