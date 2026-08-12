import { describe, expect, it } from "bun:test"
import { emptyMetadata, generateIdentifier, qn } from "@kronos-ts/common"
import type { QueryMessage } from "../message.js"
import { interceptingQueryBus } from "../intercepting-query-bus.js"
import { simpleQueryBus } from "../simple-query-bus.js"

function queryMsg(name: string, payload: unknown = {}): QueryMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

describe("InterceptingQueryBus", () => {
  it("handler interceptors can proceed with a replacement message", async () => {
    const inner = simpleQueryBus()
    const bus = interceptingQueryBus(inner)
    const seen: Array<Record<string, unknown>> = []

    bus.subscribe("test.Query", async (msg) => {
      seen.push(msg.metadata)
      return msg.payload
    })

    bus.registerHandlerInterceptor(async (msg, next) => {
      return next({
        ...msg,
        metadata: { ...msg.metadata, tenantId: "tenant-1" },
        payload: { transformed: true },
      })
    })

    bus.registerHandlerInterceptor(async (msg, next) => {
      seen.push(msg.metadata)
      return next()
    })

    const result = await bus.query(queryMsg("Query", { transformed: false }))

    expect(result).toEqual({ transformed: true })
    expect(seen).toEqual([
      { tenantId: "tenant-1" },
      { tenantId: "tenant-1" },
    ])
  })
})
