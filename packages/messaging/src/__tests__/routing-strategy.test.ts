import { describe, expect, it } from "bun:test"
import { qn, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import type { CommandMessage } from "../message.js"
import { metadataRoutingStrategy, payloadFieldRoutingStrategy } from "../routing-strategy.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function commandMsg(
  name: string,
  payload: unknown = {},
  metadata: Record<string, unknown> = {},
): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload,
    metadata,
    timestamp: Date.now(),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("metadataRoutingStrategy", () => {
  it("extracts routing key from metadata", () => {
    // given
    const strategy = metadataRoutingStrategy("aggregateId")
    const msg = commandMsg("CreateOrder", {}, { aggregateId: "order-123" })

    // when
    const key = strategy.getRoutingKey(msg)

    // then
    expect(key).toBe("order-123")
  })

  it("converts non-string values to string", () => {
    // given
    const strategy = metadataRoutingStrategy("tenantId")
    const msg = commandMsg("CreateOrder", {}, { tenantId: 42 })

    // when
    const key = strategy.getRoutingKey(msg)

    // then
    expect(key).toBe("42")
  })

  it("throws when metadata key is missing", () => {
    // given
    const strategy = metadataRoutingStrategy("aggregateId")
    const msg = commandMsg("CreateOrder")

    // when / then
    expect(() => strategy.getRoutingKey(msg)).toThrow("No routing key found in metadata")
  })
})

describe("payloadFieldRoutingStrategy", () => {
  it("extracts routing key from payload field", () => {
    // given
    const strategy = payloadFieldRoutingStrategy("orderId")
    const msg = commandMsg("CreateOrder", { orderId: "order-456", name: "Widget" })

    // when
    const key = strategy.getRoutingKey(msg)

    // then
    expect(key).toBe("order-456")
  })

  it("converts non-string values to string", () => {
    // given
    const strategy = payloadFieldRoutingStrategy("quantity")
    const msg = commandMsg("UpdateStock", { quantity: 100 })

    // when
    const key = strategy.getRoutingKey(msg)

    // then
    expect(key).toBe("100")
  })

  it("throws when payload field is missing", () => {
    // given
    const strategy = payloadFieldRoutingStrategy("orderId")
    const msg = commandMsg("CreateOrder", { name: "Widget" })

    // when / then
    expect(() => strategy.getRoutingKey(msg)).toThrow("No routing key found in payload field")
  })

  it("throws when payload field is null", () => {
    // given
    const strategy = payloadFieldRoutingStrategy("orderId")
    const msg = commandMsg("CreateOrder", { orderId: null })

    // when / then
    expect(() => strategy.getRoutingKey(msg)).toThrow("No routing key found in payload field")
  })
})
