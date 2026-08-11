import { describe, expect, it } from "bun:test"
import { createRabbitMqTopologyNames } from "../topology.js"

const identity = { serviceName: "karma-main", instanceId: "i-1" }

describe("topology consumer groups", () => {
  const names = createRabbitMqTopologyNames(identity, {})

  it("keys the queue on the module group when one is given", () => {
    expect(names.commandQueue("billing.ChargeBill", "billing")).toBe("kronos.commands.billing.billing.ChargeBill")
    expect(names.commandQueue("ordering.PlaceOrder", "ordering")).toBe("kronos.commands.ordering.ordering.PlaceOrder")
  })

  it("falls back to the service identity when no group is given", () => {
    expect(names.commandQueue("billing.ChargeBill")).toBe("kronos.commands.karma-main.billing.ChargeBill")
  })

  it("gives two modules in ONE service distinct queues for the same command", () => {
    // This is the property that makes 1-app-N-modules safe: without it both
    // modules would consume the same queue under the single service identity.
    expect(names.commandQueue("shared.Ping", "billing")).not.toBe(names.commandQueue("shared.Ping", "ordering"))
  })

  it("gives the SAME module the same queue regardless of hosting service", () => {
    // Module identity is stable across relocation — the property N-apps buys
    // today by making serviceName === module name.
    const elsewhere = createRabbitMqTopologyNames({ serviceName: "karma-billing-svc", instanceId: "i-2" }, {})
    expect(elsewhere.commandQueue("billing.ChargeBill", "billing")).toBe(
      names.commandQueue("billing.ChargeBill", "billing"),
    )
  })
})
