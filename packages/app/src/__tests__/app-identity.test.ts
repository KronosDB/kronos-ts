import { describe, expect, it } from "bun:test"
import { kronos } from "../kronos.js"

describe("app identity", () => {
  it("uses configured serviceName and instanceId", () => {
    const app = kronos({ serviceName: "faculty-service", instanceId: "pod-1", quiet: true })

    expect(app.identity).toEqual({
      serviceName: "faculty-service",
      instanceId: "pod-1",
    })
  })

  it("provides defaults when identity is not configured", () => {
    const app = kronos({ quiet: true })

    expect(app.identity.serviceName).toBe("kronos-app")
    expect(app.identity.instanceId.length).toBeGreaterThan(0)
  })
})
