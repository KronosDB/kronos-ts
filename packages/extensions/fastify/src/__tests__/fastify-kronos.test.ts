import { describe, expect, it } from "bun:test"
import { kronosPlugin } from "../fastify-kronos.js"
import { EventSourcingConfigurer } from "@kronos-ts/eventsourcing"

describe("Fastify Kronos plugin", () => {
  it("decorates fastify instance with kronos gateways", async () => {
    // given
    const decorations: Record<string, any> = {}
    const hooks: Record<string, any> = {}
    const mockFastify = {
      decorate(name: string, value: any) { decorations[name] = value },
      addHook(name: string, fn: any) { hooks[name] = fn },
    }

    const configurer = EventSourcingConfigurer.create()

    // when
    await kronosPlugin(mockFastify, configurer)

    // then
    expect(decorations.kronos).toBeDefined()
    expect(decorations.kronos.commandGateway).toBeDefined()
    expect(decorations.kronos.queryGateway).toBeDefined()
    expect(decorations.kronos.application).toBeDefined()
    expect(hooks.onClose).toBeFunction()
  })
})
