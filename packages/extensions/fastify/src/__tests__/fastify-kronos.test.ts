import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event, on } from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { append, load } from "@kronos-ts/eventsourcing"
import { kronos } from "@kronos-ts/core"
import { withFastify, type KronosDecorator } from "../fastify-kronos.js"

// ============================================================================
// Minimal domain
// ============================================================================
const Ping = command({ name: qn("test", "Ping"), payload: z.object({ id: z.string() }) })
const Pinged = event({
  name: qn("test", "Pinged"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})
const PingEntity = eventSourcedEntity({
  name: "Ping",
  id: { id: z.string() },
  initial: () => ({ pinged: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(Pinged, (s) => ({ ...s, pinged: true }))],
})
const pingHandler = commandHandler(Ping, async (cmd, _md) => {
  await load(PingEntity, { id: cmd.id })
  append(Pinged, { id: cmd.id })
})

// ============================================================================
// Mock fastify
// ============================================================================
function makeMockFastify() {
  const decorations: Record<string, any> = {}
  let closeCalls = 0
  return {
    decorate(name: string, val: any) { decorations[name] = val },
    addHook(_name: string, _fn: any) { /* no-op */ },
    async close() { closeCalls++ },
    get decorations() { return decorations },
    get closeCalls() { return closeCalls },
  }
}

// ============================================================================
// Tests
// ============================================================================

describe("withFastify (native Extension shape)", () => {
  it("returns a function with arity 1 (Extension shape)", () => {
    const mock = makeMockFastify()
    const ext = withFastify(mock as any)
    expect(typeof ext).toBe("function")
    expect(ext.length).toBe(1)
  })

  it("after kronos().use(withFastify(...)).start(), fastify.decorate('kronos', { gateways }) was called", async () => {
    const mock = makeMockFastify()
    const app = await kronos({ quiet: true })
      .entities(PingEntity)
      .commands(pingHandler)
      .use(withFastify(mock as any))
      .start()

    const decorator = mock.decorations.kronos as KronosDecorator
    expect(decorator).toBeDefined()
    expect(decorator.commandGateway).toBeDefined()
    expect(decorator.queryGateway).toBeDefined()

    await app.stop()
    expect(mock.closeCalls).toBe(1)
  })
})
