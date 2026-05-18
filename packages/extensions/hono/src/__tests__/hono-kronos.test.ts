import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event, on } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { append, load } from "@kronos-ts/eventsourcing"
import { kronos } from "@kronos-ts/core"
import { getKronos, KRONOS_CONTEXT_KEY, withHono, type KronosContext } from "../hono-kronos.js"

// ============================================================================
// Minimal domain
// ============================================================================
const Ping = command({ name: qn("test", "Ping"), payload: z.object({ id: z.string() }) })
const Pinged = event({
  name: qn("test", "Pinged"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})
const PingState = state({
  name: "Ping",
  id: { id: z.string() },
  initial: () => ({ pinged: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(Pinged, (s) => ({ ...s, pinged: true }))],
})
const pingHandler = commandHandler(Ping, async (cmd, _md) => {
  await load(PingState, { id: cmd.id })
  append(Pinged, { id: cmd.id })
})

// ============================================================================
// Mock hono
// ============================================================================
function makeMockHono() {
  const middlewares: Array<{ path: string; mw: any }> = []
  return {
    use(path: string, mw: any) {
      middlewares.push({ path, mw })
    },
    get middlewares() { return middlewares },
  }
}

// ============================================================================
// Tests
// ============================================================================

describe("withHono (native Extension shape)", () => {
  it("returns a function with arity 1 (Extension shape)", () => {
    const mock = makeMockHono()
    const ext = withHono(mock as any)
    expect(typeof ext).toBe("function")
    expect(ext.length).toBe(1)
  })

  it("after kronos().use(withHono(...)).start(), middleware is registered and sets kronos ctx via c.set", async () => {
    const mock = makeMockHono()
    const app = await kronos({ quiet: true })
      .states(PingState)
      .commands(pingHandler)
      .use(withHono(mock as any))
      .start()

    expect(mock.middlewares.length).toBe(1)
    expect(mock.middlewares[0]!.path).toBe("*")
    const middleware = mock.middlewares[0]!.mw

    // Run the middleware against a fake context — it should call c.set with the ctx and call next()
    const stored: Record<string, any> = {}
    let nextCalled = false
    const fakeC = {
      set(key: string, val: any) { stored[key] = val },
    }
    await middleware(fakeC, async () => { nextCalled = true })

    expect(stored[KRONOS_CONTEXT_KEY]).toBeDefined()
    expect(stored[KRONOS_CONTEXT_KEY].commandGateway).toBeDefined()
    expect(stored[KRONOS_CONTEXT_KEY].queryGateway).toBeDefined()
    expect(nextCalled).toBe(true)

    await app.stop()
  })
})

describe("getKronos", () => {
  it("returns kronos context from Hono context", () => {
    const mockContext: KronosContext = {
      commandGateway: { send: async () => undefined } as any,
      queryGateway: { query: async () => undefined } as any,
    }
    const c = {
      get: (key: string) => key === KRONOS_CONTEXT_KEY ? mockContext : undefined,
    }
    const k = getKronos(c)
    expect(k).toBe(mockContext)
    expect(k.commandGateway).toBeDefined()
    expect(k.queryGateway).toBeDefined()
  })

  it("throws when kronos is not initialized", () => {
    const c = { get: () => undefined }
    expect(() => getKronos(c)).toThrow("Kronos not initialized")
  })
})
