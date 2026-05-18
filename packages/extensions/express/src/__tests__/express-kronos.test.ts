import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event, on } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { append, load } from "@kronos-ts/eventsourcing"
import { kronos } from "@kronos-ts/core"
import { getKronos, withExpress, type KronosLocals } from "../express-kronos.js"

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
// Mock express
// ============================================================================
function makeMockExpress() {
  const closeCalls: any[] = []
  const server = {
    close(cb?: (err?: any) => void) {
      closeCalls.push(true)
      if (cb) cb()
    },
  }
  let listenedPort: number | undefined = undefined
  return {
    locals: {} as Record<string, any>,
    listen(port: number) {
      listenedPort = port
      return server
    },
    get listenedPort() { return listenedPort },
    get server() { return server },
    get closeCalls() { return closeCalls },
  }
}

// ============================================================================
// Tests
// ============================================================================

describe("withExpress (native Extension shape)", () => {
  it("returns a function with arity 1 (Extension shape)", () => {
    const mock = makeMockExpress()
    const ext = withExpress(mock as any, { port: 3000 })
    expect(typeof ext).toBe("function")
    expect(ext.length).toBe(1)
  })

  it("after kronos().use(withExpress(...)).start(), gateways are wired into app.locals.kronos and listen() was called", async () => {
    const mock = makeMockExpress()
    const app = await kronos({ quiet: true })
      .states(PingState)
      .commands(pingHandler)
      .use(withExpress(mock as any, { port: 4567 }))
      .start()

    expect(mock.locals.kronos).toBeDefined()
    expect(mock.locals.kronos.commandGateway).toBeDefined()
    expect(mock.locals.kronos.queryGateway).toBeDefined()
    expect(mock.listenedPort).toBe(4567)

    await app.stop()
    expect(mock.closeCalls.length).toBe(1)
  })

  it("uses default port 3000 when options.port is omitted", async () => {
    const mock = makeMockExpress()
    const app = await kronos({ quiet: true })
      .states(PingState)
      .commands(pingHandler)
      .use(withExpress(mock as any))
      .start()
    expect(mock.listenedPort).toBe(3000)
    await app.stop()
  })
})

describe("getKronos", () => {
  it("returns kronos locals from request", () => {
    const mockLocals: KronosLocals = {
      commandGateway: { send: async () => undefined } as any,
      queryGateway: { query: async () => undefined } as any,
    }
    const req = { app: { locals: { kronos: mockLocals } } }
    const k = getKronos(req)
    expect(k).toBe(mockLocals)
    expect(k.commandGateway).toBeDefined()
    expect(k.queryGateway).toBeDefined()
  })

  it("throws when kronos is not initialized", () => {
    const req = { app: { locals: {} } }
    expect(() => getKronos(req)).toThrow("Kronos not initialized")
  })
})
