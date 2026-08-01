import { describe, it, expect, afterEach } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { command, commandHandler, EventCriteria, event, } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos, type RunningApp } from "../index.js"
import { AppNotStartedError } from "../errors.js"

// Minimal in-line domain so .start() succeeds end-to-end.
const Ping = command({
  name: qn("acc", "Ping"),
  payload: z.object({ id: z.string() }),
})
const Pinged = event({
  name: qn("acc", "Pinged"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})
const PingState = state({
  name: "Ping",
  id: { id: z.string() },
  initial: () => ({ pinged: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: (on) => [on(Pinged, (s) => ({ ...s, pinged: true }))],
})
const pingHandler = commandHandler(Ping, async ({ payload: cmd }, ctx) => {
  await ctx.load(PingState, { id: cmd.id })
  ctx.append(Pinged, { id: cmd.id })
})

function makeApp() {
  return kronos({ quiet: true }).states(PingState).commands(pingHandler)
}

describe("App.commandGateway / App.queryGateway accessors", () => {
  let running: RunningApp | undefined

  afterEach(async () => {
    if (running) {
      await running.stop()
      running = undefined
    }
  })

  it("throws AppNotStartedError when commandGateway accessed BEFORE .start()", () => {
    const app = makeApp()
    expect(() => app.commandGateway).toThrow(AppNotStartedError)
  })

  it("throws AppNotStartedError when queryGateway accessed BEFORE .start()", () => {
    const app = makeApp()
    expect(() => app.queryGateway).toThrow(AppNotStartedError)
  })

  it("after .start() resolves, app.commandGateway === runningApp.commandGateway", async () => {
    const app = makeApp()
    running = await app.start()
    expect(app.commandGateway).toBe(running.commandGateway)
    expect(app.queryGateway).toBe(running.queryGateway)
  })

  it("inside an onStart('serve') hook, app.commandGateway returns the live gateway (does not throw)", async () => {
    let capturedFromHook: unknown = undefined
    let capturedQueryFromHook: unknown = undefined
    const app = makeApp()
    app.onStart("serve", () => {
      capturedFromHook = app.commandGateway
      capturedQueryFromHook = app.queryGateway
    })
    running = await app.start()
    expect(capturedFromHook).toBe(running.commandGateway)
    expect(capturedQueryFromHook).toBe(running.queryGateway)
  })

  it("inside an onStart('connect') hook, app.commandGateway throws AppNotStartedError (gateway not yet built)", async () => {
    let capturedError: unknown = undefined
    const app = makeApp()
    app.onStart("connect", () => {
      try {
        // touch the getter
        void app.commandGateway
      } catch (e) {
        capturedError = e
      }
    })
    running = await app.start()
    expect(capturedError).toBeInstanceOf(AppNotStartedError)
  })
})
