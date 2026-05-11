/**
 * Plan 09-01 Task 2 — handlerEnhancer accumulator + start-time wiring.
 *
 * Asserts:
 *  T1. accumulator stores defs in registration order; method returns App.
 *  T2. composed enhancer wraps command handlers (TRACED: marker visible).
 *  T3. composed enhancer wraps query handlers (RESEARCH Open Question #4).
 *  T4. composed enhancer wraps tracking/subscribing event processor handlers.
 *  T5. mutating handlerEnhancer after .start() throws AppAlreadyStartedError.
 */
import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  queryHandlers,
  eventHandler,
  query,
  EventCriteria,
  subscribingProcessor,
  type HandlerEnhancerDefinition,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { append, load } from "@kronos-ts/eventsourcing"
import { kronos } from "../kronos.js"
import { AppImpl, AppAlreadyStartedError } from "../app.js"
import { registerInMemoryDefaults } from "../defaults.js"
import { createWarningChannel } from "../warnings.js"

// ─── Minimal domain reused across tests ──────────────────────────────────────

const Ping = command({
  name: qn("enhancer", "Ping"),
  payload: z.object({ id: z.string() }),
})
const Pinged = event({
  name: qn("enhancer", "Pinged"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})
const Echo = query({
  name: qn("enhancer", "Echo"),
  payload: z.object({ id: z.string() }),
  result: z.string(),
})
const PingEntity = eventSourcedEntity({
  name: "EnhancerPing",
  id: { id: z.string() },
  initial: () => ({ pinged: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(Pinged, (s) => ({ ...s, pinged: true }))],
})
const pingHandler = commandHandler(Ping, async (cmd) => {
  await load(PingEntity, { id: cmd.id })
  append(Pinged, { id: cmd.id })
  return `OK:${cmd.id}` as const
})

/**
 * Test enhancer that prepends "TRACED:" to whatever the wrapped handler returns
 * AND records every wrapped descriptor onto a shared array. Side-effect-free
 * for handlers that return undefined (just records metadata).
 */
function tracingEnhancer(
  marker: string,
  recorded: Array<{ messageType: string; messageName: string; handlerGroup: string; marker: string }>,
): HandlerEnhancerDefinition {
  return {
    wrapHandler<T extends (...args: any[]) => any>(handler: T, metadata): T {
      recorded.push({ ...metadata, marker })
      const wrapped = (async (...args: any[]) => {
        const result = await handler(...args)
        return typeof result === "string" ? `${marker}${result}` : result
      }) as unknown as T
      return wrapped
    },
  }
}

// ─── T1 — accumulator stores defs in order; chainable ───────────────────────

describe("App.handlerEnhancer — accumulator (Plan 09-01 Task 2)", () => {
  it("stores handler enhancers in registration order and returns App for chaining", () => {
    const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
    registerInMemoryDefaults(app)
    const def1: HandlerEnhancerDefinition = { wrapHandler: (h) => h }
    const def2: HandlerEnhancerDefinition = { wrapHandler: (h) => h }
    const back = app.handlerEnhancer(def1).handlerEnhancer(def2)
    expect(back).toBe(app)
    expect(app._state.handlerEnhancers).toEqual([def1, def2])
  })
})

// ─── T2 — command handler tracing ───────────────────────────────────────────

describe("handlerEnhancer wires through command handler registration", () => {
  it("composedEnhancer wraps command handlers — TRACED: marker visible on return", async () => {
    const recorded: Array<any> = []
    const app = kronos({ quiet: true })
      .entities(PingEntity)
      .commands(pingHandler)
      .handlerEnhancer(tracingEnhancer("TRACED:", recorded))
    const running = await app.start()
    try {
      const result = await running.commandGateway.send(
        Ping,
        { id: "p1" },
        emptyMetadata(),
      )
      expect(result).toBe("TRACED:OK:p1")
      // The enhancer was applied to the command handler at registration time.
      const wrapped = recorded.find((r) => r.messageType === "command")
      expect(wrapped).toBeDefined()
      expect(wrapped.handlerGroup).toBe("commands")
      expect(wrapped.messageName).toContain("Ping")
    } finally {
      await running.stop()
    }
  })
})

// ─── T3 — query handler tracing (RESEARCH Open Question #4) ─────────────────

describe("handlerEnhancer wires through query handler registration", () => {
  it("composedEnhancer wraps query handlers — TRACED: marker visible on return", async () => {
    const recorded: Array<any> = []
    const echoHandlers = queryHandlers({
      name: "echo-queries",
      handlers: [on(Echo, async (payload) => `OK:${payload.id}`)],
    })
    const app = kronos({ quiet: true })
      .queries(echoHandlers)
      .handlerEnhancer(tracingEnhancer("TRACED:", recorded))
    const running = await app.start()
    try {
      const result = await running.queryGateway.query(
        Echo,
        { id: "q1" },
        emptyMetadata(),
      )
      expect(result).toBe("TRACED:OK:q1")
      const wrapped = recorded.find((r) => r.messageType === "query")
      expect(wrapped).toBeDefined()
      expect(wrapped.handlerGroup).toBe("queries")
      expect(wrapped.messageName).toContain("Echo")
    } finally {
      await running.stop()
    }
  })
})

// ─── T4 — event/processor handler tracing ───────────────────────────────────

describe("handlerEnhancer wires through subscribing event processor", () => {
  it("composedEnhancer wraps subscribing-processor event handlers", async () => {
    const recorded: Array<any> = []
    let received = ""
    const onPinged = eventHandler(Pinged, async (payload) => {
      received = payload.id
    })
    const app = kronos({ quiet: true })
      .entities(PingEntity)
      .commands(pingHandler)
      .processors(
        subscribingProcessor("ping-projection").eventHandlers(onPinged).build(),
      )
      .handlerEnhancer(tracingEnhancer("TRACED:", recorded))
    const running = await app.start()
    try {
      await running.commandGateway.send(Ping, { id: "e1" }, emptyMetadata())
      // Subscribing processor delivers events synchronously on the publisher's stack.
      expect(received).toBe("e1")
      const eventWrap = recorded.find((r) => r.messageType === "event")
      expect(eventWrap).toBeDefined()
      expect(eventWrap.handlerGroup).toBe("ping-projection")
      expect(eventWrap.messageName).toContain("Pinged")
    } finally {
      await running.stop()
    }
  })
})

// ─── T5 — guard after start ─────────────────────────────────────────────────

describe("handlerEnhancer guard after .start()", () => {
  it("throws AppAlreadyStartedError if .handlerEnhancer() called after start()", async () => {
    const app = kronos({ quiet: true })
      .handlerEnhancer({ wrapHandler: (h) => h })
    const running = await app.start()
    try {
      expect(() => app.handlerEnhancer({ wrapHandler: (h) => h })).toThrow(
        AppAlreadyStartedError,
      )
    } finally {
      await running.stop()
    }
  })
})
