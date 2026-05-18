import { describe, it, expect, afterEach } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { command, event, on, commandHandler, EventCriteria } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"
import { AppImpl, AppAlreadyStartedError, type RunningApp } from "../app.js"
import { createWarningChannel } from "../warnings.js"
import { kronos } from "../kronos.js"
import { Defaults } from "../defaults-handles.js"
import type {
  CommandMessage,
  QueryMessage,
  EventMessage,
  DispatchInterceptor,
  HandlerInterceptor,
} from "@kronos-ts/messaging"

function makeApp() {
  const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
  return app
}

// ─── Minimal domain (mirrors kronos.e2e.test.ts) ─────────────────────────────

const CreateThing = command({
  name: qn("phase6", "CreateThing"),
  payload: z.object({ id: z.string() }),
})

const ThingCreated = event({
  name: qn("phase6", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})

const Thing = state({
  name: "Thing6",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(ThingCreated, (s) => ({ ...s, created: true }))],
})

const createThingHandler = commandHandler(CreateThing, async (cmd, _md) => {
  await load(Thing, { id: cmd.id })
  append(ThingCreated, { id: cmd.id })
})

// ─── Task 1: Interceptor accumulator methods ─────────────────────────────────

describe("app.commandDispatchInterceptor()", () => {
  it("Test 1: returns App (fluent) and pushes fn into _state.commandDispatchInterceptors", () => {
    const app = makeApp()
    const fn: DispatchInterceptor<CommandMessage> = (m) => m
    const result = app.commandDispatchInterceptor(fn)
    expect(result).toBe(app)
    expect(app._state.commandDispatchInterceptors).toHaveLength(1)
    expect(app._state.commandDispatchInterceptors[0]).toBe(fn)
  })

  it("Test 4: multiple calls accumulate in registration order", () => {
    const app = makeApp()
    const a: DispatchInterceptor<CommandMessage> = (m) => m
    const b: DispatchInterceptor<CommandMessage> = (m) => m
    app.commandDispatchInterceptor(a).commandDispatchInterceptor(b)
    expect(app._state.commandDispatchInterceptors).toEqual([a, b])
  })
})

describe("app.queryDispatchInterceptor()", () => {
  it("Test 2a: returns App (fluent) and pushes fn into _state.queryDispatchInterceptors", () => {
    const app = makeApp()
    const fn: DispatchInterceptor<QueryMessage> = (m) => m
    const result = app.queryDispatchInterceptor(fn)
    expect(result).toBe(app)
    expect(app._state.queryDispatchInterceptors).toHaveLength(1)
    expect(app._state.queryDispatchInterceptors[0]).toBe(fn)
  })
})

describe("app.eventDispatchInterceptor()", () => {
  it("Test 2b: returns App (fluent) and pushes fn into _state.eventDispatchInterceptors", () => {
    const app = makeApp()
    const fn: DispatchInterceptor<EventMessage> = (m) => m
    const result = app.eventDispatchInterceptor(fn)
    expect(result).toBe(app)
    expect(app._state.eventDispatchInterceptors).toHaveLength(1)
    expect(app._state.eventDispatchInterceptors[0]).toBe(fn)
  })
})

describe("app.handlerInterceptor()", () => {
  it("Test 2c: returns App (fluent) and pushes fn into _state.handlerInterceptors", () => {
    const app = makeApp()
    const fn: HandlerInterceptor = async (_m, next) => next()
    const result = app.handlerInterceptor(fn)
    expect(result).toBe(app)
    expect(app._state.handlerInterceptors).toHaveLength(1)
    expect(app._state.handlerInterceptors[0]).toBe(fn)
  })
})

describe("interceptor accumulators — post-start guard", () => {
  it("Test 3: all four methods throw AppAlreadyStartedError after markStarted()", () => {
    const app = makeApp()
    app.markStarted()
    const cmdFn: DispatchInterceptor<CommandMessage> = (m) => m
    const queryFn: DispatchInterceptor<QueryMessage> = (m) => m
    const eventFn: DispatchInterceptor<EventMessage> = (m) => m
    const handlerFn: HandlerInterceptor = async (_m, next) => next()
    expect(() => app.commandDispatchInterceptor(cmdFn)).toThrow(AppAlreadyStartedError)
    expect(() => app.queryDispatchInterceptor(queryFn)).toThrow(AppAlreadyStartedError)
    expect(() => app.eventDispatchInterceptor(eventFn)).toThrow(AppAlreadyStartedError)
    expect(() => app.handlerInterceptor(handlerFn)).toThrow(AppAlreadyStartedError)
  })
})

describe("interceptor accumulator — compile-time type safety", () => {
  it("Test 5: type-rejects cross-bus dispatch interceptor (compile-time)", () => {
    const app = makeApp()
    const queryFn: DispatchInterceptor<QueryMessage> = (m) => m
    // @ts-expect-error — DispatchInterceptor<QueryMessage> not assignable to DispatchInterceptor<CommandMessage>
    app.commandDispatchInterceptor(queryFn)
  })
})

// ─── Task 2: Framework intercepting defaults + defaults.ts pure simple buses ──

describe("kronos() — commandBus default is a pure simple bus (no intercepting wrap baked in)", () => {
  it("Test 1: commandBus factory returns a bus without registerDispatchInterceptor (simple bus, not intercepting)", () => {
    const app = kronos({ quiet: true }) as any
    const cmdEntry = app.getRegistry().getEntry("commandBus")
    const cmdInstance = cmdEntry.factory({} as any)
    expect("registerDispatchInterceptor" in cmdInstance).toBe(false)
  })
})

describe("kronos() — three framework-default decorator registrations", () => {
  it("Test 2: _state.decoratorRegistrations contains exactly 3 frameworkDefault entries (one per bus)", () => {
    const app = kronos({ quiet: true }) as any
    const fwDefaults = app._state.decoratorRegistrations.filter((r: any) => r.frameworkDefault)
    expect(fwDefaults).toHaveLength(3)
    expect(fwDefaults.map((r: any) => r.handle.__slot).sort()).toEqual(["commandBus", "eventBus", "queryBus"])
  })
})

describe("kronos() + start() — framework intercepting default wraps the bus", () => {
  let running: RunningApp | undefined
  afterEach(async () => {
    if (running) {
      await running.stop()
      running = undefined
    }
  })

  it("Test 3: resolved commandBus is the intercepting bus (has registerDispatchInterceptor)", async () => {
    let capturedInner: any
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
    app.decorate("commandBus", (inner) => {
      capturedInner = inner
      return inner
    })
    running = await app.start()
    // The user decorator receives the intercepting bus as `inner` (framework default is innermost)
    expect(capturedInner).toBeDefined()
    expect(typeof capturedInner.dispatch).toBe("function")
    expect("registerDispatchInterceptor" in capturedInner).toBe(true)
  })

  it("Test 4: commandDispatchInterceptor(fn) registered before start — fn is invoked when a command dispatches", async () => {
    const witness: CommandMessage[] = []
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
      .commandDispatchInterceptor((m) => { witness.push(m as CommandMessage); return m })
    running = await app.start()
    await running.commandGateway.send(CreateThing, { id: "t-2" }, emptyMetadata())
    expect(witness).toHaveLength(1)
  })

  it("Test 5: removeDecorator(Defaults.commandBus.intercepting) removes the framework default — bare simple bus", async () => {
    let capturedInner: any
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
      .removeDecorator(Defaults.commandBus.intercepting)
    app.decorate("commandBus", (inner) => {
      capturedInner = inner
      return inner
    })
    running = await app.start()
    // Without the intercepting default, the user decorator receives the bare simple bus
    expect("registerDispatchInterceptor" in capturedInner).toBe(false)
  })

  it("Test 6a: removeDecorator(Defaults.queryBus.intercepting) removes queryBus intercepting default", async () => {
    let capturedInner: any
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
      .removeDecorator(Defaults.queryBus.intercepting)
    app.decorate("queryBus", (inner) => {
      capturedInner = inner
      return inner
    })
    running = await app.start()
    expect("registerDispatchInterceptor" in capturedInner).toBe(false)
  })

  it("Test 6b: removeDecorator(Defaults.eventBus.intercepting) removes eventBus intercepting default", async () => {
    let capturedInner: any
    const app = kronos({ quiet: true })
      .states(Thing)
      .commands(createThingHandler)
      .removeDecorator(Defaults.eventBus.intercepting)
    app.decorate("eventBus", (inner) => {
      capturedInner = inner
      return inner
    })
    running = await app.start()
    // Without the intercepting default, the captured inner should not have registerDispatchInterceptor
    // (the bare eventBus default is the eventStore cast — no intercepting layer)
    expect("registerDispatchInterceptor" in capturedInner).toBe(false)
  })
})
