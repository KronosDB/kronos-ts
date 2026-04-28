import { describe, it, expect } from "bun:test"
import { AppImpl, AppAlreadyStartedError } from "../app.js"
import { createWarningChannel } from "../warnings.js"
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
