import { describe, it, expect } from "bun:test"
import { makeFrameworkHandle } from "../decorator.js"
import { Defaults } from "../defaults-handles.js"
import { UnknownDecoratorHandleError } from "../errors.js"
import { AppImpl, AppAlreadyStartedError } from "../app.js"
import { registerInMemoryDefaults } from "../defaults.js"
import { createWarningChannel } from "../warnings.js"
import type { CommandBus } from "@kronos-ts/messaging"

function makeApp() {
  const app = new AppImpl({ warningChannel: createWarningChannel({ quiet: true }) })
  registerInMemoryDefaults(app)
  return app
}

// ─── Task 1: Decorator types + Defaults const + UnknownDecoratorHandleError ───

describe("DecoratorHandle — identity", () => {
  it("Test 1: two makeFrameworkHandle calls produce distinct __id symbols", () => {
    const h1 = makeFrameworkHandle("commandBus", "x")
    const h2 = makeFrameworkHandle("commandBus", "x")
    expect(h1.__id).not.toBe(h2.__id)
  })

  it("Test 2: Defaults bus handles have correct __slot types", () => {
    expect(Defaults.commandBus.intercepting.__slot).toBe("commandBus")
    expect(Defaults.queryBus.intercepting.__slot).toBe("queryBus")
    expect(Defaults.eventBus.intercepting.__slot).toBe("eventBus")
  })

  it("Test 3: Defaults and nested objects are frozen", () => {
    expect(Object.isFrozen(Defaults)).toBe(true)
    expect(Object.isFrozen(Defaults.commandBus)).toBe(true)
    expect(Object.isFrozen(Defaults.queryBus)).toBe(true)
    expect(Object.isFrozen(Defaults.eventBus)).toBe(true)
  })

  it("Test 4: Defaults.commandBus.intercepting.__name === 'intercepting'", () => {
    expect(Defaults.commandBus.intercepting.__name).toBe("intercepting")
    expect(Defaults.queryBus.intercepting.__name).toBe("intercepting")
    expect(Defaults.eventBus.intercepting.__name).toBe("intercepting")
  })
})

describe("UnknownDecoratorHandleError", () => {
  it("Test 5: error has correct name, slot, handleName, and message", () => {
    const handle = makeFrameworkHandle("commandBus", "intercepting")
    const error = new UnknownDecoratorHandleError(handle)
    expect(error.name).toBe("UnknownDecoratorHandleError")
    expect(error.slot).toBe("commandBus")
    expect(error.handleName).toBe("intercepting")
    expect(error.message).toContain("intercepting")
    expect(error.message).toContain("commandBus")
    expect(error).toBeInstanceOf(Error)
  })
})

// ─── Task 2: app.decorate() / app.removeDecorator() behavior tests ────────────

describe("app.decorate() — handle identity and registration", () => {
  it("Test 1: decorate returns a handle with __slot === slot name", () => {
    const app = makeApp()
    const handle = app.decorate("commandBus", (inner) => inner)
    expect(handle.__slot).toBe("commandBus")
  })

  it("Test 2: two decorate calls return distinct handles; both entries in _state", () => {
    const app = makeApp()
    const h1 = app.decorate("commandBus", (inner) => inner)
    const h2 = app.decorate("commandBus", (inner) => inner)
    expect(h1.__id).not.toBe(h2.__id)
    expect(app._state.decoratorRegistrations).toHaveLength(2)
  })

  it("Test 3: removeDecorator returns app (fluent) and removes entry", () => {
    const app = makeApp()
    const handle = app.decorate("commandBus", (inner) => inner)
    expect(app._state.decoratorRegistrations).toHaveLength(1)
    const result = app.removeDecorator(handle)
    expect(result).toBe(app)
    expect(app._state.decoratorRegistrations).toHaveLength(0)
  })

  it("Test 4: removeDecorator with unknown handle throws UnknownDecoratorHandleError", () => {
    const app = makeApp()
    const unknownHandle = makeFrameworkHandle("commandBus", "nonexistent")
    expect(() => app.removeDecorator(unknownHandle)).toThrow(UnknownDecoratorHandleError)
  })

  it("Test 5: cross-slot removal is a TYPE error (compile-time check via @ts-expect-error)", () => {
    const app = makeApp()
    const cmdHandle = app.decorate("commandBus", (i) => i)
    // @ts-expect-error — DecoratorHandle<"commandBus"> is not assignable to DecoratorHandle<"queryBus">
    const wrongCall = (a: typeof app) => a.removeDecorator<"queryBus">(cmdHandle)
    void wrongCall
    // If this compiles without TS error the @ts-expect-error would fail — that's the compile check.
    expect(true).toBe(true)
  })

  it("Test 6: after markStarted(), decorate and removeDecorator throw AppAlreadyStartedError", () => {
    const app = makeApp()
    const handle = app.decorate("commandBus", (i) => i)
    app.markStarted()
    expect(() => app.decorate("commandBus", (i) => i)).toThrow(AppAlreadyStartedError)
    expect(() => app.removeDecorator(handle)).toThrow(AppAlreadyStartedError)
  })
})

describe("app.start() — decoration pipeline", () => {
  it("Test 7: decorators wrap in registration order — last decorate = outermost", async () => {
    // Use applyDecorators directly to test the composition without full app startup
    const { applyDecorators } = await import("../decorator.js")
    const { buildResolved } = await import("../resolved.js")
    const { SlotRegistry } = await import("../slot-registry.js")

    // Build a minimal resolved proxy
    const registry = new SlotRegistry()
    const baseBus: CommandBus = {
      dispatch: async () => "base",
      subscribe: () => {},
    }
    registry.setDefault("commandBus", () => baseBus)
    const resolved = buildResolved(registry)

    // Two user-registered decorators in order: "a:" then "b:"
    const regs = [
      {
        handle: makeFrameworkHandle("commandBus", "a"),
        factory: (inner: CommandBus): CommandBus => ({
          ...inner,
          dispatch: async (msg: any) => "a:" + (await inner.dispatch(msg)),
        }),
        frameworkDefault: false,
      },
      {
        handle: makeFrameworkHandle("commandBus", "b"),
        factory: (inner: CommandBus): CommandBus => ({
          ...inner,
          dispatch: async (msg: any) => "b:" + (await inner.dispatch(msg)),
        }),
        frameworkDefault: false,
      },
    ]

    const decorated = applyDecorators("commandBus", baseBus, regs, resolved)
    const result = await decorated.dispatch({} as any)
    // a is applied first (inner), b is applied second (outer) → b wraps a wraps base
    expect(result).toBe("b:a:base")
  })

  it("Test 8: polymorphism — decorator receives the mock instance set via .set() (start-time binding)", async () => {
    const { applyDecorators } = await import("../decorator.js")
    const { buildResolved } = await import("../resolved.js")
    const { SlotRegistry } = await import("../slot-registry.js")

    const mockDistributedBus: CommandBus = {
      dispatch: async () => "distributed",
      subscribe: () => {},
    }
    let capturedInner: CommandBus | undefined

    const registry = new SlotRegistry()
    registry.setDefault("commandBus", () => mockDistributedBus)
    const resolved = buildResolved(registry)

    const regs = [
      {
        handle: makeFrameworkHandle("commandBus", "tracer"),
        factory: (inner: CommandBus): CommandBus => {
          capturedInner = inner
          return inner
        },
        frameworkDefault: false,
      },
    ]

    applyDecorators("commandBus", resolved.commandBus, regs, resolved)
    expect(capturedInner).toBe(mockDistributedBus)
  })
})

// ─── Plan 02 placeholder — skip until framework intercepting defaults are wired ─

describe.skip("decorator polymorphism — Plan 02 will enable", () => {
  it("tracing(intercepting(distributedBus)) — replacing base does not affect decorators (success criterion #4)", async () => {
    // Plan 02 fills this in:
    // 1. Build a kronos() app with quiet:true and an in-memory entity + handler.
    // 2. .set("commandBus", () => mockDistributedCommandBus) where mock is a structurally-typed CommandBus stub.
    // 3. .decorate("commandBus", (inner) => createTracingCommandBus(inner, mockSpans)).
    // 4. await app.start().
    // 5. Dispatch a command via app.commandGateway.send(...).
    // 6. Assert: (a) tracing span fired (inspect mockSpans), (b) mock distributed bus received the dispatch, (c) intercepting framework default is in chain.
    expect(true).toBe(true)
  })
})
