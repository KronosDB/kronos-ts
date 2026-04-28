import { describe, it, expect } from "bun:test"
import { makeFrameworkHandle } from "../decorator.js"
import { Defaults } from "../defaults-handles.js"
import { UnknownDecoratorHandleError } from "../errors.js"

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
