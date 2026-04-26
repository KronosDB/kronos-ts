import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, generateIdentifier, mergeMetadata, metadataAnd } from "@kronos-ts/common"
import type { CommandMessage } from "../message.js"
import type { ProcessingContext } from "../processing-context.js"
import { Phase } from "../processing-context.js"
import { createSimpleCommandBus } from "../simple-command-bus.js"
import { createInterceptingCommandBus } from "../intercepting-command-bus.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function commandMsg(name: string, payload: unknown = {}): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

function commandMsgWithMeta(name: string, meta: Record<string, string>): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata: meta,
    timestamp: Date.now(),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SimpleCommandBus", () => {
  describe("dispatching commands to handlers", () => {
    it("dispatches a command to the registered handler", async () => {
      const bus = createSimpleCommandBus()
      const received: CommandMessage[] = []

      bus.subscribe("test.DoSomething", async (msg) => {
        received.push(msg)
        return "handled"
      })

      const msg = commandMsg("DoSomething")
      const result = await bus.dispatch(msg)

      expect(result).toBe("handled")
      expect(received).toHaveLength(1)
      expect(received[0]!.identifier).toBe(msg.identifier)
    })

    it("dispatches to the correct handler when multiple are registered", async () => {
      const bus = createSimpleCommandBus()
      const calls: string[] = []

      bus.subscribe("test.CreateUser", async () => { calls.push("create"); return undefined })
      bus.subscribe("test.DeleteUser", async () => { calls.push("delete"); return undefined })

      await bus.dispatch(commandMsg("DeleteUser"))

      expect(calls).toEqual(["delete"])
    })
  })

  describe("unknown command handling", () => {
    it("throws a clear error when no handler is registered", async () => {
      const bus = createSimpleCommandBus()

      expect(bus.dispatch(commandMsg("Unknown"))).rejects.toThrow(
        'No handler registered for command "test.Unknown"',
      )
    })
  })

  describe("UnitOfWork lifecycle", () => {
    it("provides a ProcessingContext to the handler", async () => {
      const bus = createSimpleCommandBus()
      let receivedCtx: ProcessingContext | undefined

      bus.subscribe("test.Cmd", async (_msg, ctx) => {
        receivedCtx = ctx
        return undefined
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(receivedCtx).toBeDefined()
      expect(receivedCtx!.isCompleted).toBe(true)
    })

    it("executes lifecycle phases around handler invocation", async () => {
      const bus = createSimpleCommandBus()
      const phases: string[] = []

      // Use a custom UnitOfWork factory to observe phase execution
      bus.subscribe("test.Cmd", async (_msg, ctx) => {
        // Register hooks from inside the handler -- they should fire
        // in later phases of the same UnitOfWork
        ctx.onPrepareCommit(() => { phases.push("prepareCommit") })
        ctx.onCommit(() => { phases.push("commit") })
        ctx.onAfterCommit(() => { phases.push("afterCommit") })
        phases.push("handler")
        return undefined
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(phases).toEqual(["handler", "prepareCommit", "commit", "afterCommit"])
    })

    it("runs error handlers when the handler throws", async () => {
      const bus = createSimpleCommandBus()
      const errorsCaught: unknown[] = []

      // We need to hook into the UnitOfWork to register error handlers.
      // Since the handler receives ProcessingContext, register from inside.
      bus.subscribe("test.Cmd", async (_msg, ctx) => {
        ctx.onError((_c, err) => { errorsCaught.push(err) })
        throw new Error("handler failed")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("handler failed")
      expect(errorsCaught).toHaveLength(1)
      expect((errorsCaught[0] as Error).message).toBe("handler failed")
    })

    it("runs whenComplete handlers on success", async () => {
      const bus = createSimpleCommandBus()
      let completeCalled = false

      bus.subscribe("test.Cmd", async (_msg, ctx) => {
        ctx.whenComplete(() => { completeCalled = true })
        return "ok"
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(completeCalled).toBe(true)
    })

    it("does NOT run whenComplete handlers on failure", async () => {
      const bus = createSimpleCommandBus()
      let completeCalled = false

      bus.subscribe("test.Cmd", async (_msg, ctx) => {
        ctx.whenComplete(() => { completeCalled = true })
        throw new Error("boom")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("boom")
      expect(completeCalled).toBe(false)
    })
  })

  describe("handler results and errors", () => {
    it("propagates the handler return value to the caller", async () => {
      const bus = createSimpleCommandBus()

      bus.subscribe("test.Cmd", async () => ({ id: 42, name: "created" }))

      const result = await bus.dispatch(commandMsg("Cmd"))

      expect(result).toEqual({ id: 42, name: "created" })
    })

    it("propagates handler errors to the caller", async () => {
      const bus = createSimpleCommandBus()

      bus.subscribe("test.Cmd", async () => {
        throw new Error("business rule violated")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("business rule violated")
    })

    it("preserves error type", async () => {
      class ValidationError extends Error {
        constructor(msg: string) {
          super(msg)
          this.name = "ValidationError"
        }
      }

      const bus = createSimpleCommandBus()

      bus.subscribe("test.Cmd", async () => {
        throw new ValidationError("invalid input")
      })

      try {
        await bus.dispatch(commandMsg("Cmd"))
        expect.unreachable("should have thrown")
      } catch (e) {
        expect(e).toBeInstanceOf(ValidationError)
        expect((e as Error).message).toBe("invalid input")
      }
    })
  })
})

describe("InterceptingCommandBus", () => {
  describe("dispatch interceptors", () => {
    it("can transform a command before handling", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      let receivedPayload: unknown

      bus.subscribe("test.Cmd", async (msg) => {
        receivedPayload = msg.payload
        return undefined
      })

      bus.registerDispatchInterceptor((msg) => ({
        ...msg,
        payload: { enriched: true },
      }))

      await bus.dispatch(commandMsg("Cmd", { enriched: false }))

      expect(receivedPayload).toEqual({ enriched: true })
    })

    it("can reject a command by throwing", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      let handlerCalled = false

      bus.subscribe("test.Cmd", async () => { handlerCalled = true; return undefined })

      bus.registerDispatchInterceptor(() => {
        throw new Error("rejected by policy")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("rejected by policy")
      expect(handlerCalled).toBe(false)
    })

    it("runs dispatch interceptors in registration order", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const order: number[] = []

      bus.subscribe("test.Cmd", async () => undefined)

      bus.registerDispatchInterceptor((msg) => { order.push(1); return msg })
      bus.registerDispatchInterceptor((msg) => { order.push(2); return msg })
      bus.registerDispatchInterceptor((msg) => { order.push(3); return msg })

      await bus.dispatch(commandMsg("Cmd"))

      expect(order).toEqual([1, 2, 3])
    })

    it("chains transformations -- each interceptor sees the previous result", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      let finalMeta: Record<string, unknown> = {}

      bus.subscribe("test.Cmd", async (msg) => {
        finalMeta = { ...msg.metadata }
        return undefined
      })

      bus.registerDispatchInterceptor((msg) => ({
        ...msg,
        metadata: metadataAnd(msg.metadata, "step", "1"),
      }))
      bus.registerDispatchInterceptor((msg) => ({
        ...msg,
        metadata: metadataAnd(msg.metadata, "step2", "2"),
      }))

      await bus.dispatch(commandMsg("Cmd"))

      expect(finalMeta).toEqual({ step: "1", step2: "2" })
    })

    it("can be unsubscribed", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const calls: string[] = []

      bus.subscribe("test.Cmd", async () => undefined)

      const unsub = bus.registerDispatchInterceptor((msg) => {
        calls.push("intercepted")
        return msg
      })

      await bus.dispatch(commandMsg("Cmd"))
      expect(calls).toEqual(["intercepted"])

      // Unsubscribe and dispatch again
      unsub()
      await bus.dispatch(commandMsg("Cmd"))
      expect(calls).toEqual(["intercepted"]) // no second call
    })
  })

  describe("handler interceptors", () => {
    it("wraps the handler invocation", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const order: string[] = []

      bus.subscribe("test.Cmd", async () => {
        order.push("handler")
        return "result"
      })

      bus.registerHandlerInterceptor(async (_msg, next) => {
        order.push("before")
        const result = await next()
        order.push("after")
        return result
      })

      const result = await bus.dispatch(commandMsg("Cmd"))

      expect(result).toBe("result")
      expect(order).toEqual(["before", "handler", "after"])
    })

    it("can short-circuit by not calling next()", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      let handlerCalled = false

      bus.subscribe("test.Cmd", async () => {
        handlerCalled = true
        return "from handler"
      })

      bus.registerHandlerInterceptor(async () => {
        return "short-circuited"
      })

      const result = await bus.dispatch(commandMsg("Cmd"))

      expect(result).toBe("short-circuited")
      expect(handlerCalled).toBe(false)
    })

    it("runs as a chain -- each calls next() to proceed", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const order: string[] = []

      bus.subscribe("test.Cmd", async () => {
        order.push("handler")
        return "done"
      })

      bus.registerHandlerInterceptor(async (_msg, next) => {
        order.push("first-before")
        const r = await next()
        order.push("first-after")
        return r
      })

      bus.registerHandlerInterceptor(async (_msg, next) => {
        order.push("second-before")
        const r = await next()
        order.push("second-after")
        return r
      })

      await bus.dispatch(commandMsg("Cmd"))

      // First registered runs outermost
      expect(order).toEqual([
        "first-before",
        "second-before",
        "handler",
        "second-after",
        "first-after",
      ])
    })

    it("can be unsubscribed", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const calls: string[] = []

      bus.subscribe("test.Cmd", async () => undefined)

      const unsub = bus.registerHandlerInterceptor(async (_msg, next) => {
        calls.push("intercepted")
        return next()
      })

      await bus.dispatch(commandMsg("Cmd"))
      expect(calls).toEqual(["intercepted"])

      unsub()
      await bus.dispatch(commandMsg("Cmd"))
      expect(calls).toEqual(["intercepted"]) // not called again
    })

    it("propagates errors thrown by an interceptor", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)

      bus.subscribe("test.Cmd", async () => "ok")

      bus.registerHandlerInterceptor(async () => {
        throw new Error("interceptor failure")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("interceptor failure")
    })
  })

  describe("dispatch and handler interceptors together", () => {
    it("dispatch interceptors run before handler interceptors", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      const order: string[] = []

      bus.subscribe("test.Cmd", async () => {
        order.push("handler")
        return undefined
      })

      bus.registerDispatchInterceptor((msg) => {
        order.push("dispatch")
        return msg
      })

      bus.registerHandlerInterceptor(async (_msg, next) => {
        order.push("handler-interceptor")
        return next()
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(order).toEqual(["dispatch", "handler-interceptor", "handler"])
    })

    it("dispatch interceptor transforms are visible to handler interceptors", async () => {
      const inner = createSimpleCommandBus()
      const bus = createInterceptingCommandBus(inner)
      let payloadSeenByHandlerInterceptor: unknown

      bus.subscribe("test.Cmd", async () => undefined)

      bus.registerDispatchInterceptor((msg) => ({
        ...msg,
        payload: { transformed: true },
      }))

      bus.registerHandlerInterceptor(async (msg, next) => {
        payloadSeenByHandlerInterceptor = msg.payload
        return next()
      })

      await bus.dispatch(commandMsg("Cmd", { transformed: false }))

      expect(payloadSeenByHandlerInterceptor).toEqual({ transformed: true })
    })
  })
})
