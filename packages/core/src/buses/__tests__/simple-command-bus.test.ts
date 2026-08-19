import { describe, expect, it } from "bun:test"
import { qn } from "../../primitives/qualified-name.js"
import { emptyMetadata, metadataAnd } from "../../primitives/metadata.js"
import { generateIdentifier } from "../../primitives/identifier.js"
import type { CommandMessage } from "../../messages/message.js"
import { simpleCommandBus } from "../simple-command-bus.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { lineage, interceptingCommandBus, type Intercept } from "../intercepting-bus.js"

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SimpleCommandBus", () => {
  describe("dispatching commands to handlers", () => {
    it("dispatches a command to the registered handler", async () => {
      const bus = simpleCommandBus(unitOfWork)
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
      const bus = simpleCommandBus(unitOfWork)
      const calls: string[] = []

      bus.subscribe("test.CreateUser", async () => { calls.push("create"); return undefined })
      bus.subscribe("test.DeleteUser", async () => { calls.push("delete"); return undefined })

      await bus.dispatch(commandMsg("DeleteUser"))

      expect(calls).toEqual(["delete"])
    })
  })

  describe("unknown command handling", () => {
    it("throws a clear error when no handler is registered", async () => {
      const bus = simpleCommandBus(unitOfWork)

      expect(bus.dispatch(commandMsg("Unknown"))).rejects.toThrow(
        'No handler registered for command "test.Unknown"',
      )
    })
  })

  describe("UnitOfWork lifecycle", () => {
    it("executes lifecycle phases around handler invocation", async () => {
      const bus = simpleCommandBus(unitOfWork)
      const phases: string[] = []

      // Handlers receive the message AND the unit of work the bus opened;
      // lifecycle hooks are registered on that handle.
      bus.subscribe("test.Cmd", async (_msg, uow) => {
        uow.onPrepareCommit(() => { phases.push("prepareCommit") })
        uow.onCommit(() => { phases.push("commit") })
        uow.onAfterCommit(() => { phases.push("afterCommit") })
        phases.push("handler")
        return undefined
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(phases).toEqual(["handler", "prepareCommit", "commit", "afterCommit"])
    })

    it("runs error handlers when the handler throws", async () => {
      const bus = simpleCommandBus(unitOfWork)
      const errorsCaught: unknown[] = []

      bus.subscribe("test.Cmd", async (_msg, uow) => {
        uow.onError(async (err) => { errorsCaught.push(err) })
        throw new Error("handler failed")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("handler failed")
      expect(errorsCaught).toHaveLength(1)
      expect((errorsCaught[0] as Error).message).toBe("handler failed")
    })

    it("runs whenComplete handlers on success", async () => {
      const bus = simpleCommandBus(unitOfWork)
      let completeCalled = false

      bus.subscribe("test.Cmd", async (_msg, uow) => {
        uow.whenComplete(() => { completeCalled = true })
        return "ok"
      })

      await bus.dispatch(commandMsg("Cmd"))

      expect(completeCalled).toBe(true)
    })

    it("does NOT run whenComplete handlers on failure", async () => {
      const bus = simpleCommandBus(unitOfWork)
      let completeCalled = false

      bus.subscribe("test.Cmd", async (_msg, uow) => {
        uow.whenComplete(() => { completeCalled = true })
        throw new Error("boom")
      })

      await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("boom")
      expect(completeCalled).toBe(false)
    })
  })

  describe("handler results and errors", () => {
    it("propagates the handler return value to the caller", async () => {
      const bus = simpleCommandBus(unitOfWork)

      bus.subscribe("test.Cmd", async () => ({ id: 42, name: "created" }))

      const result = await bus.dispatch(commandMsg("Cmd"))

      expect(result).toEqual({ id: 42, name: "created" })
    })

    it("propagates handler errors to the caller", async () => {
      const bus = simpleCommandBus(unitOfWork)

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

      const bus = simpleCommandBus(unitOfWork)

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

// ---------------------------------------------------------------------------
// interceptingCommandBus — interception, minus the registry AND minus the list.
//
// What used to be `registerDispatchInterceptor`, and then a variadic provider
// list, is now ONE function. Plurality composes in function space, where the
// order is written down at the call site instead of spread across two far-apart
// registrations. These assert the same properties — transform, composition
// order, rejection, pass-through — against that one seam.
// ---------------------------------------------------------------------------

describe("interceptingCommandBus", () => {
  it("applies the intercept before the command reaches the handler", async () => {
    const inner = simpleCommandBus(unitOfWork)
    let received: CommandMessage | undefined
    inner.subscribe("test.Cmd", async (msg) => {
      received = msg
      return undefined
    })

    const tenancy: Intercept<CommandMessage> = (m) => ({
      ...m,
      metadata: metadataAnd(m.metadata, "tenantId", "t-1"),
    })
    const bus = interceptingCommandBus(inner, tenancy)

    await bus.dispatch(commandMsg("Cmd"))

    expect(received?.metadata.tenantId).toBe("t-1")
  })

  it("composes in function space — the order you write is the order you get", async () => {
    const inner = simpleCommandBus(unitOfWork)
    const order: number[] = []
    inner.subscribe("test.Cmd", async () => undefined)

    const mark = (n: number): Intercept<CommandMessage> => (m) => {
      order.push(n)
      return m
    }
    const bus = interceptingCommandBus(inner, (m) => mark(3)(mark(2)(mark(1)(m))))

    await bus.dispatch(commandMsg("Cmd"))

    expect(order).toEqual([1, 2, 3])
  })

  it("a later step sees what an earlier one wrote", async () => {
    const inner = simpleCommandBus(unitOfWork)
    let received: CommandMessage | undefined
    inner.subscribe("test.Cmd", async (msg) => {
      received = msg
      return undefined
    })

    const first: Intercept<CommandMessage> = (m) => ({
      ...m,
      metadata: metadataAnd(m.metadata, "step", "first"),
    })
    const second: Intercept<CommandMessage> = (m) => ({
      ...m,
      metadata: metadataAnd(m.metadata, "seen", String(m.metadata.step)),
    })
    const bus = interceptingCommandBus(inner, (m) => second(first(m)))

    await bus.dispatch(commandMsg("Cmd"))

    expect(received?.metadata.step).toBe("first")
    expect(received?.metadata.seen).toBe("first")
  })

  it("a throwing intercept rejects the dispatch and never reaches the handler", async () => {
    const inner = simpleCommandBus(unitOfWork)
    let handled = false
    inner.subscribe("test.Cmd", async () => {
      handled = true
      return undefined
    })

    const bus = interceptingCommandBus(inner, () => {
      throw new Error("rejected")
    })

    await expect(bus.dispatch(commandMsg("Cmd"))).rejects.toThrow("rejected")
    expect(handled).toBe(false)
  })

  it("passes subscribe through to the delegate untouched", async () => {
    const inner = simpleCommandBus(unitOfWork)
    const bus = interceptingCommandBus(inner, lineage)
    let handled = false

    bus.subscribe("test.Cmd", async () => {
      handled = true
      return "ok"
    })

    // Registered on the delegate, so dispatching on EITHER reaches it.
    expect(await inner.dispatch(commandMsg("Cmd"))).toBe("ok")
    expect(handled).toBe(true)
  })

  it("wrapping twice is a no-op on values, not a duplicate stamp", async () => {
    const inner = simpleCommandBus(unitOfWork)
    let received: CommandMessage | undefined
    inner.subscribe("test.Cmd", async (msg) => {
      received = msg
      return undefined
    })

    const bus = interceptingCommandBus(interceptingCommandBus(inner, lineage), lineage)

    const message = commandMsg("Cmd")
    await bus.dispatch(message)

    // `lineage` starts the chain at the message and names it as its own cause;
    // applying it again reads back exactly what it wrote.
    expect(received?.metadata.correlationId).toBe(message.identifier)
    expect(received?.metadata.causationId).toBe(message.identifier)
  })

  it("lineage preserves an existing correlationId and re-points causation at this message", async () => {
    const inner = simpleCommandBus(unitOfWork)
    let received: CommandMessage | undefined
    inner.subscribe("test.Cmd", async (msg) => {
      received = msg
      return undefined
    })

    const bus = interceptingCommandBus(inner, lineage)
    const message = {
      ...commandMsg("Cmd"),
      metadata: metadataAnd(emptyMetadata(), "correlationId", "chain-1"),
    }

    await bus.dispatch(message)

    expect(received?.metadata.correlationId).toBe("chain-1")
    expect(received?.metadata.causationId).toBe(message.identifier)
  })
})
