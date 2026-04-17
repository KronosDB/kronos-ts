import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, generateIdentifier } from "@kronos-ts/common"
import type { CommandMessage } from "../message.js"
import type { ProcessingContext } from "../processing-context.js"
import { createRetryingCommandBus, exponentialBackoffRetryPolicy } from "../retrying-command-bus.js"
import type { CommandBus } from "../bus.js"

class AppendConditionError extends Error {
  constructor(msg: string) {
    super(msg)
    this.name = "AppendConditionError"
  }
}

function commandMsg(name: string): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

describe("RetryingCommandBus", () => {
  it("passes through on success", async () => {
    const calls: number[] = []
    const delegate: CommandBus = {
      async dispatch() { calls.push(1); return "ok" },
      subscribe() {},
    }

    const bus = createRetryingCommandBus(delegate, exponentialBackoffRetryPolicy())
    const result = await bus.dispatch(commandMsg("TestCommand"))

    expect(result).toBe("ok")
    expect(calls).toHaveLength(1)
  })

  it("retries on AppendConditionError", async () => {
    let attempts = 0
    const delegate: CommandBus = {
      async dispatch() {
        attempts++
        if (attempts < 3) throw new AppendConditionError("conflict")
        return "ok"
      },
      subscribe() {},
    }

    const bus = createRetryingCommandBus(delegate, exponentialBackoffRetryPolicy({ initialDelayMs: 1 }))
    const result = await bus.dispatch(commandMsg("TestCommand"))

    expect(result).toBe("ok")
    expect(attempts).toBe(3)
  })

  it("does not retry non-transient errors", async () => {
    let attempts = 0
    const delegate: CommandBus = {
      async dispatch() {
        attempts++
        throw new Error("permanent failure")
      },
      subscribe() {},
    }

    const bus = createRetryingCommandBus(delegate, exponentialBackoffRetryPolicy({ initialDelayMs: 1 }))

    expect(bus.dispatch(commandMsg("TestCommand"))).rejects.toThrow("permanent failure")
    // Wait a tick for the promise to settle
    await new Promise((r) => setTimeout(r, 10))
    expect(attempts).toBe(1)
  })

  it("gives up after max retries", async () => {
    let attempts = 0
    const delegate: CommandBus = {
      async dispatch() {
        attempts++
        throw new AppendConditionError("always conflicts")
      },
      subscribe() {},
    }

    const bus = createRetryingCommandBus(delegate, exponentialBackoffRetryPolicy({ maxRetries: 3, initialDelayMs: 1 }))

    expect(bus.dispatch(commandMsg("TestCommand"))).rejects.toThrow("always conflicts")
    await new Promise((r) => setTimeout(r, 50))
    expect(attempts).toBe(4) // initial + 3 retries
  })

  it("delegates subscribe to the underlying bus", () => {
    const subscribed: string[] = []
    const delegate: CommandBus = {
      async dispatch() { return undefined },
      subscribe(name) { subscribed.push(name) },
    }

    const bus = createRetryingCommandBus(delegate, exponentialBackoffRetryPolicy())
    bus.subscribe("test.Command", async (_msg: CommandMessage, _ctx: ProcessingContext) => undefined)

    expect(subscribed).toEqual(["test.Command"])
  })
})
