import { describe, expect, it } from "bun:test"
import type { CommandBus, SubscribeOptions } from "../command-bus.js"
import type { CommandMessage } from "../message.js"
import { createInterceptingCommandBus } from "../intercepting-command-bus.js"
import { createRetryingCommandBus } from "../retrying-command-bus.js"

/**
 * A base bus that records the SubscribeOptions it actually received. The point
 * of these tests is the decorator chain: an optional parameter is silently
 * lossy — every wrapper that forwards `subscribe(name, handler)` without the
 * third argument drops the consumer group, and the compiler cannot see it.
 */
function recordingBus(): CommandBus & { seen: Array<SubscribeOptions | undefined> } {
  const seen: Array<SubscribeOptions | undefined> = []
  return {
    seen,
    async dispatch(): Promise<unknown> {
      return undefined
    },
    subscribe(_name: string, _handler: (m: CommandMessage) => Promise<unknown>, options?: SubscribeOptions) {
      seen.push(options)
    },
  }
}

const noopHandler = async () => undefined

describe("subscribe consumer group", () => {
  it("survives the intercepting decorator", () => {
    const base = recordingBus()
    createInterceptingCommandBus(base).subscribe("X", noopHandler, { group: "billing" })
    expect(base.seen).toEqual([{ group: "billing" }])
  })

  it("survives the retrying decorator", () => {
    const base = recordingBus()
    createRetryingCommandBus(base, { maxAttempts: 1 }).subscribe("X", noopHandler, { group: "billing" })
    expect(base.seen).toEqual([{ group: "billing" }])
  })

  it("survives a stacked chain — the shape the app actually builds", () => {
    const base = recordingBus()
    const stacked = createRetryingCommandBus(createInterceptingCommandBus(base), { maxAttempts: 1 })
    stacked.subscribe("X", noopHandler, { group: "ordering" })
    expect(base.seen).toEqual([{ group: "ordering" }])
  })

  it("absent group stays absent — existing deployments keep their queue names", () => {
    const base = recordingBus()
    createInterceptingCommandBus(base).subscribe("X", noopHandler)
    expect(base.seen).toEqual([undefined])
  })
})
