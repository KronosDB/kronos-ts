/**
 * eventScheduler typed slot.
 *
 * Asserts: (a) `kronos()` resolves a default in-memory EventScheduler and emits
 * the SLT-04 startup warning; (b) `app.set('eventScheduler', ...)` overrides
 * the default and suppresses the warning.
 */
import { describe, it, expect } from "bun:test"
import { kronos } from "../kronos.js"
import {
  createInMemoryEventScheduler,
  type EventScheduler,
  type EventSink,
} from "@kronos-ts/messaging"

const noopSink: EventSink = { publish: async () => {} }

describe("eventScheduler slot", () => {
  it("default in-memory eventScheduler is registered and emits the SLT-04 startup warning", async () => {
    const messages: string[] = []
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    })
    const running = await app.start()
    try {
      expect(messages).toContain(
        "[kronos] eventScheduler: in-memory — not durable, configure a persistence extension for production",
      )
    } finally {
      await running.stop()
    }
  })

  it("app.set('eventScheduler', ...) overrides the default and suppresses the warning", async () => {
    const messages: string[] = []
    const mine = createInMemoryEventScheduler({ eventSink: noopSink })
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    }).set("eventScheduler", () => mine as EventScheduler)

    const running = await app.start()
    try {
      const warn = messages.find((m) => m.includes("eventScheduler"))
      expect(warn).toBeUndefined()
    } finally {
      await running.stop()
      await mine.stop()
    }
  })
})
