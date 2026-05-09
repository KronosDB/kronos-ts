/**
 * Plan 09-01 Task 1 — tokenStore typed slot.
 *
 * Asserts: (a) `kronos()` resolves a default in-memory TokenStore and emits the
 * SLT-04 startup warning; (b) `app.set('tokenStore', ...)` overrides the default
 * and suppresses the warning.
 */
import { describe, it, expect } from "bun:test"
import { kronos } from "../kronos.js"
import { createInMemoryTokenStore, type TokenStore } from "@kronos-ts/messaging"

describe("tokenStore slot — Plan 09-01 (D-84)", () => {
  it("default in-memory tokenStore is registered and emits the SLT-04 startup warning", async () => {
    const messages: string[] = []
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    })
    const running = await app.start()
    try {
      expect(messages).toContain(
        "[kronos] tokenStore: in-memory — not durable, configure a persistence extension for production",
      )
    } finally {
      await running.stop()
    }
  })

  it("app.set('tokenStore', ...) overrides the default and suppresses the warning", async () => {
    const messages: string[] = []
    const myStore: TokenStore = createInMemoryTokenStore()
    const app = kronos({
      logger: { warn: (msg: string) => messages.push(msg) },
    }).set("tokenStore", () => myStore)

    const running = await app.start()
    try {
      // The tokenStore-specific warning must NOT be emitted.
      const tokenWarning = messages.find((m) => m.includes("tokenStore"))
      expect(tokenWarning).toBeUndefined()
    } finally {
      await running.stop()
    }
  })
})
