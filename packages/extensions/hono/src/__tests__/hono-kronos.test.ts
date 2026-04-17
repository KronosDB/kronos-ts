import { describe, expect, it } from "bun:test"
import { getKronos, KRONOS_CONTEXT_KEY, type KronosContext } from "../hono-kronos.js"

describe("Hono Kronos integration", () => {
  describe("getKronos", () => {
    it("returns kronos context from Hono context", () => {
      // given
      const mockContext: KronosContext = {
        commandGateway: { send: async () => undefined } as any,
        queryGateway: { query: async () => undefined } as any,
        application: {} as any,
      }
      const c = {
        get: (key: string) => key === KRONOS_CONTEXT_KEY ? mockContext : undefined,
      }

      // when
      const kronos = getKronos(c)

      // then
      expect(kronos).toBe(mockContext)
      expect(kronos.commandGateway).toBeDefined()
      expect(kronos.queryGateway).toBeDefined()
    })

    it("throws when kronos is not initialized", () => {
      const c = { get: () => undefined }
      expect(() => getKronos(c)).toThrow("Kronos not initialized")
    })
  })
})
