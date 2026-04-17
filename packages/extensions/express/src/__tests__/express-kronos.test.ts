import { describe, expect, it } from "bun:test"
import { getKronos, type KronosLocals } from "../express-kronos.js"

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Express Kronos integration", () => {
  describe("getKronos", () => {
    it("returns kronos locals from request", () => {
      // given
      const mockLocals: KronosLocals = {
        commandGateway: { send: async () => undefined } as any,
        queryGateway: { query: async () => undefined } as any,
        application: {} as any,
      }
      const req = { app: { locals: { kronos: mockLocals } } }

      // when
      const kronos = getKronos(req)

      // then
      expect(kronos).toBe(mockLocals)
      expect(kronos.commandGateway).toBeDefined()
      expect(kronos.queryGateway).toBeDefined()
    })

    it("throws when kronos is not initialized", () => {
      // given
      const req = { app: { locals: {} } }

      // when / then
      expect(() => getKronos(req)).toThrow("Kronos not initialized")
    })
  })
})
