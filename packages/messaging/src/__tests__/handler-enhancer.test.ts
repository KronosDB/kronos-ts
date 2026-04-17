import { describe, expect, it } from "bun:test"
import {
  type HandlerEnhancerDefinition,
  type HandlerMetadata,
  multiHandlerEnhancerDefinition,
} from "../handler-enhancer.js"

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("HandlerEnhancerDefinition", () => {
  describe("single enhancer", () => {
    it("wraps a handler function", async () => {
      // given
      const calls: string[] = []
      const enhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, metadata) {
          return ((...args: any[]) => {
            calls.push(`before:${metadata.messageName}`)
            const result = handler(...args)
            calls.push(`after:${metadata.messageName}`)
            return result
          }) as typeof handler
        },
      }

      const original = (x: number) => x * 2
      const metadata: HandlerMetadata = {
        messageType: "command",
        messageName: "test.DoSomething",
        handlerGroup: "test-commands",
      }

      // when
      const enhanced = enhancer.wrapHandler(original, metadata)
      const result = enhanced(5)

      // then
      expect(result).toBe(10)
      expect(calls).toEqual(["before:test.DoSomething", "after:test.DoSomething"])
    })

    it("can return the original handler to skip enhancement", () => {
      // given
      const enhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, metadata) {
          if (metadata.messageType !== "command") return handler
          return ((...args: any[]) => {
            return handler(...args)
          }) as typeof handler
        },
      }

      const original = (x: number) => x * 2
      const eventMetadata: HandlerMetadata = {
        messageType: "event",
        messageName: "test.SomethingHappened",
        handlerGroup: "test-handlers",
      }

      // when
      const result = enhancer.wrapHandler(original, eventMetadata)

      // then — same reference, not wrapped
      expect(result).toBe(original)
    })

    it("can selectively enhance based on message type", async () => {
      // given
      const enhanced: string[] = []
      const enhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, metadata) {
          if (metadata.messageType !== "command") return handler
          enhanced.push(metadata.messageName)
          return ((...args: any[]) => handler(...args)) as typeof handler
        },
      }

      // when
      enhancer.wrapHandler(() => {}, {
        messageType: "command",
        messageName: "CreateOrder",
        handlerGroup: "orders",
      })
      enhancer.wrapHandler(() => {}, {
        messageType: "event",
        messageName: "OrderCreated",
        handlerGroup: "orders",
      })

      // then
      expect(enhanced).toEqual(["CreateOrder"])
    })
  })

  describe("multiHandlerEnhancerDefinition", () => {
    it("applies enhancers in order — first registered wraps outermost", () => {
      // given
      const order: string[] = []

      const outerEnhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, _metadata) {
          return ((...args: any[]) => {
            order.push("outer-before")
            const result = handler(...args)
            order.push("outer-after")
            return result
          }) as typeof handler
        },
      }

      const innerEnhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, _metadata) {
          return ((...args: any[]) => {
            order.push("inner-before")
            const result = handler(...args)
            order.push("inner-after")
            return result
          }) as typeof handler
        },
      }

      const multi = multiHandlerEnhancerDefinition([outerEnhancer, innerEnhancer])
      const metadata: HandlerMetadata = {
        messageType: "command",
        messageName: "test.DoSomething",
        handlerGroup: "test",
      }

      const original = () => { order.push("handler") }

      // when
      const enhanced = multi.wrapHandler(original, metadata)
      enhanced()

      // then — outer wraps inner wraps handler
      expect(order).toEqual([
        "outer-before",
        "inner-before",
        "handler",
        "inner-after",
        "outer-after",
      ])
    })

    it("works with async handlers", async () => {
      // given
      const order: string[] = []

      const enhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, _metadata) {
          return (async (...args: any[]) => {
            order.push("before")
            const result = await handler(...args)
            order.push("after")
            return result
          }) as typeof handler
        },
      }

      const multi = multiHandlerEnhancerDefinition([enhancer])
      const metadata: HandlerMetadata = {
        messageType: "event",
        messageName: "test.EventOccurred",
        handlerGroup: "test",
      }

      const original = async () => {
        order.push("handler")
        return "result"
      }

      // when
      const enhanced = multi.wrapHandler(original, metadata)
      const result = await enhanced()

      // then
      expect(result).toBe("result")
      expect(order).toEqual(["before", "handler", "after"])
    })

    it("preserves handler signature", () => {
      // given
      const enhancer: HandlerEnhancerDefinition = {
        wrapHandler(handler, _metadata) {
          return ((...args: any[]) => handler(...args)) as typeof handler
        },
      }

      const multi = multiHandlerEnhancerDefinition([enhancer])
      const metadata: HandlerMetadata = {
        messageType: "command",
        messageName: "test.DoSomething",
        handlerGroup: "test",
      }

      const original = (a: number, b: string) => `${b}:${a}`

      // when
      const enhanced = multi.wrapHandler(original, metadata)

      // then
      expect(enhanced(42, "hello")).toBe("hello:42")
    })

    it("handles empty enhancer list", () => {
      // given
      const multi = multiHandlerEnhancerDefinition([])
      const original = (x: number) => x + 1

      // when
      const enhanced = multi.wrapHandler(original, {
        messageType: "command",
        messageName: "test.DoSomething",
        handlerGroup: "test",
      })

      // then — returns original unchanged
      expect(enhanced(5)).toBe(6)
    })
  })
})
