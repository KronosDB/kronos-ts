import { describe, expect, it } from "bun:test"
import { qn, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import type { CommandMessage, Message } from "@kronos-ts/messaging"
import { createOpenTelemetrySpanFactory } from "../opentelemetry-span-factory.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function commandMsg(name: string, metadata: Record<string, unknown> = {}): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata,
    timestamp: Date.now(),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("OpenTelemetrySpanFactory", () => {
  it("creates a span factory", () => {
    // given / when
    const factory = createOpenTelemetrySpanFactory()

    // then
    expect(factory).toBeDefined()
    expect(factory.createRootTrace).toBeFunction()
    expect(factory.createHandlerSpan).toBeFunction()
    expect(factory.createDispatchSpan).toBeFunction()
    expect(factory.createInternalSpan).toBeFunction()
    expect(factory.propagateContext).toBeFunction()
  })

  describe("span lifecycle", () => {
    it("createRootTrace returns a startable span", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()

      // when
      const span = factory.createRootTrace("test-operation")

      // then — no errors on lifecycle
      span.start()
      span.end()
    })

    it("createHandlerSpan creates a consumer span", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()
      const msg = commandMsg("DoSomething")

      // when
      const span = factory.createHandlerSpan("handle(DoSomething)", msg)

      // then
      span.start()
      span.end()
    })

    it("createDispatchSpan creates a producer span", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()
      const msg = commandMsg("DoSomething")

      // when
      const span = factory.createDispatchSpan("dispatch(DoSomething)", msg)

      // then
      span.start()
      span.end()
    })

    it("createInternalSpan creates an internal span", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()

      // when
      const span = factory.createInternalSpan("internal-op")

      // then
      span.start()
      span.end()
    })

    it("recordException ends the span with error", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()
      const span = factory.createInternalSpan("failing-op").start()

      // when / then — no errors
      span.recordException(new Error("test failure"))
    })
  })

  describe("context propagation", () => {
    it("propagateContext returns the message when no active context", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()
      const msg = commandMsg("DoSomething")

      // when
      const propagated = factory.propagateContext(msg)

      // then — message returned (possibly with trace headers)
      expect(propagated.identifier).toBe(msg.identifier)
      expect(propagated.payload).toBe(msg.payload)
    })
  })

  describe("span attribute providers", () => {
    it("registers custom attribute providers", () => {
      // given
      const factory = createOpenTelemetrySpanFactory({
        spanAttributeProviders: [
          {
            provideAttributes(message: Message) {
              return { "custom.attr": "value" }
            },
          },
        ],
      })

      // when — creating a span doesn't throw
      const msg = commandMsg("DoSomething")
      const span = factory.createHandlerSpan("handle", msg)
      span.start()
      span.end()
    })

    it("registerSpanAttributeProvider adds providers at runtime", () => {
      // given
      const factory = createOpenTelemetrySpanFactory()
      factory.registerSpanAttributeProvider({
        provideAttributes: () => ({ "runtime.attr": "added" }),
      })

      // when — no errors
      const msg = commandMsg("DoSomething")
      const span = factory.createHandlerSpan("handle", msg)
      span.start()
      span.end()
    })
  })
})
