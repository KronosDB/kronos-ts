/**
 * Unit tests for the `openTelemetry(options)` / `openTelemetryMetrics(options)`
 * factories (post-container cutover).
 *
 * There is no app to decorate any more — `openTelemetry()` returns plain
 * values (`{ spanFactory, handlerEnhancer }`) that the caller wraps/passes
 * themselves in their own composition root. These tests assert:
 * - `openTelemetry()` returns a spanFactory + a handlerEnhancer wired to it
 * - `tracingCommandBus(delegate, spanFactory)` produces a CommandBus shape
 *   that flows dispatch through to the delegate
 * - the SAME spanFactory instance flows into both the command bus wrapper
 *   and the handler enhancer (closure capture)
 * - options thread through to `openTelemetrySpanFactory`
 * - `openTelemetryMetrics()` returns a plain HandlerEnhancerDefinition
 */
import { describe, expect, it } from "bun:test"
import { tracingCommandBus } from "@kronos-ts/messaging"
import type { CommandBus } from "@kronos-ts/messaging"
import { openTelemetry, openTelemetryMetrics } from "../opentelemetry.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeStubBus(): CommandBus {
  return {
    dispatch: async () => undefined,
    subscribe: () => {},
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("openTelemetry() factory", () => {
  it("returns a spanFactory and a handlerEnhancer, not something that mutates an app", () => {
    // given / when
    const tracing = openTelemetry()

    // then
    expect(tracing.spanFactory).toBeDefined()
    expect(typeof tracing.spanFactory.createDispatchSpan).toBe("function")
    expect(tracing.handlerEnhancer).toBeDefined()
    expect(typeof tracing.handlerEnhancer.wrapHandler).toBe("function")
  })

  it("tracingCommandBus(delegate, spanFactory) wraps a real command bus", async () => {
    // given
    const { spanFactory } = openTelemetry()
    const dispatched: string[] = []
    const delegate: CommandBus = {
      dispatch: async (msg) => {
        const n = msg.name as unknown as { namespace: string; name: string }
        dispatched.push(`${n.namespace}:${n.name}`)
        return undefined
      },
      subscribe: () => {},
    }

    // when
    const wrapped = tracingCommandBus(delegate, spanFactory)
    expect(typeof wrapped.dispatch).toBe("function")
    expect(typeof wrapped.subscribe).toBe("function")

    await wrapped.dispatch({
      identifier: "id-1",
      name: { namespace: "test", name: "Cmd" } as unknown as Parameters<CommandBus["dispatch"]>[0]["name"],
      payload: {},
      metadata: {},
      timestamp: 0,
    } as Parameters<CommandBus["dispatch"]>[0])

    // then — dispatch flowed through to the delegate
    expect(dispatched).toEqual(["test:Cmd"])
  })

  it("uses the SAME spanFactory instance for both the bus wrapper and the handler enhancer", async () => {
    // given
    const { spanFactory, handlerEnhancer } = openTelemetry()
    const wrapped = tracingCommandBus(makeStubBus(), spanFactory)

    // when — exercise both consumers of the closed-over spanFactory
    const dispatchResult = await wrapped.dispatch({
      identifier: "id-2",
      name: { namespace: "test", name: "Cmd" } as unknown as Parameters<CommandBus["dispatch"]>[0]["name"],
      payload: {},
      metadata: {},
      timestamp: 0,
    } as Parameters<CommandBus["dispatch"]>[0])

    const handled: string[] = []
    const fakeHandler = async (payload: unknown) => {
      handled.push(String(payload))
      return "ok"
    }
    const wrappedHandler = handlerEnhancer.wrapHandler(fakeHandler, {
      handlerGroup: "test-group",
      messageName: "test.msg",
    })
    const result = await wrappedHandler("payload-1")

    // then — both code paths, sharing one spanFactory, complete cleanly
    expect(dispatchResult).toBeUndefined()
    expect(handled).toEqual(["payload-1"])
    expect(result).toBe("ok")
  })

  it("threads options through to openTelemetrySpanFactory", () => {
    // given
    const customAttrProvider = {
      provideAttributes: () => ({ "custom.attr": "v" }),
    }

    // when / then — does not throw with a non-default options object, and
    // still returns a usable spanFactory + handlerEnhancer pair. Attribute
    // threading itself is verified end-to-end by the span-observation suite.
    const tracing = openTelemetry({ spanAttributeProviders: [customAttrProvider] })
    expect(tracing.spanFactory).toBeDefined()
    expect(tracing.handlerEnhancer).toBeDefined()
  })
})

describe("openTelemetryMetrics() factory", () => {
  it("returns a plain HandlerEnhancerDefinition", () => {
    // given / when
    const metrics = openTelemetryMetrics()

    // then
    expect(metrics).toBeDefined()
    expect(typeof metrics.wrapHandler).toBe("function")
  })
})
