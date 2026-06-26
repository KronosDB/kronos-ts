import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { tracingHandlerEnhancerDefinition } from "../tracing-handler-enhancer.js"
import { getActiveCorrelationData } from "../correlation-data.js"
import type { SpanFactory, Span } from "../span-factory.js"
import type { Message } from "../message.js"
import { inUoW } from "./_helpers/in-uow.js"

function recordingSpanFactory() {
  const calls: Array<{ method: string; name: string; message?: Message }> = []
  let ranActive = false
  const span: Span = {
    start() { return this },
    end() {},
    recordException() { return this as any },
    runActive<T>(fn: () => T): T { ranActive = true; return fn() },
  }
  const factory: SpanFactory = {
    createRootTrace() { return span },
    createHandlerSpan(name, message) { calls.push({ method: "handler", name, message }); return span },
    createLinkedHandlerSpan(name, message) { calls.push({ method: "linked", name, message }); return span },
    createDispatchSpan(name, message) { calls.push({ method: "dispatch", name, message }); return span },
    createInternalSpan(name) { calls.push({ method: "internal", name }); return span },
    propagateContext<M extends Message>(m: M) { return m },
    currentTraceContext() { return { traceparent: "tp-1" } },
    registerSpanAttributeProvider() {},
  }
  return { factory, calls, ranActive: () => ranActive }
}

const eventMessage = {
  kind: "event", identifier: "e-1", name: qn("t", "E"),
  payload: {}, metadata: emptyMetadata(), timestamp: 0, tags: [],
} as unknown as Message

const commandMessage = {
  kind: "command", identifier: "c-1", name: qn("t", "C"),
  payload: {}, metadata: emptyMetadata(), timestamp: 0,
} as unknown as Message

describe("tracingHandlerEnhancerDefinition", () => {
  it("event handlers start a linked span from the event and contribute trace context to the UoW", async () => {
    const { factory, calls, ranActive } = recordingSpanFactory()
    let seen: Record<string, string> | undefined

    const handler = tracingHandlerEnhancerDefinition(factory).wrapHandler(
      async (_m: Message) => { seen = getActiveCorrelationData() },
      { messageType: "event", messageName: "t.E", handlerGroup: "proc" },
    )

    await inUoW(() => handler(eventMessage))

    expect(calls[0]?.method).toBe("linked")
    expect(calls[0]?.message?.identifier).toBe("e-1")
    expect(ranActive()).toBe(true)
    // Trace context captured onto the UoW so outgoing/appended messages carry it.
    expect(seen?.traceparent).toBe("tp-1")
  })

  it("command handlers continue the current trace via a child handler span", async () => {
    const { factory, calls } = recordingSpanFactory()

    const handler = tracingHandlerEnhancerDefinition(factory).wrapHandler(
      async (_m: Message) => {},
      { messageType: "command", messageName: "t.C", handlerGroup: "cmd" },
    )

    await inUoW(() => handler(commandMessage))

    expect(calls[0]?.method).toBe("handler")
    expect(calls[0]?.message?.identifier).toBe("c-1")
  })

  it("records the exception and rethrows when the handler fails", async () => {
    const { factory } = recordingSpanFactory()
    const boom = new Error("boom")
    const handler = tracingHandlerEnhancerDefinition(factory).wrapHandler(
      async (_m: Message) => { throw boom },
      { messageType: "command", messageName: "t.C", handlerGroup: "cmd" },
    )

    await expect(inUoW(() => handler(commandMessage))).rejects.toThrow("boom")
  })
})
