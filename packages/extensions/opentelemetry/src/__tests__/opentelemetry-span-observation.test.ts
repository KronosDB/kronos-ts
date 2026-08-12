/**
 * E2E span-observation tests for the `openTelemetry()` factory (post-container
 * cutover — see FUNCTIONAL-APP-FINDINGS.md).
 *
 * There is no App to `.use(openTelemetry())` any more. Tracing is ordinary
 * function composition, wired by the caller:
 *
 *   const { spanFactory, handlerEnhancer } = openTelemetry()
 *   const commandBus = tracingCommandBus(baseCommandBus, spanFactory)
 *
 * These tests exercise a real OTel TracerProvider with an `InMemorySpanExporter`
 * and assert that:
 *   1. command dispatch through a real `createApp`-composed app, with its
 *      commandBus wrapped via `tracingCommandBus`, emits a span
 *   2. `handlerEnhancer.wrapHandler(...)` emits a handler span that is a real
 *      child of the dispatch span it re-parents onto (command-kind messages)
 *   3. `handlerEnhancer.wrapHandler(...)` emits a LINKED (not parented) span
 *      for event-kind messages, matching what an event processor does when
 *      it re-traces from a propagated event
 *   4. without wrapping, no spans are emitted (control case)
 *
 * NOTE: `createApp` does not (yet) plumb a `handlerEnhancer` through to
 * `registerCommandHandlersNatively` / event processors — that wiring is
 * outside this package's scope (packages/app). Tests 2 and 3 exercise
 * `handlerEnhancer` directly against real propagated-context messages
 * instead of going through a processor, since that is exactly the shape a
 * caller uses it in today.
 *
 * Span name expectations (from packages/messaging/src/tracing-command-bus.ts +
 * tracing-handler-enhancer.ts):
 *   - command dispatch: `dispatch(<commandQualifiedName>)`
 *   - handler enhancer: `<handlerGroup>.<messageName>`
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test"
import { trace } from "@opentelemetry/api"
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import { command, event, commandHandler, EventCriteria } from "@kronos-ts/messaging"
import type { CommandBus, CommandMessage, EventMessage } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { createApp, inMemoryComponents, module, type App } from "@kronos-ts/app"
import { openTelemetry, tracingCommandBus } from "../opentelemetry.js"

// ---------------------------------------------------------------------------
// In-memory exporter setup
// ---------------------------------------------------------------------------

interface ExporterHarness {
  exporter: InMemorySpanExporter
  uninstall: () => Promise<void>
}

function installInMemoryExporter(): ExporterHarness {
  const exporter = new InMemorySpanExporter()
  const provider = new NodeTracerProvider({
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  })
  provider.register()
  return {
    exporter,
    uninstall: async () => {
      await provider.shutdown()
      trace.disable()
    },
  }
}

// ---------------------------------------------------------------------------
// Domain — minimal Greeting aggregate
// ---------------------------------------------------------------------------

const Greet = command({
  name: qn("otel-test", "Greet"),
  payload: z.object({ id: z.string(), who: z.string() }),
  routingKey: "id",
})

const Greeted = event({
  name: qn("otel-test", "Greeted"),
  payload: z.object({ id: z.string(), who: z.string() }),
  tags: (p) => [tag("id", p.id)],
})

type GreetState = { greeted: boolean }

const Greeting = state({
  name: "Greeting",
  id: { id: z.string() },
  initial: () => ({ greeted: false }) as GreetState,
  criteria: (id) => EventCriteria.havingTags(tag("id", id.id)),
  evolve: (on) => [on(Greeted, (s) => ({ ...s, greeted: true }))],
})

const greet = commandHandler(Greet, async ({ payload: cmd }, ctx) => {
  const g = await ctx.load(Greeting, { id: cmd.id })
  if (g.greeted) throw new Error("already greeted")
  ctx.append(Greeted, { id: cmd.id, who: cmd.who })
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("openTelemetry() span observation (E2E)", () => {
  let harness: ExporterHarness
  let app: App | undefined

  beforeEach(() => {
    harness = installInMemoryExporter()
  })

  afterEach(async () => {
    if (app) await app.stop()
    app = undefined
    await harness.uninstall()
  })

  it("command dispatch through a createApp-composed app emits a span", async () => {
    // given — the caller wraps the commandBus themselves, ordinary function
    // composition, no app mutation.
    const { spanFactory } = openTelemetry()
    const base = inMemoryComponents()
    const components = { ...base, commandBus: tracingCommandBus(base.commandBus, spanFactory) }

    app = createApp({ components, modules: [module("otel-test", Greeting, greet)] })

    // when
    await app.commandGateway.send(Greet, { id: "g-1", who: "world" })

    // then
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.length).toBeGreaterThanOrEqual(1)
    const dispatchSpan = spans.find((s) => s.name.startsWith("dispatch("))
    expect(dispatchSpan).toBeDefined()
    expect(dispatchSpan!.attributes["kronos.message.id"]).toBeDefined()
    expect(dispatchSpan!.attributes["kronos.message.name"]).toBeDefined()
  })

  it("emits no spans when the commandBus is not wrapped with tracingCommandBus", async () => {
    // given — identical app, commandBus left undecorated
    app = createApp({ modules: [module("otel-test", Greeting, greet)] })

    // when
    await app.commandGateway.send(Greet, { id: "g-3", who: "world" })

    // then
    const spans = harness.exporter.getFinishedSpans()
    expect(spans).toHaveLength(0)
  })

  it("handlerEnhancer re-parents a command-kind handler span onto the dispatch span", async () => {
    // given — a commandBus whose delegate itself invokes a handlerEnhancer-
    // wrapped handler; this is exactly the shape registerCommandHandlersNatively
    // uses internally when a caller supplies a handlerEnhancer.
    const { spanFactory, handlerEnhancer } = openTelemetry()

    let handledMessage: CommandMessage | undefined
    const wrappedHandler = handlerEnhancer.wrapHandler(
      async (message: CommandMessage) => {
        handledMessage = message
        return "ok"
      },
      { handlerGroup: "otel-test", messageName: "Greet", messageType: "command" },
    )

    const delegate: CommandBus = {
      dispatch: (message) => wrappedHandler(message as CommandMessage),
      subscribe: () => {},
    }
    const commandBus = tracingCommandBus(delegate, spanFactory)

    // when
    await commandBus.dispatch({
      kind: "command",
      identifier: "id-1",
      name: Greet.name,
      payload: { id: "g-2", who: "world" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
    } as CommandMessage)

    // then — both spans fired, and the handler span is a REAL child of the
    // dispatch span (proves trace context propagated through message metadata).
    expect(handledMessage).toBeDefined()
    const spans = harness.exporter.getFinishedSpans()
    const dispatchSpan = spans.find((s) => s.name.startsWith("dispatch("))
    const handlerSpan = spans.find((s) => s.name === "otel-test.Greet")
    expect(dispatchSpan).toBeDefined()
    expect(handlerSpan).toBeDefined()
    expect(handlerSpan!.parentSpanId).toBe(dispatchSpan!.spanContext().spanId)
  })

  it("handlerEnhancer links (not parents) an event-kind handler span, matching event-processor semantics", async () => {
    // given — an event carrying trace context propagated at append time (what
    // a real event processor's source event carries after `propagateContext`
    // was applied during the command handler's append).
    const { spanFactory, handlerEnhancer } = openTelemetry()

    const rootSpan = spanFactory.createRootTrace("dispatch(otel-test.Greet)").start()
    const propagatedMetadata = rootSpan.runActive!(() => spanFactory.propagateContext({
      kind: "event",
      identifier: "evt-1",
      name: Greeted.name,
      version: "1",
      payload: { id: "g-4", who: "world" },
      metadata: emptyMetadata(),
      timestamp: Date.now(),
      tags: [{ key: "id", value: "g-4" }],
    } as EventMessage).metadata)
    rootSpan.end()

    const wrappedHandler = handlerEnhancer.wrapHandler(
      async (_message: EventMessage) => "handled",
      { handlerGroup: "greet-projection", messageName: "Greeted", messageType: "event" },
    )

    // when — process the event in a NEW trace, as a real event processor does
    await wrappedHandler({
      kind: "event",
      identifier: "evt-1",
      name: Greeted.name,
      version: "1",
      payload: { id: "g-4", who: "world" },
      metadata: propagatedMetadata,
      timestamp: Date.now(),
      tags: [{ key: "id", value: "g-4" }],
    } as EventMessage)

    // then — the handler span is a NEW trace, LINKED back to the producing
    // span (not parented to it) — the originating trace may be long finished
    // by the time an asynchronous processor handles the event.
    const spans = harness.exporter.getFinishedSpans()
    const rootDispatchSpan = spans.find((s) => s.name === "dispatch(otel-test.Greet)")
    const handlerSpan = spans.find((s) => s.name === "greet-projection.Greeted")
    expect(rootDispatchSpan).toBeDefined()
    expect(handlerSpan).toBeDefined()
    expect(handlerSpan!.parentSpanId).toBeUndefined()
    expect(handlerSpan!.links.length).toBeGreaterThanOrEqual(1)
    expect(handlerSpan!.links[0]!.context.spanId).toBe(rootDispatchSpan!.spanContext().spanId)
  })
})
