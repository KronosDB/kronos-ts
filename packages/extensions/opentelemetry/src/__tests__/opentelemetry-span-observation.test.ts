/**
 * E2E span-observation test for the native `openTelemetry()` extension.
 *
 * Resolves RESEARCH Open Question #2 (EXT-04 acceptance) by exercising a real
 * App through a real OTel TracerProvider with an `InMemorySpanExporter` and
 * asserting that:
 *   1. command dispatch through the App emits a span
 *   2. event handler invocation emits a child span
 *   3. without `.use(openTelemetry())` no spans are emitted (control case)
 *
 * Span name expectations (from packages/messaging/src/tracing-command-bus.ts +
 * tracing-handler-enhancer.ts):
 *   - command dispatch: `dispatch(<commandQualifiedName>)`
 *   - command handle:   `handle(<commandQualifiedName>)`
 *   - handler enhancer: `<handlerGroup>.<messageName>`
 */
import { afterEach, beforeEach, describe, expect, it } from "bun:test"
import { trace } from "@opentelemetry/api"
import { InMemorySpanExporter, SimpleSpanProcessor } from "@opentelemetry/sdk-trace-base"
import { NodeTracerProvider } from "@opentelemetry/sdk-trace-node"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  eventHandler,
  EventCriteria,
  subscribingProcessor,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { append, load } from "@kronos-ts/eventsourcing"
import { kronos, type RunningApp } from "@kronos-ts/core"
import { openTelemetry } from "../opentelemetry.js"

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
  evolve: [on(Greeted, (s: GreetState) => ({ ...s, greeted: true }))],
})

const greet = commandHandler(Greet, async (cmd, _metadata) => {
  const g = await load(Greeting, { id: cmd.id })
  if (g.greeted) throw new Error("already greeted")
  append(Greeted, { id: cmd.id, who: cmd.who })
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("openTelemetry() span observation (E2E)", () => {
  let harness: ExporterHarness
  let running: RunningApp | undefined

  beforeEach(() => {
    harness = installInMemoryExporter()
  })

  afterEach(async () => {
    await running?.stop()
    running = undefined
    await harness.uninstall()
  })

  it("command dispatch emits a span", async () => {
    // given
    running = await kronos({ quiet: true })
      .states(Greeting)
      .commands(greet)
      .use(openTelemetry())
      .start()

    // when
    await running.commandGateway.send(Greet, { id: "g-1", who: "world" })

    // then
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.length).toBeGreaterThanOrEqual(1)
    // tracing-command-bus.ts emits a `dispatch(<name>)` span and a `handle(<name>)`
    // span. The message attributes (kronos.message.name + kronos.message.id) carry
    // the precise QualifiedName the handler dispatched.
    const dispatchSpan = spans.find((s) => s.name.startsWith("dispatch("))
    expect(dispatchSpan).toBeDefined()
    expect(dispatchSpan!.attributes["kronos.message.id"]).toBeDefined()
    expect(dispatchSpan!.attributes["kronos.message.name"]).toBeDefined()
  })

  it("event handler invocation emits a child span", async () => {
    // given
    const seen: string[] = []
    const onGreeted = eventHandler(Greeted, async (e) => {
      seen.push(e.id)
    })

    running = await kronos({ quiet: true })
      .states(Greeting)
      .commands(greet)
      .processors(
        subscribingProcessor("greet-projection")
          .eventHandlers(onGreeted)
          .build(),
      )
      .use(openTelemetry())
      .start()

    // when
    await running.commandGateway.send(Greet, { id: "g-2", who: "world" })

    // wait for handler to run
    await new Promise((r) => setTimeout(r, 50))

    // then — both command-side and handler-side spans appear
    const spans = harness.exporter.getFinishedSpans()
    expect(spans.length).toBeGreaterThanOrEqual(2)
    expect(seen).toContain("g-2")

    // dispatch span and at least one handler-enhancer span are present
    const dispatchSpan = spans.find((s) => s.name.startsWith("dispatch("))
    expect(dispatchSpan).toBeDefined()

    // tracingHandlerEnhancerDefinition emits `<group>.<messageName>` spans —
    // the projection group fires for the Greeted event handler.
    const handlerSpan = spans.find((s) => s.name.includes("greet-projection"))
    expect(handlerSpan).toBeDefined()
  })

  it("emits no spans when openTelemetry() extension is not installed", async () => {
    // given — identical app WITHOUT .use(openTelemetry())
    running = await kronos({ quiet: true })
      .states(Greeting)
      .commands(greet)
      .start()

    // when
    await running.commandGateway.send(Greet, { id: "g-3", who: "world" })

    // then
    const spans = harness.exporter.getFinishedSpans()
    expect(spans).toHaveLength(0)
  })
})
