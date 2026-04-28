/**
 * Polymorphism integration test — ROADMAP success criterion #4.
 *
 * Proves:
 *   - DEC-04: decorator factories are polymorphic over the interface — replacing the
 *     base commandBus with a mock distributed bus does not affect registered decorators.
 *   - DEC-02: framework defaults are registered via `_registerFrameworkDefaultDecorator`
 *     and exposed as removable handles (`Defaults.commandBus.intercepting`).
 *
 * Pipeline shape verified: tracing(intercepting(mockDistributedBus))
 */
import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
  type CommandBus,
  type CommandMessage,
  createTracingCommandBus,
  noOpSpanFactory,
  type SpanFactory,
} from "@kronos-ts/messaging"
import { qualifiedNameToString } from "@kronos-ts/common"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { load, append } from "@kronos-ts/eventsourcing"
import { kronos } from "../kronos.js"
import { Defaults } from "../defaults-handles.js"

// ─── Minimal domain ───────────────────────────────────────────────────────────

const CreateThing = command({
  name: qn("phase6poly", "CreateThing"),
  payload: z.object({ id: z.string() }),
})

const ThingCreated = event({
  name: qn("phase6poly", "ThingCreated"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})

const ThingEntity = eventSourcedEntity({
  name: "ThingPoly",
  id: { id: z.string() },
  initial: () => ({ created: false }),
  criteria: ({ id }) => EventCriteria.havingTags({ id }),
  evolve: [on(ThingCreated, (s) => ({ ...s, created: true }))],
})

const createThingHandler = commandHandler(CreateThing, async (cmd, _md) => {
  await load(ThingEntity, { id: cmd.id })
  append(ThingCreated, { id: cmd.id })
})

// ─── Mock distributed CommandBus builder ─────────────────────────────────────

function makeMockDistributedBus(): {
  bus: CommandBus
  dispatched: CommandMessage[]
} {
  const dispatched: CommandMessage[] = []
  const subscriptions = new Map<string, (m: CommandMessage) => Promise<unknown>>()
  const bus: CommandBus = {
    async dispatch(message: CommandMessage): Promise<unknown> {
      dispatched.push(message)
      // message.name is a QualifiedName object; use qualifiedNameToString to match subscribe keys
      const key = qualifiedNameToString(message.name)
      const handler = subscriptions.get(key)
      if (!handler) throw new Error(`no handler for ${key}`)
      return handler(message)
    },
    subscribe(name: string, handler: (m: CommandMessage) => Promise<unknown>) {
      subscriptions.set(name, handler)
    },
  }
  return { bus, dispatched }
}

// ─── Mock SpanFactory spy ─────────────────────────────────────────────────────

function makeMockSpanFactory(): { spanFactory: SpanFactory; spanEvents: string[] } {
  const spanEvents: string[] = []
  const base = noOpSpanFactory()
  const spanFactory: SpanFactory = {
    ...base,
    createDispatchSpan(name: string, _msg: any) {
      spanEvents.push(`dispatch:${name}:start`)
      return {
        start() { spanEvents.push(`dispatch:${name}:started`); return this },
        end() { spanEvents.push(`dispatch:${name}:end`) },
        recordException() {},
      }
    },
    createHandlerSpan(name: string, _msg: any) {
      spanEvents.push(`handle:${name}:start`)
      return {
        start() { spanEvents.push(`handle:${name}:started`); return this },
        end() { spanEvents.push(`handle:${name}:end`) },
        recordException() {},
      }
    },
  }
  return { spanFactory, spanEvents }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe("Polymorphism integration — tracing(intercepting(mockDistributedBus))", () => {
  it(
    "DEC-04: replacing commandBus base with mock distributed bus — decorators unaffected, correct pipeline shape",
    async () => {
      const { bus: mockDistributedBus, dispatched } = makeMockDistributedBus()
      const { spanFactory: mockSpans, spanEvents } = makeMockSpanFactory()
      const interceptorWitness: CommandMessage[] = []
      const handlerInterceptorWitness: CommandMessage[] = []

      const app = kronos({ quiet: true })
        .entities(ThingEntity)
        .commands(createThingHandler)
        .set("commandBus", () => mockDistributedBus)
        .commandDispatchInterceptor((m) => {
          interceptorWitness.push(m as CommandMessage)
          return m
        })
        .handlerInterceptor(async (m, next) => {
          handlerInterceptorWitness.push(m as CommandMessage)
          return next()
        })
      app.decorate("commandBus", (inner) => createTracingCommandBus(inner, mockSpans))

      const running = await app.start()
      try {
        await running.commandGateway.send(CreateThing, { id: "p-1" }, emptyMetadata())

        // (a) Tracing span fired (user decorator wraps the chain outermost)
        expect(spanEvents.some((e) => e.startsWith("dispatch:"))).toBe(true)

        // (b) Mock distributed bus received the dispatch
        // (chain bottomed out at mock, not in-memory default — DEC-04)
        expect(dispatched).toHaveLength(1)
        expect(qualifiedNameToString(dispatched[0]!.name)).toContain("CreateThing")

        // (c) Framework intercepting default ran (dispatch interceptor witness collected the message)
        expect(interceptorWitness).toHaveLength(1)

        // (d) Handler interceptor ran (proves intercepting default wired handlerInterceptors)
        expect(handlerInterceptorWitness).toHaveLength(1)

        // (e) Replacing the base did NOT affect any decorator (DEC-04) — proven by (a) + (c) firing.
      } finally {
        await running.stop()
      }
    },
  )
})

describe("removeDecorator(Defaults.commandBus.intercepting) drops the intercepting layer (DEC-02 + DEC-04)", () => {
  it("intercepting layer removed — dispatch interceptors registered via commandDispatchInterceptor() have no effect", async () => {
    const { bus: mockDistributedBus, dispatched } = makeMockDistributedBus()
    const interceptorWitness: CommandMessage[] = []

    const app = kronos({ quiet: true })
      .entities(ThingEntity)
      .commands(createThingHandler)
      .removeDecorator(Defaults.commandBus.intercepting)
      .set("commandBus", () => mockDistributedBus)
      .commandDispatchInterceptor((m) => {
        interceptorWitness.push(m as CommandMessage)
        return m
      })

    const running = await app.start()
    try {
      await running.commandGateway.send(CreateThing, { id: "p-2" }, emptyMetadata())

      // Mock received the dispatch (base replacement still works)
      expect(dispatched).toHaveLength(1)

      // Interceptor witness empty — intercepting layer was removed,
      // so commandDispatchInterceptor() registrations have no effect on dispatch
      expect(interceptorWitness).toHaveLength(0)
    } finally {
      await running.stop()
    }
  })
})
