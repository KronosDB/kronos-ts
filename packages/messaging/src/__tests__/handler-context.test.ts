import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import { registerCommandHandlersNatively, type MinimalConfiguration } from "../command-handling-module.js"
import { commandHandler } from "../command-handler.js"
import { eventHandler } from "../event-handler.js"
import { command, event, type CommandDescriptor } from "../descriptor.js"
import type { CommandBus } from "../command-bus.js"
import type { CommandMessage, SequencedEventMessage } from "../message.js"
import { processingStateStorage } from "../processing-state.js"
import { runInNewUoW } from "../unit-of-work.js"
import { HANDLER_CONTEXT, EVENT_HANDLER_CONTEXT, type HandlerContext } from "../handler-context.js"
import { BUFFERED_EVENTS_KEY } from "@kronos-ts/eventsourcing"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: (p) => [{ key: "courseId", value: p.courseId }],
})

// ---------------------------------------------------------------------------
// Test helpers — mirror command-handling-module.test.ts
// ---------------------------------------------------------------------------

const CONFIG_KEYS = {
  COMMAND_BUS: "commandBus",
  STATE_MANAGER: "stateManager",
} as const

type SubscribedHandler = (message: CommandMessage) => Promise<unknown>

function createRecordingCommandBus(): CommandBus & { subscriptions: Map<string, SubscribedHandler> } {
  const subscriptions = new Map<string, SubscribedHandler>()
  return {
    subscriptions,
    async dispatch(_message) {
      throw new Error("dispatch not used in these tests")
    },
    subscribe(commandName, handler) {
      subscriptions.set(commandName, handler)
    },
  }
}

function createStubConfiguration(overrides: {
  commandBus: CommandBus
  stateManager?: { load: (entity: any, id: any) => Promise<any> }
}): MinimalConfiguration {
  const components = new Map<string, unknown>()
  components.set(CONFIG_KEYS.COMMAND_BUS, overrides.commandBus)
  if (overrides.stateManager) {
    components.set(CONFIG_KEYS.STATE_MANAGER, overrides.stateManager)
  }
  return {
    getComponent<T>(type: string): T {
      const c = components.get(type)
      if (!c) throw new Error(`Component not found: ${type}`)
      return c as T
    },
    getOptionalComponent<T>(type: string): T | undefined {
      return components.get(type) as T | undefined
    },
    hasComponent(type: string): boolean {
      return components.has(type)
    },
  }
}

function makeCommandMessage(descriptor: CommandDescriptor, payload: any): CommandMessage {
  return {
    identifier: `cmd-${Date.now()}`,
    name: descriptor.name,
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("handler context", () => {
  it("command handlers receive the HandlerContext as second argument", async () => {
    const bus = createRecordingCommandBus()
    const config = createStubConfiguration({ commandBus: bus })

    let received: HandlerContext | undefined
    const handler = commandHandler(CreateCourse, async (_message, ctx) => {
      received = ctx
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }))
    })

    expect(received).toBe(HANDLER_CONTEXT)
    expect(typeof received!.append).toBe("function")
    expect(typeof received!.load).toBe("function")
    expect(typeof received!.send).toBe("function")
    expect(typeof received!.emitUpdate).toBe("function")
    expect(typeof received!.transaction).toBe("function")
  })

  it("ctx.append buffers events into the active UnitOfWork identically to module-level append", async () => {
    const bus = createRecordingCommandBus()
    const config = createStubConfiguration({ commandBus: bus })

    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      ctx.append(CourseCreated, { courseId: payload.courseId, name: payload.name })
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }))

      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as any[]
      expect(buffered).toHaveLength(1)
      expect(buffered[0].name).toEqual(CourseCreated.name)
      expect(buffered[0].payload).toEqual({ courseId: "c1", name: "Intro" })
      expect(buffered[0].tags).toEqual([{ key: "courseId", value: "c1" }])
    })
  })

  it("ctx.load resolves state through the configured state manager", async () => {
    const bus = createRecordingCommandBus()
    const loaded: unknown[] = []
    const stateManager = {
      load: async (_module: any, id: any) => {
        loaded.push(id)
        return { state: { exists: true }, sourcingInfo: { criteria: { kind: "tags", tags: [] }, markerPosition: 0n } }
      },
    }
    const config = createStubConfiguration({ commandBus: bus, stateManager })

    let result: unknown
    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      result = await ctx.load({ name: "CourseExistence" }, { courseId: payload.courseId })
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }))
    })

    expect(result).toEqual({ exists: true })
    expect(loaded).toEqual([{ courseId: "c1" }])
  })

  it("handlers declared without the context parameter remain assignable and working", async () => {
    const bus = createRecordingCommandBus()
    const config = createStubConfiguration({ commandBus: bus })

    let invoked = false
    const handler = commandHandler(CreateCourse, async ({ payload }) => {
      invoked = payload.courseId === "c1"
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }))
    })
    expect(invoked).toBe(true)
  })

  it("eventHandler definitions type the context as second argument", () => {
    // Compile-time shape check + definition wiring; processor delivery is
    // covered by the processor suites, which now pass EVENT_HANDLER_CONTEXT.
    const definition = eventHandler(CourseCreated, async (_message, ctx) => {
      void ctx.transaction
      void ctx.load
      void ctx.send
      void ctx.emitUpdate
    })
    expect(definition.kind).toBe("event-handler")
    expect(definition.handler.length).toBe(2)
  })

  it("event context deliberately has no append capability", () => {
    expect((EVENT_HANDLER_CONTEXT as Record<string, unknown>).append).toBeUndefined()
    expect((HANDLER_CONTEXT as Record<string, unknown>).append).toBeDefined()
  })

  it("context instances are frozen", () => {
    expect(Object.isFrozen(HANDLER_CONTEXT)).toBe(true)
    expect(Object.isFrozen(EVENT_HANDLER_CONTEXT)).toBe(true)
  })

  it("processors deliver the event context to event handlers", async () => {
    // Direct registration-shape test: the handler property accepts a
    // two-parameter implementation and the processors invoke it with
    // EVENT_HANDLER_CONTEXT (wired in tracking/streaming/subscribing + DLQ).
    let seen: unknown
    const definition = eventHandler(CourseCreated, async (_m: SequencedEventMessage<any>, ctx) => {
      seen = ctx
    })
    await definition.handler(
      {
        kind: "event",
        identifier: "e1",
        name: CourseCreated.name,
        version: "1.0",
        payload: { courseId: "c1", name: "Intro" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
        tags: [],
        sequence: 1n,
      } as any,
      EVENT_HANDLER_CONTEXT,
    )
    expect(seen).toBe(EVENT_HANDLER_CONTEXT)
  })
})

// ---------------------------------------------------------------------------
// Portability — the point of making capabilities explicit values
// ---------------------------------------------------------------------------

/**
 * A domain helper living OUTSIDE any handler. It takes the context as a plain
 * parameter, which is only possible because the capabilities are values on an
 * object rather than module-level statics bound to an ambient UnitOfWork.
 */
async function openCourseVia(ctx: HandlerContext, courseId: string): Promise<{ exists: boolean }> {
  const existing = (await ctx.load({ name: "CourseExistence" }, { courseId })) as { exists: boolean }
  ctx.append(CourseCreated, { courseId, name: "Intro" })
  return existing
}

describe("handler context portability", () => {
  it("can be handed to helpers defined outside the handler", async () => {
    const bus = createRecordingCommandBus()
    const stateManager = {
      load: async () => ({
        state: { exists: false },
        sourcingInfo: { criteria: { kind: "tags", tags: [] }, markerPosition: 0n },
      }),
    }
    const config = createStubConfiguration({ commandBus: bus, stateManager })

    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      // The whole capability set travels as one argument.
      await openCourseVia(ctx, payload.courseId)
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }))

      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as Array<{ payload: { courseId: string } }>
      expect(buffered).toHaveLength(1)
      expect(buffered[0]!.payload.courseId).toBe("c1")
    })
  })

  it("survives being destructured — members are not this-bound", async () => {
    const bus = createRecordingCommandBus()
    const config = createStubConfiguration({ commandBus: bus })

    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      const { append } = ctx // pulled off the object entirely
      append(CourseCreated, { courseId: payload.courseId, name: payload.name })
    })
    registerCommandHandlersNatively([handler], { commandBus: bus, config })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await runInNewUoW(emptyMetadata(), async () => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c2", name: "Algo" }))
      const state = processingStateStorage.getStore()!
      const buffered = state.resources.get(BUFFERED_EVENTS_KEY.symbol) as unknown[]
      expect(buffered).toHaveLength(1)
    })
  })
})
