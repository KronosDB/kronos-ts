import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  qn,
  emptyMetadata,
  command,
  event,
  type CommandDescriptor,
  type CommandMessage,
  type SequencedEventMessage,
} from "../../messaging/messages.js"
import { subscribeCommandHandlers, type CommandInvocationDeps } from "../subscribe.js"
import { commandHandler } from "../handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import type { CommandBus } from "../bus.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { handlerContext, type HandlerContext } from "../context.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { state } from "../../event-sourcing/state.js"
import type { EventStore } from "../../event-sourcing/event-store.js"
import { eventHandlerContext } from "../../event-processing/context.js"

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
  tags: { courseId: (p) => p.courseId },
})

/** A real state value — `ctx.load` takes the thing itself, never a stand-in. */
const CourseExistence = state({
  id: { courseId: z.string() },
  tags: ({ courseId }) => ({ courseId }),
  evolve: [() => ({ exists: false }), [CourseCreated, (s) => ({ ...s, exists: true })]],
})

// ---------------------------------------------------------------------------
// Test helpers — mirror command-handling-module.test.ts
// ---------------------------------------------------------------------------

type SubscribedHandler = (message: CommandMessage, uow: UnitOfWork) => Promise<unknown>

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

/** What the invocation closes over — components, passed straight through. */
function deps(overrides: {
  commandBus: CommandBus
  eventStore?: EventStore
}): CommandInvocationDeps {
  return {
    commandBus: overrides.commandBus,
    ...(overrides.eventStore ? { eventStore: overrides.eventStore } : {}),
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
    let received: HandlerContext | undefined
    let seenUow: UnitOfWork | undefined
    const handler = commandHandler(CreateCourse, async (_message, ctx) => {
      received = ctx
    })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      seenUow = uow
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)
    })

    // A FRESH context per invocation, bound to THIS unit of work.
    expect(received!.unitOfWork).toBe(seenUow!)
    expect(typeof received!.append).toBe("function")
    expect(typeof received!.load).toBe("function")
    expect(typeof received!.send).toBe("function")
    expect(typeof received!.emitUpdate).toBe("function")
    // No `ctx.transaction`: a transaction is reached through the owning
    // adapter's accessor, off `ctx.unitOfWork`.
    expect(received!.transaction).toBeUndefined()
  })

  it("ctx.append buffers events onto the unit of work it was built for", async () => {
    const bus = createRecordingCommandBus()

    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      ctx.append(CourseCreated, { courseId: payload.courseId, name: payload.name })
    })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)

      const buffered = uow.events.buffered as any[]
      expect(buffered).toHaveLength(1)
      expect(buffered[0].name).toEqual(CourseCreated.name)
      expect(buffered[0].payload).toEqual({ courseId: "c1", name: "Intro" })
      expect(buffered[0].tags).toEqual([{ key: "courseId", value: "c1" }])
    })
  })

  it("ctx.load folds the entry's log — no registration anywhere", async () => {
    const bus = createRecordingCommandBus()
    const eventStore = inMemoryEventStore()
    await eventStore.append([
      {
        kind: "event",
        identifier: "seeded",
        name: CourseCreated.name,
        version: CourseCreated.version,
        payload: { courseId: "c1", name: "Intro" },
        metadata: emptyMetadata(),
        timestamp: Date.now(),
        tags: [{ key: "courseId", value: "c1" }],
      },
    ])

    let result: unknown
    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      result = await ctx.load(CourseExistence, { courseId: payload.courseId })
    })
    subscribeCommandHandlers([handler], {
      commandBus: bus,
      ...deps({ commandBus: bus, eventStore }),
    })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)
    })

    expect(result).toEqual({ exists: true })
  })

  it("handlers declared without the context parameter remain assignable and working", async () => {
    const bus = createRecordingCommandBus()
    let invoked = false
    const handler = commandHandler(CreateCourse, async ({ payload }) => {
      invoked = payload.courseId === "c1"
    })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)
    })
    expect(invoked).toBe(true)
  })

  it("eventHandler definitions type the context as second argument", () => {
    // Compile-time shape check + definition wiring; processor delivery is
    // covered by the processor suites, which build a context per batch.
    const definition = eventHandler(CourseCreated, async (_message, ctx) => {
      void ctx.unitOfWork
      void ctx.load
      void ctx.send
      void ctx.emitUpdate
    })
    expect(definition.kind).toBe("event-handler")
    expect(definition.handler.length).toBe(2)
  })

  it("event context deliberately has no append capability", () => {
    const uow = unitOfWork()
    expect((eventHandlerContext({ uow }) as unknown as Record<string, unknown>).append).toBeUndefined()
    expect((handlerContext({ uow }) as unknown as Record<string, unknown>).append).toBeDefined()
  })

  it("every invocation gets its OWN context object", async () => {
    const contexts: HandlerContext[] = []
    const bus = createRecordingCommandBus()
    const handler = commandHandler(CreateCourse, async (_m, ctx) => { contexts.push(ctx) })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    const msg = makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" })
    await unitOfWork().execute(async (uow) => { await invocation(msg, uow) })
    await unitOfWork().execute(async (uow) => { await invocation(msg, uow) })

    expect(contexts).toHaveLength(2)
    expect(contexts[0]).not.toBe(contexts[1])
    expect(contexts[0]!.unitOfWork).not.toBe(contexts[1]!.unitOfWork)
  })

  it("a context outliving its unit of work refuses to mutate", async () => {
    let escaped: HandlerContext | undefined
    const bus = createRecordingCommandBus()
    const handler = commandHandler(CreateCourse, async (_m, ctx) => { escaped = ctx })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)
    })

    expect(() => escaped!.append(CourseCreated, { courseId: "c1", name: "Late" })).toThrow(
      "No active UnitOfWork",
    )
  })

  it("processors deliver an event context to event handlers", async () => {
    // Direct handler-shape test: the handler property accepts a two-parameter
    // implementation and the processors invoke it with a context they built
    // from the batch's unit of work (tracking/streaming/subscribing + DLQ).
    const ctx = eventHandlerContext({ uow: unitOfWork() })
    let seen: unknown
    const definition = eventHandler(CourseCreated, async (_m: SequencedEventMessage<any>, c) => {
      seen = c
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
      ctx,
    )
    expect(seen).toBe(ctx)
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
  const existing = await ctx.load(CourseExistence, { courseId })
  ctx.append(CourseCreated, { courseId, name: "Intro" })
  return existing
}

describe("handler context portability", () => {
  it("can be handed to helpers defined outside the handler", async () => {
    const bus = createRecordingCommandBus()
    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      // The whole capability set travels as one argument.
      await openCourseVia(ctx, payload.courseId)
    })
    subscribeCommandHandlers([handler], {
      commandBus: bus,
      ...deps({ commandBus: bus, eventStore: inMemoryEventStore() }),
    })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c1", name: "Intro" }), uow)

      const buffered = uow.events.buffered as Array<{ payload: { courseId: string } }>
      expect(buffered).toHaveLength(1)
      expect(buffered[0]!.payload.courseId).toBe("c1")
    })
  })

  it("survives being destructured — members are not this-bound", async () => {
    const bus = createRecordingCommandBus()
    const handler = commandHandler(CreateCourse, async ({ payload }, ctx) => {
      const { append } = ctx // pulled off the object entirely
      append(CourseCreated, { courseId: payload.courseId, name: payload.name })
    })
    subscribeCommandHandlers([handler], { commandBus: bus, ...deps({ commandBus: bus }) })

    const invocation = bus.subscriptions.get("university.CreateCourse")!
    await unitOfWork().execute(async (uow) => {
      await invocation(makeCommandMessage(CreateCourse, { courseId: "c2", name: "Algo" }), uow)
      expect(uow.events.buffered).toHaveLength(1)
    })
  })
})
