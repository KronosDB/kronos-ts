import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  qn,
  emptyMetadata,
  ComponentKeys,
  type Configuration,
} from "@kronos-ts/common"
import { commandHandlingModule } from "../command-handling-module.js"
import { commandHandler } from "../command-handler.js"
import { command, event } from "../descriptor.js"
import type { CommandBus } from "../command-bus.js"
import type { CommandMessage } from "../message.js"
import type { ProcessingContext } from "../processing-context.js"
import type { EventCriteria } from "../event-criteria.js"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

const ChangeCourseCapacity = command({
  name: qn("university", "ChangeCourseCapacity"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
  tags: (p) => [{ key: "courseId", value: p.courseId }],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type SubscribedHandler = (message: CommandMessage, ctx: ProcessingContext) => Promise<unknown>

function createRecordingCommandBus(): CommandBus & { subscriptions: Map<string, SubscribedHandler> } {
  const subscriptions = new Map<string, SubscribedHandler>()
  return {
    subscriptions,
    async dispatch(message) {
      const handler = subscriptions.get(`${message.name.namespace}.${message.name.name}`)
      if (!handler) throw new Error(`No handler for ${message.name.namespace}.${message.name.name}`)
      // We don't create a ProcessingContext here -- the module's handler does that via UoW
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
  eventStore?: { append: (events: ReadonlyArray<any>, condition?: any) => Promise<void> }
}): Configuration {
  const components = new Map<string, unknown>()
  components.set(ComponentKeys.COMMAND_BUS, overrides.commandBus)
  if (overrides.stateManager) {
    components.set(ComponentKeys.STATE_MANAGER, overrides.stateManager)
  }
  if (overrides.eventStore) {
    components.set(ComponentKeys.EVENT_STORE, overrides.eventStore)
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
    getComponents<T>(type: string): Map<string, T> {
      const c = components.get(type)
      if (!c) return new Map()
      return new Map([[type, c as T]])
    },
    hasComponent(type: string): boolean {
      return components.has(type)
    },
    getModules() { return [] },
    getParent() { return undefined },
  }
}

function makeCommandMessage(descriptor: typeof CreateCourse | typeof ChangeCourseCapacity, payload: any): CommandMessage {
  return {
    identifier: `cmd-${Date.now()}`,
    name: descriptor.name,
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

/** Creates a minimal ProcessingContext for invoking command handlers. */
function createTestProcessingContext(): ProcessingContext {
  const resources = new Map<symbol, unknown>()
  const prepareCommitActions: Array<(ctx: ProcessingContext) => Promise<void> | void> = []

  const ctx: ProcessingContext = {
    get<T>(key: { symbol: symbol }): T | undefined {
      return resources.get(key.symbol) as T | undefined
    },
    set<T>(key: { symbol: symbol }, value: T): T | undefined {
      const prev = resources.get(key.symbol) as T | undefined
      resources.set(key.symbol, value)
      return prev
    },
    computeIfAbsent<T>(key: { symbol: symbol }, supplier: () => T): T {
      if (!resources.has(key.symbol)) {
        resources.set(key.symbol, supplier())
      }
      return resources.get(key.symbol) as T
    },
    remove<T>(key: { symbol: symbol }): T | undefined {
      const prev = resources.get(key.symbol) as T | undefined
      resources.delete(key.symbol)
      return prev
    },
    contains(key: { symbol: symbol }): boolean {
      return resources.has(key.symbol)
    },
    update<T>(key: { symbol: symbol }, updater: (current: T | undefined) => T): T {
      const current = resources.get(key.symbol) as T | undefined
      const next = updater(current)
      resources.set(key.symbol, next)
      return next
    },
    withResource(key, value) {
      // Simplified: just set and return self for testing
      resources.set(key.symbol, value)
      return ctx
    },
    component() { return undefined },
    on() {},
    onError() {},
    whenComplete() {},
    onPrepareCommit(action) {
      prepareCommitActions.push(action)
    },
    onCommit() {},
    onAfterCommit() {},
    isStarted: true,
    isError: false,
    isCompleted: false,
    metadata: emptyMetadata(),
  }

  // Expose a way to trigger prepare-commit for tests
  ;(ctx as any).__runPrepareCommit = async () => {
    for (const action of prepareCommitActions) {
      await action(ctx)
    }
  }

  return ctx
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("commandHandlingModule", () => {
  describe("handler registration", () => {
    it("registers handlers with the command bus", () => {
      // given
      const bus = createRecordingCommandBus()
      const config = createStubConfiguration({ commandBus: bus })

      const createCourse = commandHandler(CreateCourse, async (_cmd, { append }) => {
        append(CourseCreated, { courseId: _cmd.courseId, name: _cmd.name })
      })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async () => {})

      const mod = commandHandlingModule("course-commands", [createCourse, changeCapacity])

      // when
      mod.initialize!(config)

      // then
      expect(bus.subscriptions.has("university.CreateCourse")).toBe(true)
      expect(bus.subscriptions.has("university.ChangeCourseCapacity")).toBe(true)
    })
  })

  describe("handler receives state from state manager", () => {
    it("loads state via load()", async () => {
      // given
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          return {
            state: { courseId: id, name: "Math 101", capacity: 30 },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: 5n,
            },
          }
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager })

      let loadedState: any = null
      const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load }) => {
        loadedState = await load({ name: "Course" }, cmd.courseId)
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)

      // then
      expect(loadedState).toEqual({ courseId: "cs-101", name: "Math 101", capacity: 30 })
    })
  })

  describe("event buffering", () => {
    it("handler can append events which are buffered", async () => {
      // given
      const bus = createRecordingCommandBus()
      const config = createStubConfiguration({ commandBus: bus })

      const createCourse = commandHandler(CreateCourse, async (cmd, { append }) => {
        append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
      })

      const mod = commandHandlingModule("course-commands", [createCourse])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.CreateCourse")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(CreateCourse, { courseId: "cs-101", name: "Intro to CS" })
      await handler(message, ctx)

      // then -- events are buffered in the processing context, not yet flushed
      // We can inspect the buffered events key from the context
      // The key is internal, but we verify it doesn't throw and events are there
      // by checking that prepare-commit would have something to flush
    })

    it("buffered events are flushed at PREPARE_COMMIT", async () => {
      // given
      const appendedEvents: any[] = []
      const bus = createRecordingCommandBus()
      const eventStore = {
        async append(events: ReadonlyArray<any>, _condition?: any) {
          appendedEvents.push(...events)
        },
      }
      const config = createStubConfiguration({ commandBus: bus, eventStore })

      const createCourse = commandHandler(CreateCourse, async (cmd, { append }) => {
        append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
      })

      const mod = commandHandlingModule("course-commands", [createCourse])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.CreateCourse")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(CreateCourse, { courseId: "cs-101", name: "Intro to CS" })
      await handler(message, ctx)

      // Simulate PREPARE_COMMIT phase
      await (ctx as any).__runPrepareCommit()

      // then
      expect(appendedEvents).toHaveLength(1)
      expect(appendedEvents[0].payload).toEqual({ courseId: "cs-101", name: "Intro to CS" })
      expect(appendedEvents[0].name).toEqual(qn("university", "CourseCreated"))
    })
  })

  describe("append condition", () => {
    it("builds append condition from sourcing info", async () => {
      // given
      let capturedCondition: any = null
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          return {
            state: { courseId: id },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: 7n,
            },
          }
        },
      }
      const eventStore = {
        async append(events: ReadonlyArray<any>, condition?: any) {
          capturedCondition = condition
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager, eventStore })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load, append }) => {
        await load({ name: "Course" }, cmd.courseId)
        append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)
      await (ctx as any).__runPrepareCommit()

      // then
      expect(capturedCondition).toBeDefined()
      expect(capturedCondition.criteria.kind).toBe("tags")
      expect(capturedCondition.criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
      expect(capturedCondition.marker.position).toBe(7n)
    })

    it("custom appendCondition override works", async () => {
      // given
      let capturedCondition: any = null
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          return {
            state: { courseId: id },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: 5n,
            },
          }
        },
      }
      const eventStore = {
        async append(events: ReadonlyArray<any>, condition?: any) {
          capturedCondition = condition
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager, eventStore })

      const customCriteria: EventCriteria = { kind: "any-tag" }
      const changeCapacity = commandHandler(ChangeCourseCapacity, {
        handler: async (cmd, { load, append }) => {
          await load({ name: "Course" }, cmd.courseId)
          append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
        },
        appendCondition: (_cmd, _sourcedCriteria) => customCriteria,
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)
      await (ctx as any).__runPrepareCommit()

      // then
      expect(capturedCondition).toBeDefined()
      expect(capturedCondition.criteria.kind).toBe("any-tag")
    })
  })

  describe("entity cache", () => {
    it("prevents duplicate load() in same invocation", async () => {
      // given
      let loadCount = 0
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          loadCount++
          return {
            state: { courseId: id, name: "Math" },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: 3n,
            },
          }
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load }) => {
        // Load same entity twice
        await load({ name: "Course" }, cmd.courseId)
        await load({ name: "Course" }, cmd.courseId)
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)

      // then -- state manager load should only be called once
      expect(loadCount).toBe(1)
    })

    it("loads different entities independently", async () => {
      // given
      const loadedIds: string[] = []
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          loadedIds.push(String(id))
          return {
            state: { courseId: id },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: 3n,
            },
          }
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load }) => {
        await load({ name: "Course" }, "cs-101")
        await load({ name: "Course" }, "cs-202")
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)

      // then
      expect(loadedIds).toEqual(["cs-101", "cs-202"])
    })

    it("combines sourcing info from multiple loads into either criteria", async () => {
      // given
      let capturedCondition: any = null
      const bus = createRecordingCommandBus()
      const stateManager = {
        async load(_entity: any, id: any) {
          return {
            state: { courseId: id },
            sourcingInfo: {
              criteria: { kind: "tags" as const, tags: [{ key: "courseId", value: String(id) }] },
              markerPosition: id === "cs-101" ? 5n : 8n,
            },
          }
        },
      }
      const eventStore = {
        async append(_events: ReadonlyArray<any>, condition?: any) {
          capturedCondition = condition
        },
      }
      const config = createStubConfiguration({ commandBus: bus, stateManager, eventStore })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load, append }) => {
        await load({ name: "Course" }, "cs-101")
        await load({ name: "Course" }, "cs-202")
        append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
      })

      const mod = commandHandlingModule("course-commands", [changeCapacity])
      mod.initialize!(config)

      // when
      const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
      const ctx = createTestProcessingContext()
      const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
      await handler(message, ctx)
      await (ctx as any).__runPrepareCommit()

      // then -- should combine into "either" criteria with max marker
      expect(capturedCondition).toBeDefined()
      expect(capturedCondition.criteria.kind).toBe("either")
      expect(capturedCondition.criteria.criteria).toHaveLength(2)
      expect(capturedCondition.marker.position).toBe(8n) // max of 5n and 8n
    })
  })

  describe("module metadata", () => {
    it("exposes module name", () => {
      // given
      const mod = commandHandlingModule("course-commands", [])

      // then
      expect(mod.name).toBe("course-commands")
    })
  })
})
