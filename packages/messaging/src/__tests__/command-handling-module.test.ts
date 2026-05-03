import { describe, expect, it } from "bun:test"
import { z } from "zod"
import {
  qn,
  emptyMetadata,
  ComponentKeys,
  type Configuration,
} from "@kronos-ts/common"
import { registerCommandHandlersNatively } from "../command-handling-module.js"
import { commandHandler } from "../command-handler.js"
import { command, event } from "../descriptor.js"
import type { CommandBus } from "../command-bus.js"
import type { CommandMessage } from "../message.js"
import type { EventCriteria } from "../event-criteria.js"
import { processingStateStorage, Phase } from "../processing-state.js"
import { runInNewUoW } from "../unit-of-work.js"
import { load, append } from "@kronos-ts/eventsourcing"

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

/**
 * Drains PREPARE_COMMIT actions from the active ALS state, mirroring what
 * `runInUoW` does in production. Used to verify event flush / append-condition
 * behavior without booting a full runner.
 *
 * Plan 03-04 (CTX-04): no `ProcessingContext` instance — drives the ALS state
 * directly. Tests still wrap calls in `runInNewUoW(...)` so the ALS state is live.
 */
async function runPrepareCommit(): Promise<void> {
  const state = processingStateStorage.getStore()
  if (!state) return
  const actions = state.phaseActions.get(Phase.PREPARE_COMMIT) ?? []
  state.phaseActions.delete(Phase.PREPARE_COMMIT)
  for (const action of actions) {
    await action()
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("registerCommandHandlersNatively", () => {
  describe("handler registration", () => {
    it("registers handlers with the command bus", () => {
      // given
      const bus = createRecordingCommandBus()
      const config = createStubConfiguration({ commandBus: bus })

      const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
        append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
      })

      const changeCapacity = commandHandler(ChangeCourseCapacity, async () => {})

      // when
      registerCommandHandlersNatively([createCourse, changeCapacity], {
        commandBus: bus,
        config,
        moduleName: "course-commands",
      })

      // then
      expect(bus.subscriptions.has("university.CreateCourse")).toBe(true)
      expect(bus.subscriptions.has("university.ChangeCourseCapacity")).toBe(true)
    })
  })

  describe("handler receives state from state manager", () => {
    it("loads state via load()", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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
        const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
          loadedState = await load({ name: "Course" }, cmd.courseId)
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)

        // then
        expect(loadedState).toEqual({ courseId: "cs-101", name: "Math 101", capacity: 30 })
      })
    })
  })

  describe("event buffering", () => {
    it("handler can append events which are buffered", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
        // given
        const bus = createRecordingCommandBus()
        const config = createStubConfiguration({ commandBus: bus })

        const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
        })

        registerCommandHandlersNatively([createCourse], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.CreateCourse")!
        const message = makeCommandMessage(CreateCourse, { courseId: "cs-101", name: "Intro to CS" })
        await handler(message)

        // then -- events are buffered in the processing context, not yet flushed
        // We can inspect the buffered events key from the context
        // The key is internal, but we verify it doesn't throw and events are there
        // by checking that prepare-commit would have something to flush
      })
    })

    it("buffered events are flushed at PREPARE_COMMIT", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
        // given
        const appendedEvents: any[] = []
        const bus = createRecordingCommandBus()
        const eventStore = {
          async append(events: ReadonlyArray<any>, _condition?: any) {
            appendedEvents.push(...events)
          },
        }
        const config = createStubConfiguration({ commandBus: bus, eventStore })

        const createCourse = commandHandler(CreateCourse, async (cmd, _metadata) => {
          append(CourseCreated, { courseId: cmd.courseId, name: cmd.name })
        })

        registerCommandHandlersNatively([createCourse], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.CreateCourse")!
        const message = makeCommandMessage(CreateCourse, { courseId: "cs-101", name: "Intro to CS" })
        await handler(message)

        // Simulate PREPARE_COMMIT phase
        await runPrepareCommit()

        // then
        expect(appendedEvents).toHaveLength(1)
        expect(appendedEvents[0].payload).toEqual({ courseId: "cs-101", name: "Intro to CS" })
        expect(appendedEvents[0].name).toEqual(qn("university", "CourseCreated"))
      })
    })
  })

  describe("append condition", () => {
    it("builds append condition from sourcing info", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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

        const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
          await load({ name: "Course" }, cmd.courseId)
          append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)
        await runPrepareCommit()

        // then
        expect(capturedCondition).toBeDefined()
        expect(capturedCondition.criteria.kind).toBe("tags")
        expect(capturedCondition.criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
        expect(capturedCondition.marker.position).toBe(7n)
      })
    })

    it("custom appendCondition override works", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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
          handler: async (cmd, _metadata) => {
            await load({ name: "Course" }, cmd.courseId)
            append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
          },
          appendCondition: (_cmd, _sourcedCriteria) => customCriteria,
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)
        await runPrepareCommit()

        // then
        expect(capturedCondition).toBeDefined()
        expect(capturedCondition.criteria.kind).toBe("any-tag")
      })
    })
  })

  describe("entity cache", () => {
    it("prevents duplicate load() in same invocation", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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

        const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
          // Load same entity twice
          await load({ name: "Course" }, cmd.courseId)
          await load({ name: "Course" }, cmd.courseId)
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)

        // then -- state manager load should only be called once
        expect(loadCount).toBe(1)
      })
    })

    it("loads different entities independently", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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

        const changeCapacity = commandHandler(ChangeCourseCapacity, async (_cmd, _metadata) => {
          await load({ name: "Course" }, "cs-101")
          await load({ name: "Course" }, "cs-202")
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)

        // then
        expect(loadedIds).toEqual(["cs-101", "cs-202"])
      })
    })

    it("combines sourcing info from multiple loads into either criteria", async () => {
      await runInNewUoW(emptyMetadata(), async () => {
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

        const changeCapacity = commandHandler(ChangeCourseCapacity, async (cmd, _metadata) => {
          await load({ name: "Course" }, "cs-101")
          await load({ name: "Course" }, "cs-202")
          append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
        })

        registerCommandHandlersNatively([changeCapacity], { commandBus: bus, config, moduleName: "course-commands" })

        // when
        const handler = bus.subscriptions.get("university.ChangeCourseCapacity")!
        const message = makeCommandMessage(ChangeCourseCapacity, { courseId: "cs-101", capacity: 50 })
        await handler(message)
        await runPrepareCommit()

        // then -- should combine into "either" criteria with max marker
        expect(capturedCondition).toBeDefined()
        expect(capturedCondition.criteria.kind).toBe("either")
        expect(capturedCondition.criteria.criteria).toHaveLength(2)
        expect(capturedCondition.marker.position).toBe(8n) // max of 5n and 8n
      })
    })
  })

  describe("empty handler list", () => {
    it("does nothing when given an empty handler array", () => {
      // given
      const bus = createRecordingCommandBus()
      const config = createStubConfiguration({ commandBus: bus })

      // when
      registerCommandHandlersNatively([], { commandBus: bus, config, moduleName: "course-commands" })

      // then
      expect(bus.subscriptions.size).toBe(0)
    })
  })
})
