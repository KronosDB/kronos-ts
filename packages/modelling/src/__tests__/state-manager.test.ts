import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { tag } from "@kronos-ts/common"
import { EventCriteria } from "@kronos-ts/messaging"
import { state } from "../state.js"
import { createStateManager } from "../state-manager.js"
import type { StateRepository } from "../state-manager.js"

// -- Fixtures --

type CourseState = { created: boolean; name: string }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "" }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: (on) => [],
})

describe("StateManager", () => {
  it("loads state from a registered repository", async () => {
    const stateManager = createStateManager()

    const stubRepo: StateRepository<{ courseId: string }, CourseState> = {
      stateName: "Course",
      load: async (id) => ({
        state: { created: true, name: `Course ${id.courseId}` },
        sourcingInfo: {
          criteria: EventCriteria.havingTags(tag("courseId", id.courseId)),
          markerPosition: 0n,
        },
      }),
    }

    stateManager.register(Course, stubRepo)

    const result = await stateManager.load(Course, { courseId: "cs-101" })

    expect(result.state).toEqual({ created: true, name: "Course cs-101" })
  })

  it("throws when no repository is registered for state", async () => {
    const stateManager = createStateManager()

    expect(stateManager.load(Course, { courseId: "cs-101" })).rejects.toThrow(
      'No repository registered for state "Course"',
    )
  })
})
