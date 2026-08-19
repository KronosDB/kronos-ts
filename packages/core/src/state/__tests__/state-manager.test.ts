import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { state } from "../state.js"
import { stateManager } from "../state-manager.js"
import type { StateRepository } from "../state-manager.js"

// -- Fixtures --

type CourseState = { created: boolean; name: string }

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "" }) as CourseState,
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [],
})

describe("StateManager", () => {
  it("loads state from a registered repository", async () => {
    const manager = stateManager()

    const stubRepo: StateRepository<{ courseId: string }, CourseState> = {
      stateName: "Course",
      load: async (id) => ({
        state: { created: true, name: `Course ${id.courseId}` },
        sourcingInfo: {
          query: { tags: { courseId: id.courseId } },
          markerPosition: 0n,
        },
      }),
    }

    manager.register(Course, stubRepo)

    const result = await manager.load(Course, { courseId: "cs-101" })

    expect(result.state).toEqual({ created: true, name: "Course cs-101" })
  })

  it("throws when no repository is registered for state", async () => {
    const manager = stateManager()

    expect(manager.load(Course, { courseId: "cs-101" })).rejects.toThrow(
      'No repository registered for state "Course"',
    )
  })
})
