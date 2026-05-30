import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import { event, on, EventCriteria } from "@kronos-ts/messaging"
import { state } from "../state.js"

// -- Fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

type CourseState = {
  created: boolean
  name: string
  capacity: number
}

describe("state()", () => {
  it("creates an entity definition with evolvers", () => {
    const Course = state({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [
        on(CourseCreated, (state: CourseState, { payload: event }) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })),
        on(CourseCapacityChanged, (state: CourseState, { payload: event }) => ({
          ...state,
          capacity: event.capacity,
        })),
      ],
    })

    expect(Course.kind).toBe("state-module")
    expect(Course.name).toBe("Course")
    expect(Course.evolvers).toHaveLength(2)
  })

  it("create() returns initial state", () => {
    const Course = state({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [],
    })

    const initial = Course.create({ courseId: "any" })

    expect(initial).toEqual({ created: false, name: "", capacity: 0 })
  })

  it("criteria resolves from entity id", () => {
    const Course = state({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [],
    })

    const criteria = Course.criteria({ courseId: "cs-101" })

    expect(criteria.kind).toBe("tags")
    if (criteria.kind === "tags") {
      expect(criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
    }
  })

  it("evolvers transform state correctly", () => {
    const Course = state({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [
        on(CourseCreated, (state: CourseState, { payload: event }) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })),
        on(CourseCapacityChanged, (state: CourseState, { payload: event }) => ({
          ...state,
          capacity: event.capacity,
        })),
      ],
    })

    // Manually apply evolvers to verify they work
    let current = Course.create({ courseId: "cs-101" })

    const createEvolver = Course.evolvers[0]!
    current = createEvolver.evolve(
      current,
      {
        identifier: "evt-1",
        name: CourseCreated.name,
        version: CourseCreated.version,
        payload: { courseId: "cs-101", name: "Intro to CS", capacity: 30 },
        metadata: {},
        timestamp: Date.now(),
        tags: [],
      },
    )
    expect(current).toEqual({ created: true, name: "Intro to CS", capacity: 30 })

    const capacityEvolver = Course.evolvers[1]!
    current = capacityEvolver.evolve(
      current,
      {
        identifier: "evt-2",
        name: CourseCapacityChanged.name,
        version: CourseCapacityChanged.version,
        payload: { courseId: "cs-101", capacity: 50 },
        metadata: {},
        timestamp: Date.now(),
        tags: [],
      },
    )
    expect(current).toEqual({ created: true, name: "Intro to CS", capacity: 50 })
  })
})
