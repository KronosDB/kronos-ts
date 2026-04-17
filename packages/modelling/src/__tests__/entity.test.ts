import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import { event, on, EventCriteria } from "@kronos-ts/messaging"
import { eventSourcedEntity } from "../entity.js"

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

describe("eventSourcedEntity()", () => {
  it("creates an entity definition with evolvers", () => {
    const CourseEntity = eventSourcedEntity({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [
        on(CourseCreated, (state: CourseState, event) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })),
        on(CourseCapacityChanged, (state: CourseState, event) => ({
          ...state,
          capacity: event.capacity,
        })),
      ],
    })

    expect(CourseEntity.kind).toBe("entity-module")
    expect(CourseEntity.name).toBe("Course")
    expect(CourseEntity.evolvers).toHaveLength(2)
  })

  it("create() returns initial state", () => {
    const CourseEntity = eventSourcedEntity({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [],
    })

    const initial = CourseEntity.create({ courseId: "any" })

    expect(initial).toEqual({ created: false, name: "", capacity: 0 })
  })

  it("criteria resolves from entity id", () => {
    const CourseEntity = eventSourcedEntity({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [],
    })

    const criteria = CourseEntity.criteria({ courseId: "cs-101" })

    expect(criteria.kind).toBe("tags")
    if (criteria.kind === "tags") {
      expect(criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
    }
  })

  it("evolvers transform state correctly", () => {
    const CourseEntity = eventSourcedEntity({
      name: "Course",
      id: { courseId: z.string() },
      initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
      criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
      evolve: [
        on(CourseCreated, (state: CourseState, event) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })),
        on(CourseCapacityChanged, (state: CourseState, event) => ({
          ...state,
          capacity: event.capacity,
        })),
      ],
    })

    // Manually apply evolvers to verify they work
    let state = CourseEntity.create({ courseId: "cs-101" })

    const createEvolver = CourseEntity.evolvers[0]!
    state = createEvolver.evolve(
      state,
      { courseId: "cs-101", name: "Intro to CS", capacity: 30 },
      { courseId: "cs-101" },
    )
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 30 })

    const capacityEvolver = CourseEntity.evolvers[1]!
    state = capacityEvolver.evolve(
      state,
      { courseId: "cs-101", capacity: 50 },
      { courseId: "cs-101" },
    )
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 50 })
  })
})
