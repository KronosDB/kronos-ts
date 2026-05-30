import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import { event, on, EventCriteria, type EventMessage } from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { createInMemoryEventStore } from "../in-memory-event-store.js"
import { createEventSourcedRepository } from "../event-sourced-repository.js"

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

const StudentSubscribedToCourse = event({
  name: qn("university", "StudentSubscribedToCourse"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

type CourseState = {
  created: boolean
  name: string
  capacity: number
}

const Course = state({
  name: "Course",
  id: { courseId: z.string() },
  initial: (_id) => ({ created: false, name: "", capacity: 0 }) as CourseState,
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (state: CourseState, { payload: e }) => ({
      ...state, created: true, name: e.name, capacity: e.capacity,
    })),
    on(CourseCapacityChanged, (state: CourseState, { payload: e }) => ({
      ...state, capacity: e.capacity,
    })),
  ],
})

function eventMsg(descriptor: { name: { namespace: string; name: string }; version?: string; tags?: (p: any) => Array<{ key: string; value: string }> }, payload: any): EventMessage {
  const tags = descriptor.tags ? descriptor.tags(payload) : []
  return {
    identifier: generateIdentifier(),
    name: qn(descriptor.name.namespace, descriptor.name.name),
    version: descriptor.version ?? "1.0",
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags,
  }
}

describe("EventSourcedRepository", () => {
  it("returns initial state when no events exist", async () => {
    const store = createInMemoryEventStore()
    const repo = createEventSourcedRepository(Course, store)

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then
    expect(state).toEqual({ created: false, name: "", capacity: 0 })
  })

  it("folds events into state through evolvers", async () => {
    const store = createInMemoryEventStore()
    const repo = createEventSourcedRepository(Course, store)

    // given
    await store.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }),
    ])

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 30 })
  })

  it("applies multiple events in order", async () => {
    const store = createInMemoryEventStore()
    const repo = createEventSourcedRepository(Course, store)

    // given
    await store.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }),
      eventMsg(CourseCapacityChanged, { courseId: "cs-101", capacity: 50 }),
    ])

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 50 })
  })

  it("ignores events for other entities", async () => {
    const store = createInMemoryEventStore()
    const repo = createEventSourcedRepository(Course, store)

    // given
    await store.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }),
      eventMsg(CourseCreated, { courseId: "cs-102", name: "Data Structures", capacity: 25 }),
    ])

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 30 })
  })

  it("ignores events with no matching evolver", async () => {
    const store = createInMemoryEventStore()
    const repo = createEventSourcedRepository(Course, store)

    // given — StudentSubscribedToCourse has courseId tag but no evolver on Course
    await store.append([
      eventMsg(CourseCreated, { courseId: "cs-101", name: "Intro to CS", capacity: 30 }),
      eventMsg(StudentSubscribedToCourse, { courseId: "cs-101", studentId: "stu-001" }),
    ])

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then — StudentSubscribedToCourse is silently ignored
    expect(state).toEqual({ created: true, name: "Intro to CS", capacity: 30 })
  })

  describe("multi-stream entity (DCB)", () => {
    type SubscriptionState = {
      courseId: string
      courseExists: boolean
      courseCapacity: number
      studentsInCourse: number
      studentEnrolled: boolean
    }

    const StudentEnrolledInFaculty = event({
      name: qn("university", "StudentEnrolledInFaculty"),
      payload: z.object({ studentId: z.string() }),
      tags: (p) => [tag("studentId", p.studentId)],
    })

    const Subscription = state({
      name: "CourseSubscription",
      id: { courseId: z.string(), studentId: z.string() },
      initial: (id): SubscriptionState => ({
        courseId: id.courseId,
        courseExists: false,
        courseCapacity: 0,
        studentsInCourse: 0,
        studentEnrolled: false,
      }),
      criteria: (id) =>
        EventCriteria.either(
          EventCriteria.havingTags(tag("courseId", id.courseId)),
          EventCriteria.havingTags(tag("studentId", id.studentId)),
        ),
      evolve: [
        on(CourseCreated, (state: SubscriptionState, { payload: e }) => ({
          ...state, courseExists: true, courseCapacity: e.capacity,
        })),
        on(StudentEnrolledInFaculty, (state: SubscriptionState) => ({
          ...state, studentEnrolled: true,
        })),
        on(StudentSubscribedToCourse, (state: SubscriptionState, { payload: e }) => {
          return {
            ...state,
            studentsInCourse: e.courseId === state.courseId
              ? state.studentsInCourse + 1 : state.studentsInCourse,
          }
        }),
      ],
    })

    it("sources state from multiple event streams", async () => {
      const store = createInMemoryEventStore()
      const repo = createEventSourcedRepository(Subscription, store)

      // given — events across two streams
      await store.append([
        eventMsg(CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }),
        eventMsg(StudentEnrolledInFaculty, { studentId: "stu-001" }),
        eventMsg(StudentSubscribedToCourse, { courseId: "cs-101", studentId: "stu-002" }),
      ])

      // when
      const { state } = await repo.load({ courseId: "cs-101", studentId: "stu-001" })

      // then
      expect(state.courseExists).toBe(true)
      expect(state.courseCapacity).toBe(30)
      expect(state.studentEnrolled).toBe(true)
      expect(state.studentsInCourse).toBe(1)
    })
  })
})
