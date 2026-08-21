import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, event, type EventMessage } from "../../messaging/messages.js"
import { generateIdentifier } from "../../messaging/identifier.js"
import { state } from "../state.js"
import { inMemoryEventStore } from "../in-memory.js"
import { eventSourcedRepository } from "../repository.js"

// -- Fixtures --

const CourseCreated = event({
  name: qn("university", "CourseCreated"),
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const CourseCapacityChanged = event({
  name: qn("university", "CourseCapacityChanged"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentSubscribedToCourse = event({
  name: qn("university", "StudentSubscribedToCourse"),
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
})

type CourseState = {
  created: boolean
  name: string
  capacity: number
}

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    () => ({ created: false, name: "", capacity: 0 }) as CourseState,
    [CourseCreated, (state, { payload: e }) => ({
      ...state, created: true, name: e.name, capacity: e.capacity,
    })],
    [CourseCapacityChanged, (state, { payload: e }) => ({
      ...state, capacity: e.capacity,
    })],
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
    const store = inMemoryEventStore()
    const repo = eventSourcedRepository(Course, store)

    // when
    const { state } = await repo.load({ courseId: "cs-101" })

    // then
    expect(state).toEqual({ created: false, name: "", capacity: 0 })
  })

  it("folds events into state through evolvers", async () => {
    const store = inMemoryEventStore()
    const repo = eventSourcedRepository(Course, store)

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
    const store = inMemoryEventStore()
    const repo = eventSourcedRepository(Course, store)

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
    const store = inMemoryEventStore()
    const repo = eventSourcedRepository(Course, store)

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
    const store = inMemoryEventStore()
    const repo = eventSourcedRepository(Course, store)

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
      tags: { studentId: (p) => p.studentId },
    })

    const Subscription = state({
      id: { courseId: z.string(), studentId: z.string() },
      // ONE record, spanning two streams. A course event carries only
      // `courseId` and a faculty enrolment only `studentId`; the derivation
      // scopes each key to the event types that declare it and ORs the result,
      // so the multi-stream scope falls out of the plain record.
      tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
      evolve: [
        // The initial state is the evolver of NOTHING, but it is not the knower of
        // nothing: it is handed the id this fold is being run for, so WHICH
        // course this is has never had to be learned from an event. That
        // matters here — `studentsInCourse` counts subscriptions against
        // `state.courseId`, and before it took the id that counter could
        // only start once `CourseCreated` had been folded.
        (id): SubscriptionState => ({
          courseId: id.courseId,
          courseExists: false,
          courseCapacity: 0,
          studentsInCourse: 0,
          studentEnrolled: false,
        }),
        [CourseCreated, (state, { payload: e }) => ({
          ...state, courseExists: true, courseCapacity: e.capacity,
        })],
        [StudentEnrolledInFaculty, (state) => ({
          ...state, studentEnrolled: true,
        })],
        [StudentSubscribedToCourse, (state, { payload: e }) => {
          return {
            ...state,
            studentsInCourse: e.courseId === state.courseId
              ? state.studentsInCourse + 1 : state.studentsInCourse,
          }
        }],
      ],
    })

    it("sources state from multiple event streams", async () => {
      const store = inMemoryEventStore()
      const repo = eventSourcedRepository(Subscription, store)

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
