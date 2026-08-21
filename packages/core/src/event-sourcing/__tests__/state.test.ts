import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, event } from "../../messaging/messages.js"
import { tag } from "../../messaging/tag.js"
import { state } from "../state.js"

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

type CourseState = {
  created: boolean
  name: string
  capacity: number
}

describe("state()", () => {
  it("creates an entity definition with evolvers", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [
        () => ({ created: false, name: "", capacity: 0 }) as CourseState,
        [CourseCreated, (state, { payload: event }) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })],
        [CourseCapacityChanged, (state, { payload: event }) => ({
          ...state,
          capacity: event.capacity,
        })],
      ],
    })

    expect(Course.kind).toBe("state-module")
    // Nothing names a state. Where its snapshots are filed is something the
    // author writes down in `snapshot: { key, when }`, and its process identity
    // is what a diagnostic prints.
    expect("name" in Course).toBe(false)
    expect(Course.snapshot).toBeUndefined()
    expect(Course.identity).toMatch(/^state#\d+$/)
    expect(Course.evolvers).toHaveLength(2)
  })

  it("the initial state — `evolve[0]` — is the zeroth state", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [() => ({ created: false, name: "", capacity: 0 }) as CourseState],
    })

    const initial = Course.initial({ courseId: "cs-101" })

    expect(initial).toEqual({ created: false, name: "", capacity: 0 })
  })

  it("the initial state may READ the id it is being folded for", () => {
    // Nothing has happened yet, so the identity is the only thing the zeroth
    // state can honestly know — and a fold that carries its own key does not
    // have to wait for an event to tell it what it already is.
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [(id) => ({ courseId: id.courseId, capacity: 0 })],
    })

    expect(Course.initial({ courseId: "cs-101" })).toEqual({ courseId: "cs-101", capacity: 0 })
  })

  it("a state with no evolvers gets tags only — an empty fold means 'all types', not 'none'", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [() => ({ created: false, name: "", capacity: 0 }) as CourseState],
    })

    expect(Course.query({ courseId: "cs-101" })).toEqual({ tags: { courseId: "cs-101" } })
  })

  it("derives the event-TYPE filter from the evolve tuples", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [
        () => ({ created: false, name: "", capacity: 0 }) as CourseState,
        [CourseCreated, (state) => state],
        [CourseCapacityChanged, (state) => state],
      ],
    })

    // The fold names the types; nobody restated them.
    expect(Course.query({ courseId: "cs-101" })).toEqual({
      tags: { courseId: "cs-101" },
      types: ["university.CourseCreated", "university.CourseCapacityChanged"],
    })
  })

  it("an ARRAY of tag records is an OVERRIDE — every record against every folded type", () => {
    // The array form deliberately skips the per-type intersection: `studentId`
    // is paired with CourseCreated even though CourseCreated never carries it.
    // That is the escape hatch's whole point, and why it is not the default.
    const Subscription = state({
      id: { courseId: z.string(), studentId: z.string() },
      tags: (id) => [{ courseId: id.courseId }, { studentId: id.studentId }],
      evolve: [() => ({ created: false, name: "", capacity: 0 }) as CourseState, [CourseCreated, (state) => state]],
    })

    // Each item carries the SAME derived type filter — the fold is one fold.
    expect(Subscription.query({ courseId: "cs-101", studentId: "stu-1" })).toEqual([
      { tags: { courseId: "cs-101" }, types: ["university.CourseCreated"] },
      { tags: { studentId: "stu-1" }, types: ["university.CourseCreated"] },
    ])
  })

  it("`name` is optional, and every definition still gets a distinct identity", () => {
    const def = {
      id: { courseId: z.string() },
      tags: (id: { courseId: string }) => ({ courseId: id.courseId }),
      evolve: [() =>
          ({ created: false, name: "", capacity: 0 }) as CourseState],
    } as const

    const a = state({ ...def })
    const b = state({ ...def })

    expect(a.name).toBeUndefined()
    expect(a.identity).not.toBe(b.identity)
    // The identity rides along when a host spreads the state to attach a log.
    expect({ ...a, eventStore: {} }.identity).toBe(a.identity)
  })

  it("evolvers transform state correctly", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [
        () => ({ created: false, name: "", capacity: 0 }) as CourseState,
        [CourseCreated, (state, { payload: event }) => ({
          ...state,
          created: true,
          name: event.name,
          capacity: event.capacity,
        })],
        [CourseCapacityChanged, (state, { payload: event }) => ({
          ...state,
          capacity: event.capacity,
        })],
      ],
    })

    // Manually apply evolvers to verify they work
    let current = Course.initial({ courseId: "cs-101" })

    const [createDescriptor, createEvolve] = Course.evolvers[0]!
    expect(createDescriptor).toBe(CourseCreated)
    current = createEvolve(
      current,
      {
        kind: "event",
        identifier: "evt-1",
        name: CourseCreated.name,
        version: CourseCreated.version,
        payload: { courseId: "cs-101", name: "Intro to CS", capacity: 30 },
        metadata: {},
        timestamp: Date.now(),
        tags: [],
      },
    ) as CourseState
    expect(current).toEqual({ created: true, name: "Intro to CS", capacity: 30 })

    const [capacityDescriptor, capacityEvolve] = Course.evolvers[1]!
    expect(capacityDescriptor).toBe(CourseCapacityChanged)
    current = capacityEvolve(
      current,
      {
        kind: "event",
        identifier: "evt-2",
        name: CourseCapacityChanged.name,
        version: CourseCapacityChanged.version,
        payload: { courseId: "cs-101", capacity: 50 },
        metadata: {},
        timestamp: Date.now(),
        tags: [],
      },
    ) as CourseState
    expect(current).toEqual({ created: true, name: "Intro to CS", capacity: 50 })
  })

  it("per-element inference: each evolver's message.payload narrows to ITS OWN event, not a union", () => {
    // Type-level assertion only — nothing here runs, it just must compile.
    // If inference degraded to a union or `any`, this would either fail to
    // compile (wrong payload access) or silently accept the typo below.
    state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [
        () => ({ created: false, name: "", capacity: 0 }) as CourseState,
        [CourseCreated, (state, { payload }) => ({
          ...state,
          created: true,
          name: payload.name,
          capacity: payload.capacity,
        })],
        [CourseCapacityChanged, (state, { payload }) => ({
          ...state,
          // @ts-expect-error — CourseCapacityChanged's payload has no `name`
          // field; if inference collapsed to a union of all evolve entries'
          // payloads (or to `any`), this typo would compile silently.
          name: payload.name,
          capacity: payload.capacity,
        })],
      ],
    })

    expect(true).toBe(true)
  })
})

describe("granular query derivation", () => {
  // A course event carries only `courseId`; a faculty enrolment only
  // `studentId`; a subscription carries BOTH. That spread is what makes this
  // the canonical multi-stream DCB shape.
  const StudentEnrolledInFaculty = event({
    name: qn("university", "StudentEnrolledInFaculty"),
    payload: z.object({ studentId: z.string() }),
    tags: { studentId: (p) => p.studentId },
  })

  const StudentSubscribedToCourse = event({
    name: qn("university", "StudentSubscribedToCourse"),
    payload: z.object({ courseId: z.string(), studentId: z.string() }),
    tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
  })

  const Untagged = event({
    name: qn("university", "SemesterRolled"),
    payload: z.object({ semester: z.string() }),
  })

  it("scopes each event type to the tags IT declares, not the state's whole record", () => {
    const Subscription = state({
      id: { courseId: z.string(), studentId: z.string() },
      tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
      evolve: [
        () => ({}),
        [CourseCreated, (s) => s],
        [StudentEnrolledInFaculty, (s) => s],
      ],
    })

    // ONE plain record produced the OR across two streams.
    expect(Subscription.query({ courseId: "cs-101", studentId: "stu-1" })).toEqual([
      { tags: { courseId: "cs-101" }, types: ["university.CourseCreated"] },
      { tags: { studentId: "stu-1" }, types: ["university.StudentEnrolledInFaculty"] },
    ])
  })

  it("ANDs multiple shared keys within a type when nothing forces a wider read", () => {
    const Enrolment = state({
      id: { courseId: z.string(), studentId: z.string() },
      tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
      evolve: [() => ({}), [StudentSubscribedToCourse, (s) => s]],
    })

    // Both shared keys are ANDed — this state is pinned to the exact pair.
    expect(Enrolment.query({ courseId: "cs-101", studentId: "stu-1" })).toEqual({
      tags: { courseId: "cs-101", studentId: "stu-1" },
      types: ["university.StudentSubscribedToCourse"],
    })
  })

  it("a type that declares a narrower item's tags in full also rides that item", () => {
    // THE CAPACITY CASE. StudentSubscribedToCourse carries both keys, but a
    // sibling type (CourseCreated) pins a `courseId`-only item, so the
    // subscription event joins it too — otherwise this state could never see
    // OTHER students' subscriptions, and a capacity check would be unsourceable
    // and its append condition too narrow to catch the conflict.
    const Subscription = state({
      id: { courseId: z.string(), studentId: z.string() },
      tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
      evolve: [
        () => ({}),
        [CourseCreated, (s) => s],
        [StudentEnrolledInFaculty, (s) => s],
        [StudentSubscribedToCourse, (s) => s],
      ],
    })

    expect(Subscription.query({ courseId: "cs-101", studentId: "stu-1" })).toEqual([
      {
        tags: { courseId: "cs-101" },
        types: ["university.CourseCreated", "university.StudentSubscribedToCourse"],
      },
      {
        tags: { studentId: "stu-1" },
        types: ["university.StudentEnrolledInFaculty", "university.StudentSubscribedToCourse"],
      },
    ])
  })

  it("event types sharing an intersection collapse into ONE item", () => {
    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [
        () => ({}),
        [CourseCreated, (s) => s],
        [CourseCapacityChanged, (s) => s],
      ],
    })

    // Not two identical-tag items — ONE item, two types.
    expect(Course.query({ courseId: "cs-101" })).toEqual({
      tags: { courseId: "cs-101" },
      types: ["university.CourseCreated", "university.CourseCapacityChanged"],
    })
  })

  it("boot error names the state (identity + folded events) AND the event type when they share no tag key", () => {
    expect(() =>
      state({
        id: { courseId: z.string() },
        tags: (id) => ({ courseId: id.courseId }),
        // A faculty enrolment carries only `studentId` — this fold can never fire.
        evolve: [() => ({}), [StudentEnrolledInFaculty, (s) => s]],
      }),
    ).toThrow(/state#\d+ \(folds .*university\.StudentEnrolledInFaculty.*share no tag key/s)
  })

  it("an event with no tags at all shares no key, and is caught the same way", () => {
    expect(() =>
      state({
        id: { courseId: z.string() },
        tags: (id) => ({ courseId: id.courseId }),
        evolve: [() => ({}), [Untagged, (s) => s]],
      }),
    ).toThrow(/university\.SemesterRolled.*carries no tags/s)
  })

  it("refuses to guess when a folded event never declared its tag keys", () => {
    const Opaque = event({
      name: qn("university", "Opaque"),
      payload: z.object({ courseId: z.string() }),
      // Function form, no `tagKeys` — the keys are genuinely unknown.
      tags: (p) => [tag("courseId", p.courseId)],
    })

    expect(Opaque.tagKeys).toBeUndefined()
    expect(() =>
      state({
        id: { courseId: z.string() },
        tags: (id) => ({ courseId: id.courseId }),
        evolve: [() => ({}), [Opaque, (s) => s]],
      }),
    ).toThrow(/does not declare its tag keys/)
  })

  it("an explicit `tagKeys` makes the function form usable again", () => {
    const Declared = event({
      name: qn("university", "Declared"),
      payload: z.object({ courseId: z.string() }),
      tags: (p) => [tag("courseId", p.courseId)],
      tagKeys: ["courseId"],
    })

    const Course = state({
      id: { courseId: z.string() },
      tags: (id) => ({ courseId: id.courseId }),
      evolve: [() => ({}), [Declared, (s) => s]],
    })

    expect(Course.query({ courseId: "cs-101" })).toEqual({
      tags: { courseId: "cs-101" },
      types: ["university.Declared"],
    })
  })

  it("the derived query is never match-all — every item carries a tag", () => {
    expect(() =>
      state({
        id: {},
        tags: () => ({}),
        evolve: [() => ({})],
      }),
    ).toThrow(/match EVERY event/)
  })
})
