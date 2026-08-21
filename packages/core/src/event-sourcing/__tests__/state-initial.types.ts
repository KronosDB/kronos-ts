/**
 * The TYPE test for THE SEED THAT TAKES ITS ID.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * (where the package build and the published `files` list already drop it) and
 * is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red — the only way a "this must not compile" claim
 * can be honest.
 *
 * What it pins: `evolve[0]` is handed the state's inferred id, and NOTHING the
 * fold already guaranteed was traded for it. An initial state may read the id or decline
 * it; either way `S` comes off it, and every case still narrows to ITS
 * OWN descriptor's payload rather than to a union of all of them.
 *
 * WHY THIS IS WORTH PINNING. Making the initial state take the id makes an
 * id-reading literal a CONTEXT-SENSITIVE expression, and a context-sensitive element
 * zero is exactly what a naive spelling of `EvolveTuple` cannot survive: the
 * inference variable gets fixed to its constraint and every case's descriptor
 * goes with it, silently, leaving `payload` as `any`. Nothing about that shows
 * up in a runtime test — the folds still run. It shows up here.
 */
import { z } from "zod"
import { qn, event } from "../../messaging/messages.js"
import { state } from "../state.js"

const CourseCreated = event({
  name: qn("seedprobe", "CourseCreated"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: { courseId: (p) => p.courseId },
})

const StudentEnrolledInFaculty = event({
  name: qn("seedprobe", "StudentEnrolledInFaculty"),
  payload: z.object({ studentId: z.string() }),
  tags: { studentId: (p) => p.studentId },
})

type SubscriptionState = {
  courseId: string
  studentId: string
  capacity: number
  enrolled: boolean
}

// ---------------------------------------------------------------------------
// (a) THE SEED READS ITS ID — and the id is the INFERRED record, not `any`.
// ---------------------------------------------------------------------------

const Subscription = state({
  id: { courseId: z.string(), studentId: z.string() },
  tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
  evolve: [
    (id) => ({
      courseId: id.courseId,
      studentId: id.studentId,
      capacity: 0,
      enrolled: false,
    }),
    [CourseCreated, (s, { payload }) => ({ ...s, capacity: payload.capacity })],
    [StudentEnrolledInFaculty, (s) => ({ ...s, enrolled: true })],
  ],
})

/** `S` is read off the initial state, field by field — not `unknown`, not `any`. */
export const initialised: SubscriptionState = Subscription.initial({
  courseId: "cs-101",
  studentId: "stu-1",
})

state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    // @ts-expect-error the id is the inferred record — there is no `nope` on it
    (id) => ({ courseId: id.nope, capacity: 0 }),
    [CourseCreated, (s) => s],
  ],
})

// @ts-expect-error the initial state takes the id, and the id record is exact
Subscription.initial({ courseId: "cs-101" })

// ---------------------------------------------------------------------------
// (b) PER-CASE NARROWING SURVIVES IT — with an id-reading initial state AND two
//     heterogeneous cases, which is the shape that degrades first.
// ---------------------------------------------------------------------------

state({
  id: { courseId: z.string(), studentId: z.string() },
  tags: (id) => ({ courseId: id.courseId, studentId: id.studentId }),
  evolve: [
    (id): SubscriptionState => ({ ...id, capacity: 0, enrolled: false }),
    [CourseCreated, (s, { payload }) => ({ ...s, capacity: payload.capacity })],
    [StudentEnrolledInFaculty, (s, { payload }) => ({
      ...s,
      // @ts-expect-error StudentEnrolledInFaculty's payload has no `capacity`;
      // if inference had collapsed to the constraint this would be `any`.
      capacity: payload.capacity,
      enrolled: payload.studentId.length > 0,
    })],
  ],
})

// ---------------------------------------------------------------------------
// (c) A SEED MAY DECLINE THE ID — `() => S` is assignable to `(id) => S` by
//     TypeScript's arity rule, and that route infers `S` just as well.
// ---------------------------------------------------------------------------

const Course = state({
  id: { courseId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [
    () => ({ created: false, capacity: 0 }),
    [CourseCreated, (s, { payload }) => ({ ...s, created: true, capacity: payload.capacity })],
  ],
})

export const declined: { created: boolean; capacity: number } = Course.initial({ courseId: "cs-101" })
