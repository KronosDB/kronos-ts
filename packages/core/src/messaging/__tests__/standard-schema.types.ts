/**
 * The TYPE test for SCHEMA FREEDOM.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not
 * subject to `exclude`, so it lives beside its runtime siblings in `__tests__`
 * (where the package build and the published `files` list already drop it) and
 * is still judged by `bunx tsc --noEmit`. A `@ts-expect-error` that stops
 * erroring turns that gate red — the only way a "this must not compile" claim
 * can be honest.
 *
 * What it pins, in one sentence: core stopped depending on zod and NOTHING a
 * zod consumer writes changed. Descriptor payload, result and id constraints
 * are `StandardSchemaV1` now, so a zod schema, a valibot schema, an arktype
 * schema and a twelve-line hand-written object are all equally welcome — and
 * the inference a handler signature gets out of a zod schema is EXACTLY what it
 * was, down to the exact object type and down to the wrong payload still being
 * a compile error.
 */
import { z } from "zod"
import { qn, command, event, queryDescriptor, type CommandDescriptor, type EventDescriptor } from "../messages.js"
import type { InferOutput, StandardSchemaV1 } from "../standard-schema.js"
import { state } from "../../event-sourcing/state.js"
import { commandHandler } from "../../command-handling/handler.js"
import { eventHandler } from "../../event-processing/handler.js"
import { queryHandler } from "../../query-handling/handler.js"

// ---------------------------------------------------------------------------
// (a) ZOD, UNCHANGED — the descriptor infers the EXACT object type.
// ---------------------------------------------------------------------------

const CreateCourse = command({
  name: qn("probe", "CreateCourse"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  result: z.object({ courseId: z.string() }),
})

/** The payload is the object type, field by field — not `unknown`, not `any`. */
export const exactPayload: (p: InferOutput<typeof CreateCourse.payload>) => {
  courseId: string
  capacity: number
} = (p) => p

/** …and the result schema types what a handler must return. */
export const createCourse = commandHandler(CreateCourse, async ({ payload }) => {
  const id: string = payload.courseId
  const cap: number = payload.capacity
  return { courseId: `${id}:${cap}` }
})

// @ts-expect-error the handler must return the RESULT type, not a string
commandHandler(CreateCourse, async () => "nope")

// ---------------------------------------------------------------------------
// (b) EVENTS — the tag extractor sees the exact payload, and folds do too.
// ---------------------------------------------------------------------------

const CourseCreated = event({
  name: qn("probe", "CourseCreated"),
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: {
    courseId: (p) => p.courseId,
    // @ts-expect-error `capacity` is a number; a tag value is a string
    capacity: (p) => p.capacity,
  },
})

export const onCourseCreated = eventHandler(CourseCreated, ({ payload, timestamp }) => {
  const cap: number = payload.capacity
  // A DELIVERED event has an instant — `timestamp` is `number`, not `number | undefined`,
  // even though it is optional on the base `Message`.
  const at: number = timestamp
  void [cap, at]
})

// ---------------------------------------------------------------------------
// (c) QUERIES and STATE IDS — same story, same inference.
// ---------------------------------------------------------------------------

const GetCourse = queryDescriptor({
  name: qn("probe", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

export const getCourse = queryHandler(GetCourse, async ({ payload }) => payload.courseId.length)

const Course = state({
  id: { courseId: z.string(), tenantId: z.string() },
  tags: (id) => ({ courseId: id.courseId }),
  evolve: [() => ({ capacity: 0 }), [CourseCreated, (s, { payload }) => ({ ...s, capacity: payload.capacity })]],
})

/** `state({ id })` is a record of schemas, and the id it hands back is inferred. */
export const courseTags: ReturnType<typeof Course.tags> = Course.tags({
  courseId: "cs-101",
  tenantId: "t-1",
})

// @ts-expect-error the id record is exact — `tenantId` is not optional
Course.tags({ courseId: "cs-101" })

// ---------------------------------------------------------------------------
// (d) A HAND-WRITTEN Standard Schema is accepted — no library anywhere.
// ---------------------------------------------------------------------------

type Reservation = { readonly reservationId: string; readonly seats: number }

const reservationSchema: StandardSchemaV1<Reservation, Reservation> = {
  "~standard": {
    version: 1,
    vendor: "probe",
    validate: (value) =>
      typeof (value as Reservation).reservationId === "string"
        ? { value: value as Reservation }
        : { issues: [{ message: "reservationId must be a string" }] },
  },
}

const Reserve = command({
  name: qn("probe", "Reserve"),
  payload: reservationSchema,
})

export const reserve = commandHandler(Reserve, async ({ payload }) => {
  const seats: number = payload.seats
  void seats
})

commandHandler(Reserve, async ({ payload }) => {
  // @ts-expect-error the hand-written schema's payload is exact too — `seats` is a number
  const wrong: string = payload.seats
  void wrong
})

// ---------------------------------------------------------------------------
// (e) THE DERIVATION MEASUREMENT (mission item 7).
//
// `CorrelatingUnitOfWork = ReturnType<typeof correlating>` works because
// `correlating` is a PLAIN function with an INFERRED return. The descriptor
// constructors are neither: they are OVERLOADED (result-schema present or
// absent), so an instantiation expression over the overload set resolves to the
// first overload and can never name the two-parameter form; and their own
// return type is the alias, so deriving the alias from them would be circular.
//
// What IS derived is the place where a shape would otherwise be RESTATED: the
// namespace-scoped constructors state their returns as the plain constructors'
// returns, so `withNamespace` has no second descriptor shape to keep in step.
// These two assignments are what "no degradation" means — the derived spelling
// and the hand-written one are the same type, in both directions.
// ---------------------------------------------------------------------------

type ZodCourseId = z.ZodObject<{ courseId: z.ZodString }>
declare const zodCourseId: ZodCourseId

const derived: ReturnType<typeof command<ZodCourseId>> = command({
  name: qn("probe", "Derived"),
  payload: zodCourseId,
})
export const handWritten: CommandDescriptor<ZodCourseId, undefined> = derived
export const backAgain: ReturnType<typeof command<ZodCourseId>> = handWritten

/** And the event descriptor alias is likewise exactly what `event()` returns. */
export const eventDerived: EventDescriptor<z.ZodObject<{ courseId: z.ZodString; capacity: z.ZodNumber }>> =
  CourseCreated
