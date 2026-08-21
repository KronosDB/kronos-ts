/**
 * `validate(descriptor, payload)` — the primitive.
 *
 * These are the semantics the validating SERIALIZER used to carry, minus the
 * registry that made it possible. There is nothing to register here and nothing
 * to look up: the descriptor is the argument, and it brought its own schema.
 * The two registry-mechanics claims that used to live beside them — "passes
 * through when no schema is registered" and "falls back to a no-revision
 * schema" — died with the registry, because a descriptor has exactly one
 * payload schema and there is no version of this call that has no schema to
 * check against.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, command, event, queryDescriptor } from "../../messaging/messages.js"
import { validate } from "../validate.js"

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

describe("validate — the parsed value, or a throw naming the message type", () => {
  it("returns the PARSED value, synchronously, for a synchronous schema", () => {
    const parsed = validate(CreateCourse, { courseId: "cs-101", name: "Intro" })

    expect(parsed).toEqual({ courseId: "cs-101", name: "Intro" })
    // Not a promise — a sync schema must not tax a sync caller with an await.
    expect(parsed).not.toBeInstanceOf(Promise)
  })

  it("returns what the SCHEMA produced, not what the caller passed", () => {
    // Standard validation is a parse. Defaults, coercions and transforms are
    // part of what a schema says, so the value that comes back is the value the
    // rest of the system should see — dropping it keeps the check and throws
    // away half of it.
    const Enroll = command({
      name: qn("university", "Enroll"),
      payload: z.object({
        courseId: z.string(),
        capacity: z.number().default(30),
        seats: z.coerce.number(),
      }),
    })

    expect(validate(Enroll, { courseId: "cs-101", seats: "12" })).toEqual({
      courseId: "cs-101",
      capacity: 30,
      seats: 12,
    })
  })

  it("throws naming the message type and joining everything the schema found", () => {
    expect(() => validate(CreateCourse, { courseId: "cs-101" })).toThrow(
      /university\.CreateCourse.*failed validation/s,
    )
  })

  it("takes an EVENT or a QUERY descriptor just the same — every face is (descriptor, …)", () => {
    const CourseCreated = event({
      name: qn("university", "CourseCreated"),
      payload: z.object({ courseId: z.string() }),
      tags: { courseId: (p) => p.courseId },
    })
    const GetCourse = queryDescriptor({
      name: qn("university", "GetCourse"),
      payload: z.object({ courseId: z.string() }),
    })

    expect(validate(CourseCreated, { courseId: "cs-101" })).toEqual({ courseId: "cs-101" })
    expect(validate(GetCourse, { courseId: "cs-101" })).toEqual({ courseId: "cs-101" })
    expect(() => validate(GetCourse, {})).toThrow(/university\.GetCourse/)
  })

  it("validates against a HAND-WRITTEN Standard Schema — no schema library involved", () => {
    const Reserve = command({
      name: qn("hotel", "Reserve"),
      payload: {
        "~standard": {
          version: 1,
          vendor: "hand",
          validate: (value: unknown) =>
            typeof (value as { courseId?: unknown }).courseId === "string"
              ? { value }
              : { issues: [{ message: "courseId must be a string" }] },
        },
      } as const,
    })

    expect(validate(Reserve, { courseId: "cs-101" })).toEqual({ courseId: "cs-101" })
    expect(() => validate(Reserve, { courseId: 7 })).toThrow(/courseId must be a string/)
  })

  it("hands back the PROMISE when the schema validates asynchronously", async () => {
    // The union is the honest return type: whoever is in an async position
    // awaits, and the parse — success or throw — is carried on the promise.
    const Slow = command({
      name: qn("university", "Slow"),
      payload: {
        "~standard": {
          version: 1,
          vendor: "slow",
          validate: async (value: unknown) =>
            (value as { ok?: boolean }).ok
              ? { value: { ok: true, parsed: true } }
              : { issues: [{ message: "ok must be true" }] },
        },
      } as const,
    })

    const pending = validate(Slow, { ok: true })
    expect(pending).toBeInstanceOf(Promise)
    expect(await pending).toEqual({ ok: true, parsed: true })

    await expect(Promise.resolve(validate(Slow, { ok: false }))).rejects.toThrow(
      /university\.Slow.*ok must be true/s,
    )
  })
})
