import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { tag } from "../../primitives/tag.js"
import { command, event, query } from "../../index.js"

describe("Message Descriptors", () => {
  describe("command()", () => {
    it("creates a command descriptor with name and payload", () => {
      const CreateCourse = command({
        name: qn("university.courses", "CreateCourse"),
        payload: z.object({ courseId: z.string(), name: z.string() }),
      })

      expect(CreateCourse.kind).toBe("command")
      expect(CreateCourse.name.namespace).toBe("university.courses")
      expect(CreateCourse.name.name).toBe("CreateCourse")
      expect(CreateCourse.payload).toBeDefined()
    })
  })

  describe("event()", () => {
    it("creates an event descriptor with tags function", () => {
      const CourseCreated = event({
        name: qn("university.courses", "CourseCreated"),
        payload: z.object({ courseId: z.string(), name: z.string() }),
        tags: { courseId: (p) => p.courseId },
      })

      expect(CourseCreated.kind).toBe("event")
      expect(CourseCreated.tags).toBeDefined()

      const tags = CourseCreated.tags!({ courseId: "cs-101", name: "Intro" })
      expect(tags).toEqual([{ key: "courseId", value: "cs-101" }])
    })

    it("creates an event descriptor without tags", () => {
      const SystemEvent = event({
        name: qn("system", "Heartbeat"),
        payload: z.object({ timestamp: z.number() }),
      })

      expect(SystemEvent.tags).toBeUndefined()
    })
  })

  describe("query()", () => {
    it("creates a query descriptor with name and payload", () => {
      const GetCourse = query({
        name: qn("university.courses", "GetCourse"),
        payload: z.object({ courseId: z.string() }),
      })

      expect(GetCourse.kind).toBe("query")
      expect(GetCourse.payload).toBeDefined()
    })
  })

  describe("Zod schema validation via descriptors", () => {
    it("validates command payloads", () => {
      const CreateCourse = command({
        name: qn("university.courses", "CreateCourse"),
        payload: z.object({
          courseId: z.string().uuid(),
          name: z.string().min(1),
        }),
      })

      const valid = CreateCourse.payload.safeParse({
        courseId: "550e8400-e29b-41d4-a716-446655440000",
        name: "Intro to CS",
      })
      expect(valid.success).toBe(true)

      const invalid = CreateCourse.payload.safeParse({
        courseId: "not-a-uuid",
        name: "",
      })
      expect(invalid.success).toBe(false)
    })
  })

  describe("tagKeys", () => {
    it("derives them from the record-of-extractors form", () => {
      const Subscribed = event({
        name: qn("university", "StudentSubscribedToCourse"),
        payload: z.object({ courseId: z.string(), studentId: z.string() }),
        tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
      })

      expect(Subscribed.tagKeys).toEqual(["courseId", "studentId"])
      // and the record still compiles to the same Tag[] the stores index on
      expect(Subscribed.tags?.({ courseId: "cs-101", studentId: "stu-1" })).toEqual([
        { key: "courseId", value: "cs-101" },
        { key: "studentId", value: "stu-1" },
      ])
    })

    it("is UNDEFINED — not guessed — for the opaque function form", () => {
      const Opaque = event({
        name: qn("university", "Opaque"),
        payload: z.object({ courseId: z.string() }),
        tags: (p) => [tag("courseId", p.courseId)],
      })

      expect(Opaque.tagKeys).toBeUndefined()
      expect(Opaque.tags?.({ courseId: "cs-101" })).toEqual([{ key: "courseId", value: "cs-101" }])
    })

    it("takes an explicit declaration alongside the function form", () => {
      const Declared = event({
        name: qn("university", "Declared"),
        payload: z.object({ items: z.array(z.string()) }),
        tags: (p) => p.items.map((id) => tag("itemId", id)),
        tagKeys: ["itemId"],
      })

      expect(Declared.tagKeys).toEqual(["itemId"])
      expect(Declared.tags?.({ items: ["a", "b"] })).toEqual([
        { key: "itemId", value: "a" },
        { key: "itemId", value: "b" },
      ])
    })

    it("an event with NO tags declares the empty key set, not an unknown one", () => {
      const Untagged = event({
        name: qn("university", "SemesterRolled"),
        payload: z.object({ semester: z.string() }),
      })

      expect(Untagged.tagKeys).toEqual([])
      expect(Untagged.tags).toBeUndefined()
    })

    it("rejects `tagKeys` given alongside a tags RECORD — they cannot disagree", () => {
      expect(() =>
        event({
          name: qn("university", "Conflicted"),
          payload: z.object({ courseId: z.string() }),
          tags: { courseId: (p) => p.courseId },
          tagKeys: ["somethingElse"],
        }),
      ).toThrow(/cannot disagree|remove `tagKeys`/)
    })
  })
})
