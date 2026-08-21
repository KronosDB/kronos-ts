import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { is, qn, type Message } from "../messages.js"
import { tag } from "../tag.js"
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

  describe("is()", () => {
    const CreateCourse = command({
      name: qn("university", "CreateCourse"),
      payload: z.object({ courseId: z.string(), name: z.string() }),
    })
    const GetCourse = query({
      name: qn("university", "GetCourse"),
      payload: z.object({ courseId: z.string() }),
    })
    const CourseCreated = event({
      name: qn("university", "CourseCreated"),
      version: "2.0",
      payload: z.object({ courseId: z.string(), capacity: z.number() }),
      tags: { courseId: (p) => p.courseId },
    })

    const createCourse: Message = {
      kind: "command",
      identifier: "c-1",
      name: CreateCourse.name,
      payload: { courseId: "cs-101", name: "Intro" },
      metadata: {},
    }
    const getCourse: Message = {
      kind: "query",
      identifier: "q-1",
      name: GetCourse.name,
      payload: { courseId: "cs-101" },
      metadata: {},
    }
    const courseCreated: Message = {
      kind: "event",
      identifier: "e-1",
      name: CourseCreated.name,
      version: "2.0",
      payload: { courseId: "cs-101", capacity: 30 },
      metadata: {},
      timestamp: 1_700_000_000_000,
      tags: [{ key: "courseId", value: "cs-101" }],
    } as Message

    it("narrows a COMMAND to its descriptor's payload", () => {
      if (is(createCourse, CreateCourse)) {
        const name: string = createCourse.payload.name
        expect(name).toBe("Intro")
        expect(createCourse.kind).toBe("command")
      } else {
        throw new Error("expected the command to match")
      }
    })

    it("narrows a QUERY to its descriptor's payload", () => {
      if (is(getCourse, GetCourse)) {
        const courseId: string = getCourse.payload.courseId
        expect(courseId).toBe("cs-101")
      } else {
        throw new Error("expected the query to match")
      }
    })

    it("narrows an EVENT to its descriptor's payload", () => {
      if (is(courseCreated, CourseCreated)) {
        const capacity: number = courseCreated.payload.capacity
        expect(capacity).toBe(30)
        expect(courseCreated.tags).toEqual([{ key: "courseId", value: "cs-101" }])
      } else {
        throw new Error("expected the event to match")
      }
    })

    it("does not match across KINDS, even at the same qualified name", () => {
      const sameName = command({
        name: qn("university", "CourseCreated"),
        payload: z.object({ courseId: z.string() }),
      })
      expect(is(courseCreated, sameName)).toBe(false)
    })

    it("does not match a different NAME", () => {
      expect(is(createCourse, command({
        name: qn("billing", "CreateCourse"),
        payload: z.object({ courseId: z.string() }),
      }))).toBe(false)
    })

    it("does not match an event at a DIFFERENT VERSION", () => {
      const v1 = event({
        name: qn("university", "CourseCreated"),
        version: "1.0",
        payload: z.object({ courseId: z.string() }),
        tags: { courseId: (p) => p.courseId },
      })
      expect(is(courseCreated, v1)).toBe(false)
      expect(is(courseCreated, CourseCreated)).toBe(true)
    })

    it("ignores the descriptor's version for a COMMAND — the message carries none", () => {
      const versioned = command({
        name: qn("university", "CreateCourse"),
        version: "9.9",
        payload: z.object({ courseId: z.string(), name: z.string() }),
      })
      expect(is(createCourse, versioned)).toBe(true)
    })
  })
})
