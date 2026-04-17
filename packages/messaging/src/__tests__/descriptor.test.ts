import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import { command, event, query } from "../descriptor.js"

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
        tags: (p) => [tag("courseId", p.courseId)],
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
})
