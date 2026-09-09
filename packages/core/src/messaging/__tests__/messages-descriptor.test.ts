import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { emptyMetadata, is, qn, tagKeysOf, tagsOf, type Message } from "../messages.js"
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
    it("creates an event descriptor with a tags record", () => {
      const CourseCreated = event({
        name: qn("university.courses", "CourseCreated"),
        payload: z.object({ courseId: z.string(), name: z.string() }),
        tags: { courseId: (p) => p.courseId },
      })

      expect(CourseCreated.kind).toBe("event")
      expect(Object.keys(CourseCreated.tags!)).toEqual(["courseId"])

      const tags = tagsOf(CourseCreated, { courseId: "cs-101", name: "Intro" }, emptyMetadata())
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

  describe("tags — a record of lambdas, decided once at birth", () => {
    const meta = { ...emptyMetadata(), tenantId: "t-9" }

    it("keeps the record as written and derives the keys from it", () => {
      const Subscribed = event({
        name: qn("university", "StudentSubscribedToCourse"),
        payload: z.object({ courseId: z.string(), studentId: z.string() }),
        tags: { courseId: (p) => p.courseId, studentId: (p) => p.studentId },
      })

      expect(tagKeysOf(Subscribed)).toEqual(["courseId", "studentId"])
      expect(tagsOf(Subscribed, { courseId: "cs-101", studentId: "stu-1" }, meta)).toEqual([
        { key: "courseId", value: "cs-101" },
        { key: "studentId", value: "stu-1" },
      ])
    })

    it("a lambda answers one value, several, or none", () => {
      const Relabelled = event({
        name: qn("catalog", "ItemsRelabelled"),
        payload: z.object({ items: z.array(z.string()), region: z.string().optional() }),
        tags: {
          itemId: (p) => p.items,
          region: (p) => p.region,
        },
      })

      expect(tagKeysOf(Relabelled)).toEqual(["itemId", "region"])
      expect(tagsOf(Relabelled, { items: ["a", "b"] }, meta)).toEqual([
        { key: "itemId", value: "a" },
        { key: "itemId", value: "b" },
      ])
      expect(tagsOf(Relabelled, { items: [], region: "eu" }, meta)).toEqual([{ key: "region", value: "eu" }])
    })

    it("sees the metadata the event is born with", () => {
      const Charged = event({
        name: qn("billing", "Charged"),
        payload: z.object({ accountId: z.string() }),
        tags: { accountId: (p) => p.accountId, tenantId: (_p, m) => m.tenantId as string },
      })

      expect(tagsOf(Charged, { accountId: "a-1" }, meta)).toEqual([
        { key: "accountId", value: "a-1" },
        { key: "tenantId", value: "t-9" },
      ])
    })

    it("an event's tags are a set — an identical pair appears once", () => {
      const Twice = event({
        name: qn("probe", "Twice"),
        payload: z.object({ ids: z.array(z.string()) }),
        tags: { id: (p) => p.ids },
      })

      expect(tagsOf(Twice, { ids: ["x", "y", "x"] }, meta)).toEqual([
        { key: "id", value: "x" },
        { key: "id", value: "y" },
      ])
    })

    it("an event with no tags has the empty key set and no tags", () => {
      const Untagged = event({
        name: qn("university", "SemesterRolled"),
        payload: z.object({ semester: z.string() }),
      })

      expect(tagKeysOf(Untagged)).toEqual([])
      expect(tagsOf(Untagged, { semester: "S1" }, meta)).toEqual([])
      expect(Untagged.tags).toBeUndefined()
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
