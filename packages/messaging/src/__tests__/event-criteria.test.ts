import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import { EventCriteria } from "../event-criteria.js"
import { event } from "../descriptor.js"

describe("EventCriteria", () => {
  describe("havingTags", () => {
    it("creates a tag criteria", () => {
      const criteria = EventCriteria.havingTags(tag("courseId", "cs-101"))

      expect(criteria.kind).toBe("tags")
      if (criteria.kind === "tags") {
        expect(criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
      }
    })

    it("supports multiple tags", () => {
      const criteria = EventCriteria.havingTags(
        tag("courseId", "cs-101"),
        tag("studentId", "stu-001"),
      )

      if (criteria.kind === "tags") {
        expect(criteria.tags).toHaveLength(2)
      }
    })
  })

  describe("havingAnyTag", () => {
    it("creates an any-tag criteria", () => {
      const criteria = EventCriteria.havingAnyTag()

      expect(criteria.kind).toBe("any-tag")
    })
  })

  describe("ofTypes", () => {
    it("restricts tag criteria by event descriptor types", () => {
      const CourseCreated = event({
        name: qn("university", "CourseCreated"),
        payload: z.object({ courseId: z.string() }),
      })

      const criteria = EventCriteria
        .havingTags(tag("courseId", "cs-101"))
        .ofTypes(CourseCreated)

      expect(criteria.kind).toBe("type-restricted")
      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual(["university.CourseCreated"])
      }
    })

    it("accepts string type names", () => {
      const criteria = EventCriteria
        .havingTags(tag("courseId", "cs-101"))
        .ofTypes("university.CourseCreated", "university.CourseDeleted")

      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual([
          "university.CourseCreated",
          "university.CourseDeleted",
        ])
      }
    })

    it("accepts qualified names", () => {
      const criteria = EventCriteria
        .havingTags(tag("courseId", "cs-101"))
        .ofTypes(qn("university", "CourseCreated"))

      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual(["university.CourseCreated"])
      }
    })
  })

  describe("either", () => {
    it("combines multiple criteria with logical OR", () => {
      const criteria = EventCriteria.either(
        EventCriteria.havingTags(tag("courseId", "cs-101")),
        EventCriteria.havingTags(tag("studentId", "stu-001")),
      )

      expect(criteria.kind).toBe("either")
      if (criteria.kind === "either") {
        expect(criteria.criteria).toHaveLength(2)
      }
    })

    it("supports nested either with type restrictions", () => {
      const criteria = EventCriteria.either(
        EventCriteria
          .havingTags(tag("courseId", "cs-101"))
          .ofTypes("CourseCreated"),
        EventCriteria
          .havingTags(tag("studentId", "stu-001"))
          .ofTypes("StudentEnrolled"),
      )

      expect(criteria.kind).toBe("either")
      if (criteria.kind === "either") {
        expect(criteria.criteria[0]!.kind).toBe("type-restricted")
        expect(criteria.criteria[1]!.kind).toBe("type-restricted")
      }
    })
  })
})
