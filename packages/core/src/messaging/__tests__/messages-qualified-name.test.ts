import { describe, expect, it } from "bun:test"
import { qn, qualifiedNameToString, qualifiedNameFromString, qualifiedNamesEqual } from "../messages.js"

describe("QualifiedName", () => {
  describe("qn", () => {
    it("creates a qualified name", () => {
      const name = qn("university.courses", "CreateCourse")

      expect(name.namespace).toBe("university.courses")
      expect(name.name).toBe("CreateCourse")
    })
  })

  describe("qualifiedNameToString", () => {
    it("serializes to dot-separated string", () => {
      const name = qn("university.courses", "CreateCourse")

      expect(qualifiedNameToString(name)).toBe("university.courses.CreateCourse")
    })
  })

  describe("qualifiedNameFromString", () => {
    it("splits on last dot", () => {
      const name = qualifiedNameFromString("university.courses.CreateCourse")

      expect(name.namespace).toBe("university.courses")
      expect(name.name).toBe("CreateCourse")
    })

    it("handles single segment with no dot", () => {
      const name = qualifiedNameFromString("CreateCourse")

      expect(name.namespace).toBe("")
      expect(name.name).toBe("CreateCourse")
    })
  })

  describe("qualifiedNamesEqual", () => {
    it("returns true for equal names", () => {
      const a = qn("university.courses", "CreateCourse")
      const b = qn("university.courses", "CreateCourse")

      expect(qualifiedNamesEqual(a, b)).toBe(true)
    })

    it("returns false when namespace differs", () => {
      const a = qn("university.courses", "CreateCourse")
      const b = qn("university.students", "CreateCourse")

      expect(qualifiedNamesEqual(a, b)).toBe(false)
    })

    it("returns false when name differs", () => {
      const a = qn("university.courses", "CreateCourse")
      const b = qn("university.courses", "DeleteCourse")

      expect(qualifiedNamesEqual(a, b)).toBe(false)
    })
  })
})
