import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { compileQuery, queryItems, type EventQuery } from "../event-query.js"
import { event } from "../../messages/descriptor.js"

describe("queryItems", () => {
  it("wraps a single item", () => {
    expect(queryItems({ tags: { courseId: "cs-101" } })).toEqual([
      { tags: { courseId: "cs-101" } },
    ])
  })

  it("passes an array of items through", () => {
    const items: EventQuery = [{ tags: { courseId: "cs-101" } }, { tags: { studentId: "stu-001" } }]
    expect(queryItems(items)).toEqual(items)
  })

  it("rejects an EMPTY array — zero ORed items matches nothing", () => {
    expect(() => queryItems([])).toThrow(/cannot be an EMPTY array/)
  })

  it("rejects a non-item query with a message naming what it got", () => {
    expect(() => queryItems("cs-101" as unknown as EventQuery)).toThrow(
      /must be a query item .* but got string/,
    )
    expect(() => queryItems(null as unknown as EventQuery)).toThrow(/but got null/)
  })

  it("rejects a NESTED array — the items of a query are one flat list", () => {
    expect(() =>
      queryItems([[{ tags: { courseId: "cs-101" } }]] as unknown as EventQuery),
    ).toThrow(/Item 0 .* but got an array/)
  })
})

describe("compileQuery", () => {
  describe("tags", () => {
    it("compiles a tag record to a tag criteria", () => {
      const criteria = compileQuery({ tags: { courseId: "cs-101" } })

      expect(criteria.kind).toBe("tags")
      if (criteria.kind === "tags") {
        expect(criteria.tags).toEqual([{ key: "courseId", value: "cs-101" }])
      }
    })

    it("supports multiple tags", () => {
      const criteria = compileQuery({ tags: { courseId: "cs-101", studentId: "stu-001" } })

      expect(criteria.kind).toBe("tags")
      if (criteria.kind === "tags") {
        expect(criteria.tags).toEqual([
          { key: "courseId", value: "cs-101" },
          { key: "studentId", value: "stu-001" },
        ])
      }
    })

    it("an empty item restricts nothing — a tag criteria with no tags", () => {
      const criteria = compileQuery({})

      expect(criteria.kind).toBe("tags")
      if (criteria.kind === "tags") {
        expect(criteria.tags).toEqual([])
      }
    })
  })

  describe("types", () => {
    it("restricts by event descriptor types", () => {
      const CourseCreated = event({
        name: qn("university", "CourseCreated"),
        payload: z.object({ courseId: z.string() }),
      })

      const criteria = compileQuery({ tags: { courseId: "cs-101" }, types: [CourseCreated] })

      expect(criteria.kind).toBe("type-restricted")
      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual(["university.CourseCreated"])
        expect(criteria.inner).toEqual({
          kind: "tags",
          tags: [{ key: "courseId", value: "cs-101" }],
        })
      }
    })

    it("accepts string type names", () => {
      const criteria = compileQuery({
        tags: { courseId: "cs-101" },
        types: ["university.CourseCreated", "university.CourseDeleted"],
      })

      expect(criteria.kind).toBe("type-restricted")
      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual([
          "university.CourseCreated",
          "university.CourseDeleted",
        ])
      }
    })

    it("accepts qualified names", () => {
      const criteria = compileQuery({
        tags: { courseId: "cs-101" },
        types: [qn("university", "CourseCreated")],
      })

      expect(criteria.kind).toBe("type-restricted")
      if (criteria.kind === "type-restricted") {
        expect(criteria.types).toEqual(["university.CourseCreated"])
      }
    })

    it("an EMPTY type list applies no restriction — an empty fold means 'all', not 'none'", () => {
      const criteria = compileQuery({ tags: { courseId: "cs-101" }, types: [] })

      expect(criteria.kind).toBe("tags")
    })
  })

  describe("several items are a logical OR", () => {
    it("compiles to an either criteria", () => {
      const criteria = compileQuery([
        { tags: { courseId: "cs-101" } },
        { tags: { studentId: "stu-001" } },
      ])

      expect(criteria.kind).toBe("either")
      if (criteria.kind === "either") {
        expect(criteria.criteria).toHaveLength(2)
        expect(criteria.criteria[0]!.kind).toBe("tags")
        expect(criteria.criteria[1]!.kind).toBe("tags")
      }
    })

    it("each item keeps its own type restriction", () => {
      const criteria = compileQuery([
        { tags: { courseId: "cs-101" }, types: ["CourseCreated"] },
        { tags: { studentId: "stu-001" }, types: ["StudentEnrolled"] },
      ])

      expect(criteria.kind).toBe("either")
      if (criteria.kind === "either") {
        expect(criteria.criteria[0]!.kind).toBe("type-restricted")
        expect(criteria.criteria[1]!.kind).toBe("type-restricted")
      }
    })

    it("a ONE-item array compiles like the bare item — no either wrapper", () => {
      expect(compileQuery([{ tags: { courseId: "cs-101" } }])).toEqual(
        compileQuery({ tags: { courseId: "cs-101" } }),
      )
    })
  })

  describe("a query literal keeps its excess-property check", () => {
    // Compile-time assertions only — never invoked. `EventQuery` is a UNION
    // (one item | many), and a union target must NOT weaken TypeScript's
    // excess-property check: a typo'd field in a query literal has to stay an
    // error, at the literal, or the plain-data form silently swallows mistakes
    // that `eventQuery({ … })` used to catch. Each @ts-expect-error below fails
    // the build the moment that stops being true.
    const _typeOnly = () => {
      // @ts-expect-error - `type` is not a field; the field is `types`
      const _typoSingle: EventQuery = { tags: { courseId: "cs-101" }, type: ["X"] }
      // @ts-expect-error - `tag` is not a field; the field is `tags`
      const _typoTag: EventQuery = { tag: { courseId: "cs-101" } }
      // @ts-expect-error - a typo INSIDE an array item is caught at that item
      const _typoInItem: EventQuery = [{ tags: { a: "1" } }, { tags: { b: "2" }, kinds: [] }]
      // @ts-expect-error - the store-facing `EventCriteria` union is not a query
      const _notCriteria: EventQuery = { kind: "tags", tags: [{ key: "a", value: "1" }] }
      // @ts-expect-error - tag VALUES are strings, not arbitrary data
      const _badTagValue: EventQuery = { tags: { courseId: 101 } }
      // Well-formed literals in both shapes stay assignable.
      const _okSingle: EventQuery = { tags: { courseId: "cs-101" }, types: ["X"] }
      const _okArray: EventQuery = [{ tags: { a: "1" } }, { types: ["X"] }]
      const _okEmpty: EventQuery = {}
      return [_typoSingle, _typoTag, _typoInItem, _notCriteria, _badTagValue, _okSingle, _okArray, _okEmpty]
    }
    it("is enforced by the compiler, not at runtime", () => {
      expect(typeof _typeOnly).toBe("function")
    })
  })
})
