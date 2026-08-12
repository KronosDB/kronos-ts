import { describe, expect, it } from "bun:test"
import { qn, tag, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import { EventCriteria, type EventMessage } from "@kronos-ts/messaging"
import { inMemoryEventStore, AppendConditionError } from "../in-memory-event-store.js"
import { sourcingCondition } from "../sourcing-condition.js"
import { appendCondition } from "../append-condition.js"

function eventMessage(name: string, payload: unknown, tags: Array<{ key: string; value: string }>): EventMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    version: "1.0",
    payload,
    metadata: emptyMetadata(),
    timestamp: Date.now(),
    tags,
  }
}

describe("InMemoryEventStore", () => {
  describe("append and source", () => {
    it("appends events and sources them back by tag criteria", async () => {
      const store = inMemoryEventStore()

      // given
      const event1 = eventMessage("CourseCreated", { courseId: "cs-101" }, [tag("courseId", "cs-101")])
      const event2 = eventMessage("CourseCreated", { courseId: "cs-102" }, [tag("courseId", "cs-102")])
      await store.append([event1, event2])

      // when
      const result = await store.source(
        sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
      )

      // then
      expect(result.events).toHaveLength(1)
      expect(result.events[0]!.payload).toEqual({ courseId: "cs-101" })
    })

    it("returns empty events when nothing matches", async () => {
      const store = inMemoryEventStore()

      // when
      const result = await store.source(
        sourcingCondition(EventCriteria.havingTags(tag("courseId", "nonexistent"))),
      )

      // then
      expect(result.events).toHaveLength(0)
    })

    it("sources events matching either criteria", async () => {
      const store = inMemoryEventStore()

      // given
      const courseEvent = eventMessage("CourseCreated", {}, [tag("courseId", "cs-101")])
      const studentEvent = eventMessage("StudentEnrolled", {}, [tag("studentId", "stu-001")])
      const unrelatedEvent = eventMessage("Other", {}, [tag("other", "value")])
      await store.append([courseEvent, studentEvent, unrelatedEvent])

      // when
      const result = await store.source(
        sourcingCondition(
          EventCriteria.either(
            EventCriteria.havingTags(tag("courseId", "cs-101")),
            EventCriteria.havingTags(tag("studentId", "stu-001")),
          ),
        ),
      )

      // then
      expect(result.events).toHaveLength(2)
    })

    it("sources events with type restriction", async () => {
      const store = inMemoryEventStore()

      // given
      const created = eventMessage("CourseCreated", {}, [tag("courseId", "cs-101")])
      const changed = eventMessage("CourseCapacityChanged", {}, [tag("courseId", "cs-101")])
      await store.append([created, changed])

      // when
      const result = await store.source(
        sourcingCondition(
          EventCriteria
            .havingTags(tag("courseId", "cs-101"))
            .ofTypes("test.CourseCreated"),
        ),
      )

      // then
      expect(result.events).toHaveLength(1)
      expect(result.events[0]!.name.name).toBe("CourseCreated")
    })
  })

  describe("append condition", () => {
    it("succeeds when no conflicting events exist", async () => {
      const store = inMemoryEventStore()

      // given
      const event1 = eventMessage("CourseCreated", {}, [tag("courseId", "cs-101")])
      await store.append([event1])

      const { marker } = await store.source(
        sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
      )

      // when — append with condition, no conflict
      const event2 = eventMessage("CourseCapacityChanged", {}, [tag("courseId", "cs-101")])
      const condition = appendCondition(
        EventCriteria.havingTags(tag("courseId", "cs-101")),
        marker,
      )

      // then — should not throw
      await store.append([event2], condition)
    })

    it("fails when conflicting events exist after marker", async () => {
      const store = inMemoryEventStore()

      // given
      const event1 = eventMessage("CourseCreated", {}, [tag("courseId", "cs-101")])
      await store.append([event1])

      const { marker } = await store.source(
        sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
      )

      // A conflicting event is appended after our marker
      const conflicting = eventMessage("CourseCapacityChanged", {}, [tag("courseId", "cs-101")])
      await store.append([conflicting])

      // when — try to append with the old marker
      const event2 = eventMessage("CourseRenamed", {}, [tag("courseId", "cs-101")])
      const condition = appendCondition(
        EventCriteria.havingTags(tag("courseId", "cs-101")),
        marker,
      )

      // then
      expect(store.append([event2], condition)).rejects.toThrow(AppendConditionError)
    })

    it("succeeds when conflicting events are outside the criteria", async () => {
      const store = inMemoryEventStore()

      // given
      const event1 = eventMessage("CourseCreated", {}, [tag("courseId", "cs-101")])
      await store.append([event1])

      const { marker } = await store.source(
        sourcingCondition(EventCriteria.havingTags(tag("courseId", "cs-101"))),
      )

      // An event for a DIFFERENT course is appended — not a conflict
      const nonConflicting = eventMessage("CourseCreated", {}, [tag("courseId", "cs-102")])
      await store.append([nonConflicting])

      // when — append with condition scoped to cs-101
      const event2 = eventMessage("CourseCapacityChanged", {}, [tag("courseId", "cs-101")])
      const condition = appendCondition(
        EventCriteria.havingTags(tag("courseId", "cs-101")),
        marker,
      )

      // then — should succeed because cs-102 event doesn't match our criteria
      await store.append([event2], condition)
    })
  })
})
