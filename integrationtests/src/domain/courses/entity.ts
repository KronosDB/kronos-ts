import { z } from "zod"
import { tag } from "@kronos-ts/common"
import { on, EventCriteria } from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { CourseCreated, CourseCapacityChanged, StudentSubscribed, StudentUnsubscribed } from "./messages.js"

export type CourseState = {
  created: boolean
  name: string
  capacity: number
  enrolled: string[]
}

export const CourseEntity = eventSourcedEntity({
  name: "Course",
  id: { courseId: z.string() },
  initial: () => ({ created: false, name: "", capacity: 0, enrolled: [] }),
  criteria: (id) => EventCriteria.havingTags(tag("courseId", id.courseId)),
  evolve: [
    on(CourseCreated, (s: CourseState, e) => ({
      ...s, created: true, name: e.name, capacity: e.capacity,
    })),
    on(CourseCapacityChanged, (s: CourseState, e) => ({
      ...s, capacity: e.capacity,
    })),
    on(StudentSubscribed, (s: CourseState, e) => ({
      ...s, enrolled: [...s.enrolled, e.studentId],
    })),
    on(StudentUnsubscribed, (s: CourseState, e) => ({
      ...s, enrolled: s.enrolled.filter((sid) => sid !== e.studentId),
    })),
  ],
})
