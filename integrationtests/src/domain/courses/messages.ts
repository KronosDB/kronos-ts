import { z } from "zod"
import { tag } from "@kronos-ts/common"
import { withNamespace } from "@kronos-ts/messaging"

const ns = withNamespace("university.courses")

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

export const CreateCourse = ns.command("CreateCourse", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
})

export const ChangeCourseCapacity = ns.command("ChangeCourseCapacity", {
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
})

export const SubscribeStudent = ns.command("SubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

export const UnsubscribeStudent = ns.command("UnsubscribeStudent", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
})

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

export const CourseCreated = ns.event("CourseCreated", {
  payload: z.object({ courseId: z.string(), name: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

export const CourseCapacityChanged = ns.event("CourseCapacityChanged", {
  payload: z.object({ courseId: z.string(), capacity: z.number() }),
  tags: (p) => [tag("courseId", p.courseId)],
})

export const StudentSubscribed = ns.event("StudentSubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

export const StudentUnsubscribed = ns.event("StudentUnsubscribed", {
  payload: z.object({ courseId: z.string(), studentId: z.string() }),
  tags: (p) => [tag("courseId", p.courseId), tag("studentId", p.studentId)],
})

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

export const GetCourseView = ns.query("GetCourseView", {
  payload: z.object({ courseId: z.string() }),
})

export const GetAllCourses = ns.query("GetAllCourses", {
  payload: z.object({}),
})
