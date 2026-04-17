import { on, eventHandlers, queryHandlers } from "@kronos-ts/messaging"
import {
  CourseCreated, CourseCapacityChanged, StudentSubscribed, StudentUnsubscribed,
  GetCourseView, GetAllCourses,
} from "./messages.js"

// ---------------------------------------------------------------------------
// Read model — in-memory (will be replaced with database in future)
// ---------------------------------------------------------------------------

export type CourseView = {
  courseId: string
  name: string
  capacity: number
  enrolledCount: number
  students: string[]
}

const courseViews = new Map<string, CourseView>()

export function getCourseViews() { return courseViews }
export function clearCourseViews() { courseViews.clear() }

// ---------------------------------------------------------------------------
// Projection — event handler group
// ---------------------------------------------------------------------------

export const courseProjection = eventHandlers({
  name: "course-projection",
  handlers: [
    on(CourseCreated, async (e, ctx) => {
      courseViews.set(e.courseId, {
        courseId: e.courseId,
        name: e.name,
        capacity: e.capacity,
        enrolledCount: 0,
        students: [],
      })
      ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
    }),
    on(CourseCapacityChanged, async (e, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.capacity = e.capacity
        ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
      }
    }),
    on(StudentSubscribed, async (e, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.enrolledCount++
        view.students.push(e.studentId)
        ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
      }
    }),
    on(StudentUnsubscribed, async (e, ctx) => {
      const view = courseViews.get(e.courseId)
      if (view) {
        view.enrolledCount--
        view.students = view.students.filter((id) => id !== e.studentId)
        ctx.emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
      }
    }),
  ],
  onReset: async () => {
    courseViews.clear()
  },
})

// ---------------------------------------------------------------------------
// Query handlers
// ---------------------------------------------------------------------------

export const courseQueries = queryHandlers({
  name: "course-queries",
  handlers: [
    on(GetCourseView, async (q) => {
      const view = courseViews.get(q.courseId)
      if (!view) throw new Error(`Course "${q.courseId}" not found`)
      return view
    }),
    on(GetAllCourses, async () => {
      return [...courseViews.values()].map((v) => ({
        courseId: v.courseId,
        name: v.name,
        enrolledCount: v.enrolledCount,
      }))
    }),
  ],
})
