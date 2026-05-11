import { on, eventHandler, queryHandlers, emitUpdate } from "@kronos-ts/messaging"
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
// Projection — singular event handlers (Plan 11-02 flat shape)
// ---------------------------------------------------------------------------

export const onCourseCreated = eventHandler(CourseCreated, async (e, _metadata) => {
  courseViews.set(e.courseId, {
    courseId: e.courseId,
    name: e.name,
    capacity: e.capacity,
    enrolledCount: 0,
    students: [],
  })
  emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, courseViews.get(e.courseId))
})

export const onCourseCapacityChanged = eventHandler(CourseCapacityChanged, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.capacity = e.capacity
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

export const onStudentSubscribed = eventHandler(StudentSubscribed, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount++
    view.students.push(e.studentId)
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

export const onStudentUnsubscribed = eventHandler(StudentUnsubscribed, async (e, _metadata) => {
  const view = courseViews.get(e.courseId)
  if (view) {
    view.enrolledCount--
    view.students = view.students.filter((id) => id !== e.studentId)
    emitUpdate(GetCourseView, (q) => q.courseId === e.courseId, view)
  }
})

export const courseProjectionHandlers = [
  onCourseCreated,
  onCourseCapacityChanged,
  onStudentSubscribed,
  onStudentUnsubscribed,
]

export const courseProjectionOnReset = async (): Promise<void> => {
  courseViews.clear()
}

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
