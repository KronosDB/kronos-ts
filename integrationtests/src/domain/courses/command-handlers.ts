import { commandHandler } from "@kronos-ts/messaging"
import { CourseEntity } from "./entity.js"
import {
  CreateCourse, ChangeCourseCapacity, SubscribeStudent, UnsubscribeStudent,
  CourseCreated, CourseCapacityChanged, StudentSubscribed, StudentUnsubscribed,
} from "./messages.js"

export const createCourse = commandHandler(CreateCourse, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (course.created) throw new Error("Course already exists")
  append(CourseCreated, { courseId: cmd.courseId, name: cmd.name, capacity: cmd.capacity })
})

export const changeCourseCapacity = commandHandler(ChangeCourseCapacity, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (cmd.capacity === course.capacity) return
  if (cmd.capacity < course.enrolled.length) throw new Error("Cannot reduce capacity below enrolled count")
  append(CourseCapacityChanged, { courseId: cmd.courseId, capacity: cmd.capacity })
})

export const subscribeStudent = commandHandler(SubscribeStudent, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (course.enrolled.length >= course.capacity) throw new Error("Course is full")
  if (course.enrolled.includes(cmd.studentId)) throw new Error("Student already subscribed")
  append(StudentSubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})

export const unsubscribeStudent = commandHandler(UnsubscribeStudent, async (cmd, { load, append }) => {
  const course = await load(CourseEntity, { courseId: cmd.courseId })
  if (!course.created) throw new Error("Course does not exist")
  if (!course.enrolled.includes(cmd.studentId)) throw new Error("Student not subscribed")
  append(StudentUnsubscribed, { courseId: cmd.courseId, studentId: cmd.studentId })
})
