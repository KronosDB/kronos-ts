import type { App } from "@kronos-ts/core"
import { trackingProcessor } from "@kronos-ts/messaging"
import { CourseEntity } from "./entity.js"
import { createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent } from "./command-handlers.js"
import { courseProjection, courseQueries } from "./projections.js"

/**
 * Course domain slice configuration — kronos() App shape.
 *
 * Plan 08-03b collapsed Plan 02's transitional dual-export back into a single
 * native (app: App) => void shape. The legacy configurer-shaped variant has
 * been deleted; the only remaining shape is the native App API.
 */
export function configureCourses(app: App): void {
  app.entities(CourseEntity)
  app.commands(createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent)
  app.queries(courseQueries)
  app.processors(
    trackingProcessor("course-projection")
      .registerEventHandler(courseProjection)
      .build(),
  )
}
