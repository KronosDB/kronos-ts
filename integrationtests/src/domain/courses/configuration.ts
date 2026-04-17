import type { EventSourcingConfigurer } from "@kronos-ts/eventsourcing"
import { trackingProcessor } from "@kronos-ts/messaging"
import { CourseEntity } from "./entity.js"
import { createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent } from "./command-handlers.js"
import { courseProjection, courseQueries } from "./projections.js"

/**
 * Course domain slice configuration.
 * Registers all entities, command handlers, projections, and query handlers.
 */
export function configureCourses(c: EventSourcingConfigurer) {
  c.registerEntity(CourseEntity)
  c.messaging(m => {
    m.registerCommandHandler(() => createCourse)
    m.registerCommandHandler(() => changeCourseCapacity)
    m.registerCommandHandler(() => subscribeStudent)
    m.registerCommandHandler(() => unsubscribeStudent)
    m.registerEventProcessor(config =>
      trackingProcessor("course-projection")
        .registerEventHandler(courseProjection)
        .build()
    )
    m.registerQueryHandlers(() => courseQueries)
  })
}
