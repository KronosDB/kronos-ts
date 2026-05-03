import type { EventSourcingConfigurer } from "@kronos-ts/eventsourcing"
import type { App } from "@kronos-ts/core"
import { trackingProcessor } from "@kronos-ts/messaging"
import { CourseEntity } from "./entity.js"
import { createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent } from "./command-handlers.js"
import { courseProjection, courseQueries } from "./projections.js"

/**
 * Course domain slice configuration — NEW kronos() App shape.
 *
 * Used by tests that drive through the kronos() App fluent surface
 * (university.integration.test.ts via createTestFixture; future
 * e2e-* migrations under Plan 03).
 */
export function configureCoursesApp(app: App): void {
  app.entities(CourseEntity)
  app.commands(createCourse, changeCourseCapacity, subscribeStudent, unsubscribeStudent)
  app.queries(courseQueries)
  app.processors(
    trackingProcessor("course-projection")
      .registerEventHandler(courseProjection)
      .build(),
  )
}

/**
 * Course domain slice configuration — LEGACY EventSourcingConfigurer shape.
 *
 * Still consumed by e2e-* integration tests that exercise EventSourcingConfigurer
 * directly. Plan 03 migrates those tests; Plan 04 deletes this export along with
 * the EventSourcingConfigurer surface.
 *
 * DO NOT add new callers — use {@link configureCoursesApp} instead.
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
