import type { EntityModule } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlersDefinition,
  EventHandlersDefinition,
  EventProcessorModule,
} from "@kronos-ts/messaging"
import { AppImpl, type App } from "./app.js"
import { registerInMemoryDefaults } from "./defaults.js"
import { createWarningChannel, type WarningLogger } from "./warnings.js"

/**
 * Partial-config shorthand options for kronos(). APP-02.
 *
 * Domain registrations passed here are appended to the same internal accumulators
 * as fluent .entities()/.commands()/etc. calls. quiet/logger configure the warning
 * channel BEFORE in-memory defaults are registered.
 */
export interface KronosPartialConfig {
  entities?: EntityModule[]
  commands?: CommandHandlerDefinition<any, any>[]
  queries?: QueryHandlersDefinition[]
  events?: EventHandlersDefinition[]
  processors?: EventProcessorModule[]
  quiet?: boolean
  logger?: WarningLogger
}

/**
 * Create a new Kronos App.
 *
 * ```typescript
 * const app = await kronos()
 *   .entities(CourseEntity)
 *   .commands(createCourseHandler)
 *   .start()
 *
 * await app.commandGateway.send(CreateCourse, { courseId: "cs-101", name: "Intro" }, emptyMetadata())
 * ```
 *
 * Or with a partial config (APP-02):
 *
 * ```typescript
 * const app = await kronos({ entities: [CourseEntity], commands: [createCourseHandler], quiet: true }).start()
 * ```
 */
export function kronos(partial?: KronosPartialConfig): App {
  const warningChannel = createWarningChannel({ quiet: partial?.quiet, logger: partial?.logger })
  const app = new AppImpl({ warningChannel })

  // Register in-memory defaults FIRST so user partial-config / fluent calls override them
  // via set/forceSet (setDefault is ifAbsent — first registration wins).
  registerInMemoryDefaults(app)

  if (partial?.entities) app.entities(...partial.entities)
  if (partial?.commands) app.commands(...partial.commands)
  if (partial?.queries) app.queries(...partial.queries)
  if (partial?.events) app.events(...partial.events)
  if (partial?.processors) app.processors(...partial.processors)

  return app
}
