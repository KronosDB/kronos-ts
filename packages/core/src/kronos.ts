import type { EntityModule } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlersDefinition,
  EventHandlersDefinition,
  EventProcessorModule,
} from "@kronos-ts/messaging"
import {
  createInterceptingCommandBus,
  createInterceptingQueryBus,
  createInterceptingEventBus,
} from "@kronos-ts/messaging"
import { AppImpl, type App } from "./app.js"
import { registerInMemoryDefaults } from "./defaults.js"
import { createWarningChannel, type WarningLogger } from "./warnings.js"
import { Defaults } from "./defaults-handles.js"

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
  /**
   * Per-stage timeout (ms) for native lifecycle execution (D-77).
   * If a single stage exceeds this, AppImpl emits a warning and continues
   * to the next stage WITHOUT cancelling the slow hooks (warn-then-continue).
   *
   * Default: 5000.
   */
  stageTimeoutMs?: number
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
  const app = new AppImpl({ warningChannel, stageTimeoutMs: partial?.stageTimeoutMs })

  // Register in-memory defaults FIRST so user partial-config / fluent calls override them
  // via set/forceSet (setDefault is ifAbsent — first registration wins).
  registerInMemoryDefaults(app)

  // Register framework-default `intercepting` decorators for the 3 buses (DEC-02, D-57).
  // Each closure reads `app._state.{bus}DispatchInterceptors` + `app._state.handlerInterceptors`
  // at decoration time (i.e., during .start()) — extensions and user code populate these
  // arrays before .start() runs, so the snapshot is complete by the time the closure fires.
  // Removable via `removeDecorator(Defaults.{bus}.intercepting)`.
  app._registerFrameworkDefaultDecorator(
    Defaults.commandBus.intercepting,
    (inner, _resolved) => {
      const wrapped = createInterceptingCommandBus(inner)
      for (const fn of app._state.commandDispatchInterceptors) wrapped.registerDispatchInterceptor(fn)
      for (const fn of app._state.handlerInterceptors) wrapped.registerHandlerInterceptor(fn)
      return wrapped
    },
  )
  app._registerFrameworkDefaultDecorator(
    Defaults.queryBus.intercepting,
    (inner, _resolved) => {
      const wrapped = createInterceptingQueryBus(inner)
      for (const fn of app._state.queryDispatchInterceptors) wrapped.registerDispatchInterceptor(fn)
      for (const fn of app._state.handlerInterceptors) wrapped.registerHandlerInterceptor(fn)
      return wrapped
    },
  )
  app._registerFrameworkDefaultDecorator(
    Defaults.eventBus.intercepting,
    (inner, _resolved) => {
      // EventBus intercepting wrapper takes its dispatch interceptors at construction time;
      // it has no register* methods and no handler interceptor concept (verified
      // packages/messaging/src/intercepting-event-bus.ts — no handlerInterceptors field).
      // Snapshot the array at decoration time; mutations after .start() are blocked
      // by the AppAlreadyStartedError guard on `eventDispatchInterceptor()`.
      return createInterceptingEventBus(inner, [...app._state.eventDispatchInterceptors])
    },
  )

  if (partial?.entities) app.entities(...partial.entities)
  if (partial?.commands) app.commands(...partial.commands)
  if (partial?.queries) app.queries(...partial.queries)
  if (partial?.events) app.events(...partial.events)
  if (partial?.processors) app.processors(...partial.processors)

  return app
}
