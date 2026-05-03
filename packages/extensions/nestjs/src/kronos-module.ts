import type { App, RunningApp } from "@kronos-ts/core"
import { kronos } from "@kronos-ts/core"
import type { CommandGateway, QueryGateway } from "@kronos-ts/messaging"

/**
 * Injection tokens for Kronos components in NestJS DI container.
 */
export const KRONOS_APPLICATION = Symbol("KRONOS_APPLICATION")
export const KRONOS_COMMAND_GATEWAY = Symbol("KRONOS_COMMAND_GATEWAY")
export const KRONOS_QUERY_GATEWAY = Symbol("KRONOS_QUERY_GATEWAY")
export const KRONOS_APP = Symbol("KRONOS_APP")

/**
 * Options for `KronosModule.forRoot()`.
 */
export interface KronosModuleOptions {
  /**
   * Configure function that receives the unstarted Kronos App.
   * Register entities, handlers, processors, and extensions via the fluent App API.
   */
  configure: (app: App) => void
}

/**
 * Async options for `KronosModule.forRootAsync()`.
 * Allows injecting NestJS providers into the configuration.
 */
export interface KronosModuleAsyncOptions {
  /**
   * NestJS providers to inject into the factory.
   */
  inject?: any[]
  /**
   * Factory function that receives injected dependencies and returns
   * the unstarted App.
   */
  useFactory: (...args: any[]) => App | Promise<App>
}

/**
 * NestJS DynamicModule definition for Kronos Framework.
 *
 * Integrates Kronos with NestJS's dependency injection and lifecycle:
 * - `OnModuleInit`: builds and starts the Kronos application
 * - `OnModuleDestroy`: stops the Kronos application
 * - Provides `CommandGateway` and `QueryGateway` as injectable services
 *
 * ```typescript
 * @Module({
 *   imports: [
 *     KronosModule.forRoot({
 *       configure: (app) => {
 *         app
 *           .entities(CourseEntity)
 *           .commands(createCourse)
 *           .processors(
 *             trackingProcessor("courses").registerEventHandler(projection).build(),
 *           )
 *       },
 *     }),
 *   ],
 * })
 * export class AppModule {}
 *
 * // In a controller:
 * @Controller("courses")
 * export class CourseController {
 *   constructor(
 *     @Inject(KRONOS_COMMAND_GATEWAY) private commands: CommandGateway,
 *     @Inject(KRONOS_QUERY_GATEWAY) private queries: QueryGateway,
 *   ) {}
 * }
 * ```
 */
export const KronosModule = {
  /**
   * Synchronous configuration. Use when no NestJS providers are needed
   * during Kronos configuration.
   */
  forRoot(options: KronosModuleOptions) {
    return {
      module: KronosModule as any,
      global: true,
      providers: [
        {
          provide: KRONOS_APP,
          useFactory: () => {
            const app = kronos()
            options.configure(app)
            return app
          },
        },
        ...kronosProviders(),
      ],
      exports: [KRONOS_APPLICATION, KRONOS_COMMAND_GATEWAY, KRONOS_QUERY_GATEWAY],
    }
  },

  /**
   * Async configuration. Use when NestJS providers need to be injected
   * (e.g., ConfigService, database connections).
   */
  forRootAsync(options: KronosModuleAsyncOptions) {
    return {
      module: KronosModule as any,
      global: true,
      providers: [
        {
          provide: KRONOS_APP,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        ...kronosProviders(),
      ],
      exports: [KRONOS_APPLICATION, KRONOS_COMMAND_GATEWAY, KRONOS_QUERY_GATEWAY],
    }
  },
}

function kronosProviders() {
  return [
    {
      provide: KRONOS_APPLICATION,
      useFactory: async (app: App): Promise<RunningApp> => app.start(),
      inject: [KRONOS_APP],
    },
    {
      provide: KRONOS_COMMAND_GATEWAY,
      useFactory: (app: RunningApp): CommandGateway => app.commandGateway,
      inject: [KRONOS_APPLICATION],
    },
    {
      provide: KRONOS_QUERY_GATEWAY,
      useFactory: (app: RunningApp): QueryGateway => app.queryGateway,
      inject: [KRONOS_APPLICATION],
    },
  ]
}
