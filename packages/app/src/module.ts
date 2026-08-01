import {
  commandHandler,
  eventHandler,
  HANDLER_CONTEXT,
  EVENT_HANDLER_CONTEXT,
  type HandlerContext,
  type EventHandlerContext,
  type CommandHandlerDefinition,
  type EventHandlerDefinition,
  type CommandDescriptor,
  type EventDescriptor,
  type CommandMessage,
  type SequencedEventMessage,
  type EventCriteria,
  type QueryHandlerDefinition,
  type EventProcessorModule,
} from "@kronos-ts/messaging"
import type { z } from "zod"
import type { App, Extension, StatesArg } from "./app.js"

// ---------------------------------------------------------------------------
// Module composition — encapsulated dependency injection over the handler
// context.
//
// A module declares its dependency TYPES at definition time and receives the
// VALUES at the composition root (`app.use(mod.with(deps))`), Fastify-plugin
// style. Every handler declared through the module's factories receives a
// context that is the frozen merge of the framework capabilities and the
// module's deps — `HandlerContext & Deps` — built ONCE per configuration.
// Two modules (or two configurations of the same module) never share deps:
// isolation is by closure, not by registry, so there is no global mutable
// state and no name-collision surface between modules.
// ---------------------------------------------------------------------------

/** Framework capability names — module deps may not shadow these. */
const RESERVED_CONTEXT_KEYS = new Set([
  "append",
  "load",
  "send",
  "emitUpdate",
  "transaction",
  "schedule",
  "scheduleAfter",
  "cancelSchedule",
])

/** Thrown when a module's deps would shadow a framework capability. */
export class ReservedContextKeyError extends Error {
  constructor(moduleName: string, key: string) {
    super(
      `Module "${moduleName}": dep "${key}" would shadow the framework capability ` +
        `ctx.${key}. Rename the dep — reserved keys: ${[...RESERVED_CONTEXT_KEYS].join(", ")}.`,
    )
    this.name = "ReservedContextKeyError"
  }
}

/** The command-handler context a module hands its handlers: capabilities + deps. */
export type ModuleHandlerContext<Deps> = HandlerContext & Readonly<Deps>

/** The event-handler context a module hands its handlers: capabilities + deps. */
export type ModuleEventHandlerContext<Deps> = EventHandlerContext & Readonly<Deps>

/**
 * The registration surface a module's `setup` callback receives. Handler
 * factories mirror `commandHandler` / `eventHandler` but bind the module's
 * merged context; everything else passes through to the underlying {@link App}
 * so a module is a complete registration scope, not just a handler namespace.
 */
export interface ModuleApi<Deps> {
  readonly name: string
  /** The configured dependency values (frozen). */
  readonly deps: Readonly<Deps>
  /**
   * Declare and register a command handler whose context carries the module
   * deps. Registered on the app immediately; also returned for tests.
   */
  command<P extends z.ZodType>(
    descriptor: CommandDescriptor<P, undefined>,
    handler: (message: CommandMessage<z.infer<P>>, context: ModuleHandlerContext<Deps>) => Promise<void> | void,
  ): CommandHandlerDefinition<P, undefined>
  command<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    handler: (
      message: CommandMessage<z.infer<P>>,
      context: ModuleHandlerContext<Deps>,
    ) => Promise<z.infer<R>> | z.infer<R>,
  ): CommandHandlerDefinition<P, R>
  command<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    options: {
      handler: (
        message: CommandMessage<z.infer<P>>,
        context: ModuleHandlerContext<Deps>,
      ) => Promise<z.infer<R>> | z.infer<R>
      appendCondition?: (message: CommandMessage<z.infer<P>>, sourcedCriteria: EventCriteria) => EventCriteria
    },
  ): CommandHandlerDefinition<P, R>
  /**
   * Declare an event handler whose context carries the module deps. NOT
   * registered on the app — event handlers belong to a processor, so the
   * definition is returned for use in `processors(...)` / `.eventHandlers(...)`.
   */
  event<P extends z.ZodType>(
    descriptor: EventDescriptor<P>,
    handler: (
      message: SequencedEventMessage<z.infer<P>>,
      context: ModuleEventHandlerContext<Deps>,
    ) => Promise<void> | void,
  ): EventHandlerDefinition<P>
  /** Register state modules on the app (passthrough). */
  states(...args: StatesArg[]): void
  /** Register query handlers on the app (passthrough). */
  queries(...handlers: QueryHandlerDefinition[]): void
  /** Register event processors on the app (passthrough). */
  processors(...modules: EventProcessorModule[]): void
  /** The underlying app — escape hatch for advanced registration. */
  readonly app: App
}

/** A defined module: bind dependency values with `.with(deps)` to get an Extension. */
export interface ModuleDefinition<Deps> {
  readonly moduleName: string
  /**
   * Bind dependency values and produce an app Extension. Each call creates an
   * independent configuration — registering the same module twice with
   * different deps yields two fully isolated instances.
   */
  with(deps: Deps): Extension
}

/** A module with no deps also composes with plain `.use(mod.extension)`. */
export interface DeplessModuleDefinition extends ModuleDefinition<Record<never, never>> {
  readonly extension: Extension
}

/**
 * Define a module: a named registration scope with typed, isolated
 * dependency injection.
 *
 * ```ts
 * interface SupportDeps { db: Db; storage: Storage }
 *
 * export const supportModule = defineModule<SupportDeps>("support", (m) => {
 *   m.command(OpenTicket, async ({ payload }, ctx) => {
 *     await ctx.db.insert(messageBodies).values({ ... })   // ctx.db — typed dep
 *     ctx.append(TicketOpened, { ticketId: payload.ticketId })
 *   })
 * })
 *
 * // composition root — deps bound here, per configuration:
 * kronos()
 *   .use(supportModule.with({ db, storage }))
 *   .use(schedulingModule.with({ db: schedulingDb }))   // different deps, zero bleed
 * ```
 */
export function defineModule<Deps extends Record<string, unknown> = Record<never, never>>(
  name: string,
  setup: (m: ModuleApi<Deps>) => void,
): ModuleDefinition<Deps> {
  const withDeps = (deps: Deps): Extension => {
    for (const key of Object.keys(deps)) {
      if (RESERVED_CONTEXT_KEYS.has(key)) throw new ReservedContextKeyError(name, key)
    }
    return (app: App) => {
      const frozenDeps = Object.freeze({ ...deps })
      const commandContext = Object.freeze({ ...HANDLER_CONTEXT, ...frozenDeps }) as ModuleHandlerContext<Deps>
      const eventContext = Object.freeze({ ...EVENT_HANDLER_CONTEXT, ...frozenDeps }) as ModuleEventHandlerContext<Deps>

      const api: ModuleApi<Deps> = {
        name,
        deps: frozenDeps,
        app,
        command(descriptor: CommandDescriptor<any, any>, handlerOrOptions: any) {
          const bare = typeof handlerOrOptions === "function" ? handlerOrOptions : handlerOrOptions.handler
          const definition = commandHandler(descriptor, {
            handler: (message: CommandMessage<any>) => bare(message, commandContext),
            ...(typeof handlerOrOptions === "object" && handlerOrOptions.appendCondition
              ? { appendCondition: handlerOrOptions.appendCondition }
              : {}),
          })
          app.commands(definition)
          return definition
        },
        event(descriptor: EventDescriptor<any>, bare: any) {
          return eventHandler(descriptor, (message: SequencedEventMessage<any>) => bare(message, eventContext))
        },
        states(...args) {
          app.states(...args)
        },
        queries(...handlers) {
          app.queries(...handlers)
        },
        processors(...modules) {
          app.processors(...modules)
        },
      }
      setup(api)
    }
  }
  return { moduleName: name, with: withDeps }
}
