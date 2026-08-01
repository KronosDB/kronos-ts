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
import type { Slice } from "./slice.js"

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
  commandHandler<P extends z.ZodType>(
    descriptor: CommandDescriptor<P, undefined>,
    handler: (message: CommandMessage<z.infer<P>>, context: ModuleHandlerContext<Deps>) => Promise<void> | void,
  ): CommandHandlerDefinition<P, undefined>
  commandHandler<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    handler: (
      message: CommandMessage<z.infer<P>>,
      context: ModuleHandlerContext<Deps>,
    ) => Promise<z.infer<R>> | z.infer<R>,
  ): CommandHandlerDefinition<P, R>
  commandHandler<P extends z.ZodType, R extends z.ZodType>(
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
  eventHandler<P extends z.ZodType>(
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
  /**
   * Register slices: each slice's `register` runs against this module api (so
   * its handlers get the module context), and its name + meta are recorded on
   * the app for host iteration via `app.slices()`. Duplicate slice names
   * throw {@link import("./slice.js").DuplicateSliceNameError}.
   */
  slices(...slices: Slice<Deps, any>[]): void
  /** The underlying app — escape hatch for advanced registration. */
  readonly app: App
}

/**
 * A defined module IS a function: bind dependency values by calling it —
 * `app.use(mod({ db }))` — and each call creates an independent, fully
 * isolated configuration. No wrapper object, no `.with` ceremony: modules
 * compose the way every other closure in the framework does. `moduleName`
 * rides along as a property for diagnostics.
 */
export type Module<Deps> = ((deps: Deps) => Extension) & { readonly moduleName: string }

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
 *   .use(supportModule({ db, storage }))
 *   .use(schedulingModule({ db: schedulingDb }))   // different deps, zero bleed
 * ```
 */
export function defineModule<Deps extends Record<string, unknown> = Record<never, never>>(
  name: string,
  setup: (m: ModuleApi<Deps>) => void,
): Module<Deps> {
  const configure = (deps: Deps): Extension => {
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
        commandHandler(descriptor: CommandDescriptor<any, any>, handlerOrOptions: any) {
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
        eventHandler(descriptor: EventDescriptor<any>, bare: any) {
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
        slices(...sliceDefs) {
          for (const slice of sliceDefs) {
            app.slices({ name: slice.name, module: name, meta: slice.meta })
            slice.register(api)
          }
        },
      }
      setup(api)
    }
  }
  return Object.assign(configure, { moduleName: name })
}
