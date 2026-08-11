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
import type { StateModule } from "@kronos-ts/modelling"
import type { z } from "zod"
import type { App, Extension, StateOptions, StatesArg } from "./app.js"
import type { KronosComponents, SlotName } from "./components.js"
import type { SlotFactory } from "./slot-registry.js"
import { createModuleScope } from "./module-scope.js"

// ---------------------------------------------------------------------------
// Module composition — encapsulated dependency injection over the handler
// context.
//
// A module declares the TYPE of the dependencies it needs at definition time
// and receives the VALUES at the composition root (`app.use(mod(dependencies))`), Fastify-plugin
// style. Every handler declared through the module's factories receives a
// context that is the frozen merge of the framework capabilities and the
// module's own — `HandlerContext & Dependencies` — built ONCE per configuration.
// Two modules (or two configurations of the same module) never share dependencies:
// isolation is by closure, not by registry, so there is no global mutable
// state and no name-collision surface between modules.
// ---------------------------------------------------------------------------

/** Trace/error label for modules defined without an explicit name. */
const ANONYMOUS_MODULE = "module"

/** Framework capability names — a module's dependencies may not shadow these. */
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

/** Thrown when a module's dependencies would shadow a framework capability. */
export class ReservedContextKeyError extends Error {
  constructor(moduleName: string, key: string) {
    super(
      `Module "${moduleName}": dependency "${key}" would shadow the framework capability ` +
        `ctx.${key}. Rename it — reserved keys: ${[...RESERVED_CONTEXT_KEYS].join(", ")}.`,
    )
    this.name = "ReservedContextKeyError"
  }
}

/** The command-handler context a module hands its handlers: capabilities + dependencies. */
export type ModuleHandlerContext<Dependencies> = HandlerContext & Readonly<Dependencies>

/** The event-handler context a module hands its handlers: capabilities + dependencies. */
export type ModuleEventHandlerContext<Dependencies> = EventHandlerContext & Readonly<Dependencies>

/**
 * The registration surface a module's `setup` callback receives. Handler
 * factories mirror `commandHandler` / `eventHandler` but bind the module's
 * merged context; everything else passes through to the underlying {@link App}
 * so a module is a complete registration scope, not just a handler namespace.
 */
export interface ModuleApi<Dependencies> {
  readonly name: string
  /** The dependency values this module was configured with (frozen). */
  readonly dependencies: Readonly<Dependencies>
  /**
   * Declare and register a command handler whose context carries the module's
   * dependencies. Registered on the app immediately; also returned for tests.
   */
  commandHandler<P extends z.ZodType>(
    descriptor: CommandDescriptor<P, undefined>,
    handler: (message: CommandMessage<z.infer<P>>, context: ModuleHandlerContext<Dependencies>) => Promise<void> | void,
  ): CommandHandlerDefinition<P, undefined>
  commandHandler<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    handler: (
      message: CommandMessage<z.infer<P>>,
      context: ModuleHandlerContext<Dependencies>,
    ) => Promise<z.infer<R>> | z.infer<R>,
  ): CommandHandlerDefinition<P, R>
  commandHandler<P extends z.ZodType, R extends z.ZodType>(
    descriptor: CommandDescriptor<P, R>,
    options: {
      handler: (
        message: CommandMessage<z.infer<P>>,
        context: ModuleHandlerContext<Dependencies>,
      ) => Promise<z.infer<R>> | z.infer<R>
      appendCondition?: (message: CommandMessage<z.infer<P>>, sourcedCriteria: EventCriteria) => EventCriteria
    },
  ): CommandHandlerDefinition<P, R>
  /**
   * Declare an event handler whose context carries the module's dependencies. NOT
   * registered on the app — event handlers belong to a processor, so the
   * definition is returned for use in `processors(...)` / `.eventHandlers(...)`.
   */
  eventHandler<P extends z.ZodType>(
    descriptor: EventDescriptor<P>,
    handler: (
      message: SequencedEventMessage<z.infer<P>>,
      context: ModuleEventHandlerContext<Dependencies>,
    ) => Promise<void> | void,
  ): EventHandlerDefinition<P>
  /**
   * Override a framework slot FOR THIS MODULE ONLY. Slots left alone are
   * inherited from the app by identity, so overriding `eventStore` gives the
   * module its own store while it keeps sharing the root's command/query/event
   * buses — different persistence, one messaging fabric.
   *
   * The factory form receives the scope's components as they resolve, so an
   * override may build on the root's (or on an earlier override in the same
   * module).
   */
  set<K extends SlotName>(slot: K, factory: SlotFactory<K> | KronosComponents[K]): void
  /**
   * Register state modules in this module's scope. They are wired to a state
   * manager built over THIS module's event store, so a module's states are not
   * visible to another module's handlers.
   */
  states(...args: StatesArg[]): void
  /** Register query handlers in this module's scope. */
  queries(...handlers: QueryHandlerDefinition[]): void
  /** Register event processors in this module's scope (scoped event store + state manager). */
  processors(...modules: EventProcessorModule[]): void
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
export type Module<Dependencies> = ((dependencies: Dependencies) => Extension) & { readonly moduleName: string }

/**
 * Define a module: a named registration scope with typed, isolated
 * dependency injection.
 *
 * `Dependencies` is inferred from the setup callback's annotation — there is no type
 * argument to spell out. The name is optional; supply it only to label the
 * module in traces and error messages.
 *
 * ```ts
 * interface SupportContext { db: Db; storage: Storage }
 *
 * export const supportModule = defineModule((m: ModuleApi<SupportContext>) => {
 *   m.commandHandler(OpenTicket, async ({ payload }, ctx) => {
 *     await ctx.db.insert(messageBodies).values({ ... })   // ctx.db — the module's own
 *     ctx.append(TicketOpened, { ticketId: payload.ticketId })
 *   })
 * })
 * ```
 *
 * A module is also the ENCAPSULATION boundary for framework components: call
 * `m.set(slot, ...)` to scope a slot to this module. Anything not overridden is
 * inherited from the app by identity, so modules can own separate event stores
 * while sharing one messaging fabric:
 *
 * ```ts
 * const support = defineModule("support", (m: ModuleApi<SupportContext>) => {
 *   m.set("eventStore", supportStore)      // own persistence
 *   m.states(TicketExistence)              // wired to supportStore
 *   m.commandHandler(OpenTicket, async ({ payload }, ctx) => { ... })
 * })
 *
 * kronos()
 *   .use(postgres({ adapter }))            // root infra: buses + default store
 *   .use(support({ db, storage }))         // scoped store, SHARED buses
 *   .use(billing({ db: billingDb }))       // its own scope again
 * ```
 */
export function defineModule<Dependencies extends Record<string, unknown> = Record<never, never>>(
  setup: (m: ModuleApi<Dependencies>) => void,
): Module<Dependencies>
export function defineModule<Dependencies extends Record<string, unknown> = Record<never, never>>(
  name: string,
  setup: (m: ModuleApi<Dependencies>) => void,
): Module<Dependencies>
export function defineModule<Dependencies extends Record<string, unknown> = Record<never, never>>(
  nameOrSetup: string | ((m: ModuleApi<Dependencies>) => void),
  maybeSetup?: (m: ModuleApi<Dependencies>) => void,
): Module<Dependencies> {
  const name = typeof nameOrSetup === "string" ? nameOrSetup : ANONYMOUS_MODULE
  const setup = (typeof nameOrSetup === "string" ? maybeSetup : nameOrSetup) as (m: ModuleApi<Dependencies>) => void
  const configure = (dependencies: Dependencies): Extension => {
    for (const key of Object.keys(dependencies)) {
      if (RESERVED_CONTEXT_KEYS.has(key)) throw new ReservedContextKeyError(name, key)
    }
    return (app: App) => {
      const scope = createModuleScope(app, name)
      const frozenDependencies = Object.freeze({ ...dependencies })
      const commandContext = Object.freeze({ ...HANDLER_CONTEXT, ...frozenDependencies }) as ModuleHandlerContext<Dependencies>
      const eventContext = Object.freeze({ ...EVENT_HANDLER_CONTEXT, ...frozenDependencies }) as ModuleEventHandlerContext<Dependencies>

      const api: ModuleApi<Dependencies> = {
        name,
        dependencies: frozenDependencies,
        app,
        commandHandler(descriptor: CommandDescriptor<any, any>, handlerOrOptions: any) {
          const bare = typeof handlerOrOptions === "function" ? handlerOrOptions : handlerOrOptions.handler
          const definition = commandHandler(descriptor, {
            handler: (message: CommandMessage<any>) => bare(message, commandContext),
            ...(typeof handlerOrOptions === "object" && handlerOrOptions.appendCondition
              ? { appendCondition: handlerOrOptions.appendCondition }
              : {}),
          })
          scope.commandHandlers.push(definition)
          return definition
        },
        eventHandler(descriptor: EventDescriptor<any>, bare: any) {
          return eventHandler(descriptor, (message: SequencedEventMessage<any>) => bare(message, eventContext))
        },
        set(slot, factory) {
          const normalized: SlotFactory<SlotName> = (
            typeof factory === "function" ? factory : () => factory
          ) as SlotFactory<SlotName>
          scope.slotOverrides.push({ slot, factory: normalized })
        },
        states(...args) {
          for (const arg of args) {
            if (Array.isArray(arg)) {
              const [stateModule, options] = arg as readonly [StateModule, StateOptions]
              scope.stateEntries.push({ module: stateModule, options })
            } else {
              scope.stateEntries.push({ module: arg as StateModule, options: {} })
            }
          }
        },
        queries(...handlers) {
          scope.queryHandlers.push(...handlers)
        },
        processors(...modules) {
          scope.processors.push(...modules)
        },
      }
      setup(api)
    }
  }
  return Object.assign(configure, { moduleName: name })
}
