import type { StateModule } from "@kronos-ts/modelling"
import type {
  CommandHandlerDefinition,
  QueryHandlerDefinition,
  EventProcessorModule,
} from "@kronos-ts/messaging"
import type { App, StateOptions } from "./app.js"
import type { SlotName } from "./components.js"
import type { SlotFactory } from "./slot-registry.js"

// ---------------------------------------------------------------------------
// Module scopes — the encapsulation boundary.
//
// A module scope is a registration bucket that carries its OWN slot overrides.
// At `.start()` the app resolves each scope against the root components:
// everything the scope does not override is INHERITED BY IDENTITY (the same
// commandBus / queryBus / eventBus instance the root uses), and only the
// overridden slots are re-resolved. That is what lets one kronos instance host
// modules that write to different event stores while sharing one messaging
// fabric — the Fastify-encapsulation shape, applied to framework components
// rather than to decorators.
//
// Scopes hang off the App in a WeakMap rather than on the public App surface:
// they are framework-internal bookkeeping written by defineModule and read by
// AppImpl.start(), and nothing else should be able to reach them.
// ---------------------------------------------------------------------------

/** One module's encapsulated registrations plus its slot overrides. */
export interface ModuleScope {
  readonly name: string
  /**
   * Slot overrides in declaration order. Applied sequentially over the root
   * components, so a later override's factory observes earlier ones in the
   * same scope (e.g. a scoped snapshotStore may read the scoped eventStore).
   */
  readonly slotOverrides: Array<{ slot: SlotName; factory: SlotFactory<SlotName> }>
  /** State modules registered against THIS scope's event store. */
  readonly stateEntries: Array<{ module: StateModule; options: StateOptions }>
  readonly commandHandlers: CommandHandlerDefinition<any, any>[]
  readonly queryHandlers: QueryHandlerDefinition[]
  readonly processors: EventProcessorModule[]
}

const SCOPES = new WeakMap<App, ModuleScope[]>()

/** Create and attach a fresh scope for one module configuration. */
export function createModuleScope(app: App, name: string): ModuleScope {
  const scope: ModuleScope = {
    name,
    slotOverrides: [],
    stateEntries: [],
    commandHandlers: [],
    queryHandlers: [],
    processors: [],
  }
  const existing = SCOPES.get(app)
  if (existing) existing.push(scope)
  else SCOPES.set(app, [scope])
  return scope
}

/** All module scopes attached to an app, in registration order. */
export function moduleScopesOf(app: App): readonly ModuleScope[] {
  return SCOPES.get(app) ?? []
}
