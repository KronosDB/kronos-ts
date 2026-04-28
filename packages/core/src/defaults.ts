import type { App } from "./app.js"
import type { EventBus } from "@kronos-ts/messaging"
import {
  createInMemoryEventStore,
  createInMemorySnapshotStore,
  descriptorBasedTagResolver,
} from "@kronos-ts/eventsourcing"
import {
  createSimpleCommandBus,
  createInterceptingCommandBus,
  createSimpleQueryBus,
  createInterceptingQueryBus,
  jsonSerializer,
  runInNewUoW,
} from "@kronos-ts/messaging"

/**
 * Register the 8 in-memory defaults (SLT-04, D-51).
 * Slots flagged inMemory:true emit a startup warning at .start() unless the slot
 * was overridden via .set/.forceSet (or another .setDefault won — but setDefault
 * is ifAbsent, so only the FIRST setDefault wins; user setDefault calls on already-defaulted
 * slots are no-ops, which is the intended SLT-02 semantics).
 */
export function registerInMemoryDefaults(app: App): void {
  // Durability-flagged defaults (SLT-04 warning fires unless overridden):
  app.setDefault("eventStore", () => createInMemoryEventStore(), {
    inMemory: true,
    warning: "[kronos] eventStore: in-memory — not durable, configure an extension for production",
  })
  app.setDefault("snapshotStore", () => createInMemorySnapshotStore(), {
    inMemory: true,
    warning: "[kronos] snapshotStore: in-memory — not durable, configure an extension for production",
  })
  app.setDefault("commandBus", () => createInterceptingCommandBus(createSimpleCommandBus()), {
    inMemory: true,
    warning: "[kronos] commandBus: in-memory — single-process only, configure an extension for distribution",
  })
  app.setDefault("queryBus", () => createInterceptingQueryBus(createSimpleQueryBus()), {
    inMemory: true,
    warning: "[kronos] queryBus: in-memory — single-process only, configure an extension for distribution",
  })
  // EventBus default: in ES setups EventStore IS the EventBus (configurer mirrors this at line 796)
  app.setDefault("eventBus", ({ eventStore }) => eventStore as unknown as EventBus, {
    inMemory: true,
    warning: "[kronos] eventBus: in-memory — single-process only",
  })

  // Stateless / non-durability-implied defaults — NO inMemory flag, no warning emission:
  app.setDefault("serializer", () => jsonSerializer())
  app.setDefault("unitOfWorkFactory", () => runInNewUoW)
  app.setDefault("tagResolver", () => descriptorBasedTagResolver())
}
