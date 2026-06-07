import type { App } from "./app.js"
import type { EventBus } from "@kronos-ts/messaging"
import {
  createInMemoryEventStore,
  createInMemorySnapshotStore,
  descriptorBasedTagResolver,
} from "@kronos-ts/eventsourcing"
import {
  createSimpleCommandBus,
  createSimpleQueryBus,
  jsonSerializer,
  runInNewUoW,
  createInMemoryTokenStore,
  noTransactionManager,
  createInMemoryEventScheduler,
  type InMemoryEventScheduler,
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
  // Run handlers through the resolved unitOfWorkFactory so a transactional
  // backend (e.g. postgres) gives each command's UoW a transaction. With the
  // in-memory default factory (runInNewUoW) this is identical to before.
  app.setDefault("commandBus", ({ unitOfWorkFactory }) => createSimpleCommandBus(unitOfWorkFactory), {
    inMemory: true,
    warning: "[kronos] commandBus: in-memory — single-process only, configure an extension for distribution",
  })
  app.setDefault("queryBus", () => createSimpleQueryBus(), {
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

  // Plan 09-01 (D-84): typed slots for token persistence + transactional wrapping.
  // Both default to in-memory — extensions (KronosDB, Drizzle/Knex/Kysely token stores,
  // user-supplied TransactionManagers) override via app.set('tokenStore', ...) etc.
  app.setDefault("tokenStore", () => createInMemoryTokenStore(), {
    inMemory: true,
    warning: "[kronos] tokenStore: in-memory — not durable, configure a persistence extension for production",
  })
  app.setDefault("transactionManager", () => noTransactionManager(), {
    inMemory: true,
    warning: "[kronos] transactionManager: in-memory — pass-through, configure a transactional extension for production",
  })

  // In-memory EventScheduler: setTimeout-backed, fires into the resolved
  // eventBus. Closure captures the instance so onStop can clear armed timers —
  // without this cleanup, scheduled-but-unfired events keep the process alive
  // past app.stop().
  let inMemoryScheduler: InMemoryEventScheduler | undefined
  app.setDefault(
    "eventScheduler",
    ({ eventBus }) => {
      inMemoryScheduler = createInMemoryEventScheduler({ eventSink: eventBus })
      return inMemoryScheduler
    },
    {
      inMemory: true,
      warning:
        "[kronos] eventScheduler: in-memory — not durable, configure a persistence extension for production",
    },
  )
  app.onStop("connect", async () => {
    if (inMemoryScheduler) await inMemoryScheduler.stop()
  })
}
