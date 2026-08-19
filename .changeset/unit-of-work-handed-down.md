---
"@kronos-ts/core": minor
"@kronos-ts/test": minor
"@kronos-ts/axon-server": minor
"@kronos-ts/kronosdb": minor
"@kronos-ts/rabbitmq": minor
"@kronos-ts/postgres": minor
"@kronos-ts/drizzle": minor
"@kronos-ts/knex": minor
"@kronos-ts/kysely": minor
"@kronos-ts/prisma": minor
"@kronos-ts/typeorm": minor
---

The unit of work is handed down as a parameter. AsyncLocalStorage is deleted.

**`UoWRunner` hands the unit of work to the action.** Per-message state used to
live in an `AsyncLocalStorage` holding a `Map<symbol, unknown>`; every capability
a handler used reached into it at call time. There is now an explicit
`UnitOfWork` handle, and it travels as an argument.

```ts
// before — the action took nothing; state was ambient
export type UoWRunner = <R>(metadata: Metadata | undefined, action: () => Promise<R>) => Promise<R>

// after — the action is given the unit of work
export type UoWRunner = <R>(
  metadata: Metadata | undefined,
  action: (uow: UnitOfWork) => Promise<R>,
) => Promise<R>
```

`UnitOfWork` carries as REAL TYPED FIELDS what the resource-key map held loosely:
`metadata`, `phase`, `closed`, the lifecycle registrations (`on` /
`onPrepareCommit` / `onCommit` / `onAfterCommit` / `onError` / `whenComplete`),
`correlationData()` / `contributeCorrelationData()`, the append buffer and
sourcing infos (`uow.events`), the per-UoW state cache (`uow.stateCache`),
`replaying`, and the adapter transaction (`transaction()` /
`activeTransaction()` / `setTransaction()` / `setTransactionOpener()`).

The phase model is UNCHANGED — `PRE_INVOCATION → INVOCATION → POST_INVOCATION →
PREPARE_COMMIT → COMMIT → AFTER_COMMIT`, same numeric values, same
late-registration draining (an action registered while its own phase is running
still runs in that phase; earlier phases are already past).

**Deleted.** `processing-state.ts` and the whole resource-key system:
`resourceKey` / `ResourceKey`, `getResource` / `setResource` /
`computeIfAbsent` / `removeResource` / `hasResource` / `updateResource`,
`withOverride`, `processingStateStorage`, `initialProcessingState`,
`requireInvocationPhase`, and every `*_KEY` (`COMMAND_BUS_KEY`, `QUERY_BUS_KEY`,
`TRANSACTION_KEY`, `CORRELATION_DATA_KEY`, `BUFFERED_EVENTS_KEY`,
`SOURCING_INFOS_KEY`, `STATE_MANAGER_KEY`, `STATE_CACHE_KEY`,
`STATE_MODULES_KEY`, `EVENT_SCHEDULER_KEY`, `REPLAY_STATE_KEY`,
`EVENT_FLUSH_REGISTERED_KEY`, `MARKER_RESOURCE_KEY`, `TAG_RESOURCE_KEY`). Also
gone: `getActiveTransaction` / `getOrBeginActiveTransaction`,
`activeCorrelationData`, the module-level `contributeCorrelationData`, the
no-arg `isReplay()`, the frozen `HANDLER_CONTEXT` / `EVENT_HANDLER_CONTEXT` /
`QUERY_HANDLER_CONTEXT` singletons, and the `MinimalConfiguration` config-shim.

`NoActiveUnitOfWork` and `WrongUoWPhase` REMAIN. `closed` replaces the
ALS-absence check: a ctx used after its unit of work committed throws
`NoActiveUnitOfWork`; a mutator called outside INVOCATION throws
`WrongUoWPhase`.

**Handler contexts are built fresh per invocation.** They were three frozen
shared singletons that only worked because every capability re-resolved through
ALS. Each is now a closure over that invocation's unit of work, the buses the
caller already holds, and the item's stores.

```ts
// before — one frozen object, every capability an ambient lookup
export const HANDLER_CONTEXT: HandlerContext = Object.freeze({ load, append, send, … })
handler(message, HANDLER_CONTEXT)

// after — a closure over this invocation's unit of work
handler(message, handlerContext({ uow, stateManager, commandBus, queryBus, eventScheduler }))
```

`handlerContext` / `eventHandlerContext` / `queryHandlerContext` are exported.
Contexts gain `unitOfWork`, `contributeCorrelationData`, and — on the event and
command contexts — `isReplay()`, which replaces the deleted module-level
`isReplay()`.

**Correlation lineage rides on the message.** `correlatingCommandBus` /
`correlatingQueryBus` no longer read ambient correlation data; they apply their
`MetadataProvider`s and nothing else. `ctx.send` / `ctx.query` stamp the unit of
work's lineage onto the outgoing message BEFORE any bus sees it, so the local
and the remote branch carry identical metadata. End-to-end lineage behaviour is
unchanged. `applyCorrelationData(uow, message, providers)` takes the unit of
work first.

**`TokenStore` and `SequencedDeadLetterQueue` take the unit of work as a
trailing parameter**, on every method. It is OPTIONAL, because the lifecycle and
admin paths (`initializeSegments`, the startup `get`, `resetTokens`, `clear`,
`sequenceIdentifiers`) legitimately run outside any unit of work — exactly where
the old permissive `getActiveTransaction()` returned `undefined`.

```ts
// before
store(processorName: string, segment: number, token: TrackingToken): Promise<void>
enqueue(letter: DeadLetter): Promise<void>

// after
store(processorName: string, segment: number, token: TrackingToken, uow?: UnitOfWork): Promise<void>
enqueue(letter: DeadLetter, uow?: UnitOfWork): Promise<void>
```

Every implementation follows suit — the in-memory ones plus `drizzle`, `knex`,
`kysely`, `postgres`, `prisma` and `typeorm`. Their writer helper changes shape
and nothing else:

```ts
// before
function getDb() { return getActiveTransaction<DrizzleTransaction>() ?? config.db }

// after
function getDb(uow?: UnitOfWork) { return uow?.activeTransaction<DrizzleTransaction>() ?? config.db }
```

`EventScheduler.schedule/cancel`, `EventSink.publish` and
`EventStorageEngine.append/appendEvents` gain the same trailing `uow?`, so a
scheduler insert, an event append and a token write inside one unit of work land
in one adapter transaction.

**Bus signatures.** `CommandBus.subscribe` and `QueryBus.subscribe` hand the unit
of work to the handler: `(message, uow) => Promise<unknown>`. `QueryBus.query`
takes a trailing `uow?` — passing one NESTS the read in that unit of work, which
is how `ctx.query` shares the handler's transaction. `QueryBus.emitUpdate` /
`completeSubscription` / `completeSubscriptionExceptionally` take a trailing
`uow?` so updates defer to its AFTER_COMMIT. `CommandBus.dispatch` deliberately
does NOT take one: every command is its own fresh unit of work.

`runInNewUoW` (always fresh) and `runInUoW` (reuse if given) stay distinct;
`runInUoW(uow, metadata, action, runner?)` now expresses "is one active" as
"was one passed in".
