# kronos-ts — SURFACE v2 (the frozen target)

The rules this surface is held to — every export must survive all of them:
- Core is only what cannot be a helper: task lifecycle, bus/store shapes,
  reference-following assembly, the DCB model. Everything else is a package
  of functions over public shapes that anybody could have written.
- Libraries export functions of ALL their real arguments; hosts write the
  arrows. No default-conjured delegates, no curried config-first factories.
- One function per seam; plurality is composed in function space by the host.
- Entries point at shared objects; records contain their own parts; the
  framework follows references and never counts. No module concept.
- Fields for order-free parameters of one algorithm; pipes only where order
  is real. A commuting pipeline stage is a lie.
- Parameter type expresses lifetime. Per-request data enters where the
  message is born (edge); per-handling via ctx; message-derivable via
  Intercept.
- One operation = a function type. A record of functions = shared state.
- No "factory" naming, no one-field return records, no sentinels in
  signatures, no variadic behavior parameters, no ALS, no registration.

## Package layout
```
packages/core          ← merge of common + messaging + eventsourcing + modelling + app
packages/test
packages/rabbitmq · kronosdb · axon-server            (transports)
packages/postgres · drizzle · knex · kysely · prisma · typeorm   (persistence)
packages/otlp          ← replaces opentelemetry; zero @opentelemetry deps
```
The `extensions/` directory is gone; the concept is gone.

## @kronos-ts/core

```ts
// primitives
qn(ns, name): QualifiedName · type Metadata · emptyMetadata() · type Tag · tag(k, v)
generateIdentifier(): string · type SerializedError
type Clock = () => number                      // an INSTANT, epoch ms — same unit as message.timestamp

// ── task lifecycle: THE primitive ──────────────────────────────────────────
unitOfWork(clock?: Clock): UnitOfWork          // message-agnostic (AF5): a task's processing
                                               // clock ABSENT = system time (the null behaviour)
interface UnitOfWork {
  execute<R>(action: (uow: UnitOfWork) => Promise<R>): Promise<R>
  on(phase, action) · onPrepareCommit · onCommit · onAfterCommit · onError · whenComplete
  now(): number                                // THE task's instant — every message it births stamps from here
  correlationData() · contributeCorrelationData(partial)
  readonly events: UoWEventBuffer · readonly stateCache: UoWStateCache
  readonly phase · readonly closed
}                                              // NO transaction surface — adapters own theirs
// ONE CLOCK PER TASK. ctx.append / ctx.send / ctx.query / ctx.schedule all stamp
// `timestamp` from uow.now(); scheduleAfter measures its delay from it too. A test
// that freezes the clock freezes every timestamp under the task, uniformly.

// ── messages & descriptors ─────────────────────────────────────────────────
type CommandMessage · QueryMessage · EventMessage
type Unstamped<M extends Message>              // M with `timestamp` optional — a message before its birth instant
stamped(message: Unstamped<M>, clock: Clock): M   // idempotent: an already-stamped message passes through
command({ name, payload, result? }) · query({ name, payload, result? })
event({ name, payload, tags?, tagKeys?, version? })
  // tags: { key: (p) => string }  — record of extractors; keys ARE the tag keys
  // tags: (p) => Tag[] needs explicit tagKeys when a state folds it
withNamespace(ns): { command, query, event }

// ── DCB queries: plain data, spec vocabulary ───────────────────────────────
type QueryItem  = { types?: ReadonlyArray<EventDescriptor | QualifiedName | string>; tags?: Record<string, string> }
type EventQuery = QueryItem | ReadonlyArray<QueryItem>      // items OR · tags ALL-of · types ANY-of

// ── buses: shapes + local segments ─────────────────────────────────────────
type CommandBus = { dispatch(m: Unstamped<CommandMessage>): Promise<unknown>; subscribe(name, handler): void }
type QueryBus   = { query(m: Unstamped<QueryMessage>, uow?): Promise<unknown>; subscribe(…); subscriptionQuery(…); emitUpdate(…) }
simpleCommandBus(unitOfWork: () => UnitOfWork): CommandBus
simpleQueryBus(unitOfWork: () => UnitOfWork): QueryBus
// EDGE DISPATCH IS WHERE THE INSTANT IS SETTLED: the verb builds the message with
// no timestamp, the bus mints the unit of work and stamps from uow.now(). A nested
// query is stamped by the task it joins. A transport, having no task, stamps from
// system time at the wire — and hands a locally-shortcut message on unstamped.

// ── interception: one seam, one function ───────────────────────────────────
type Intercept<M extends Message = Message> = (message: M) => M
interceptingCommandBus(bus, intercept: Intercept<CommandMessage>): CommandBus
interceptingQueryBus(bus, intercept: Intercept<QueryMessage>): QueryBus
lineage: Intercept        // correlationId ?? identifier · causationId ?? identifier (SEEDS ROOTS ONLY — ctx re-stamps causation per hop)

// ── edge verbs: build the message, dispatch it. Nothing named "gateway". ───
send(bus: CommandBus, descriptor, payload, metadata?): Promise<Result>
query(bus: QueryBus, descriptor, payload, metadata?): Promise<Result>
subscriptionQuery(bus: QueryBus, descriptor, payload, metadata?): SubscriptionQueryResult

// ── handlers: three contexts (the safety), one definition shape ────────────
commandHandler(descriptor, (message, ctx: HandlerContext) => result)
queryHandler(descriptor,  (message, ctx: QueryHandlerContext) => result)
eventHandler(descriptor,  (message, ctx: EventHandlerContext) => void)
interface EventHandlerContext { load · send · query · emitUpdate · schedule · scheduleAfter · cancelSchedule · unitOfWork }
interface HandlerContext extends EventHandlerContext { append }     // the atomic decide-append boundary
interface QueryHandlerContext { load · query · unitOfWork }         // read-only by construction
// ctx carries the handled message's metadata outward on send/query/append —
// uniformly, command leg and event leg alike. CorrelationDataProvider does not exist.

// ── event delivery: tracked only; the processor is a value ────────────────
type Sequence = (event: EventMessage) => string    // TOTAL — (e) => e.identifier = own lane = no constraint
sequentialPerTag(key: string): Sequence            // one-line transparent helper
eventProcessor({
  name: string,
  eventStore: EventStore, tokenStore: TokenStore, unitOfWork: () => UnitOfWork,   // constitutive
  sequence?: Sequence,                             // absent = global stream order (projection-safe)
  deadLetterQueue?: SequencedDeadLetterQueue,      // absent = propagate & retry
  batchSize?: number,
}): EventProcessor
  // construction errors: deadLetterQueue without sequence (parking is a lane operation)

// ── assembly: four lists. Nothing else. ────────────────────────────────────
kronos({
  commandHandlers?: ReadonlyArray<CommandHandlerDefinition & { commandBus; queryBus; eventStore; eventScheduler?; name? }>,
  queryHandlers?:   ReadonlyArray<QueryHandlerDefinition & { queryBus; name? }>,
  eventHandlers?:   ReadonlyArray<EventHandlerDefinition & { commandBus; queryBus; processor: EventProcessor; eventScheduler?; name? }>,
  states?:          ReadonlyArray<(StateModule | [StateModule, StateOptions]) & { eventStore; snapshotStore?; tagResolver?; name? }>,
}): App
// `eventScheduler` rides on the entry like the buses do: which scheduler an
// automation arms is a deployment fact. Absent and ctx.schedule throws — a
// scheduler is durable infrastructure and there is nothing honest to default to.
interface App { processors: ReadonlyMap<string, RunningProcessor>; stop(): Promise<void> }
// grouping: stores by OBJECT identity; processors by NAME (the durable identity —
// tokens persist by name across restarts). Same name + equal config ⇒ one delivery;
// same name + conflicting config ⇒ boot error.
// boot errors name the entry: missing eventStore, missing processor, processor field conflicts

// ── decision models ────────────────────────────────────────────────────────
state({ name?, id, initial, tags: (id) => TagRecord | readonly [TagRecord, ...], evolve: [[EventDescriptor, fold], ...], lifecycle? })
  // DCB query DERIVED per event type from tags × evolve; name required only when snapshotting

// ── store seams + in-memory implementations ────────────────────────────────
type EventStore · SnapshotStore
type TokenStore                       // methods take (processorName, …, uow?)
type SequencedDeadLetterQueue         // methods take (processingGroup, …, uow?)
type TagResolver = (event: EventMessage) => Tag[]
inMemoryEventStore() · inMemorySnapshotStore() · inMemoryTokenStore()
inMemoryEventScheduler({ eventSink, clock? })   // clock absent = system time
```
Core contains ZERO tracing vocabulary.

## @kronos-ts/rabbitmq — dumb pipe: client-side routing, configurable
```ts
rabbitMqConnection(url, { serviceName, instanceId, topology?, retry? }): Promise<RabbitMqConnection>  // start()/close()
rabbitMqCommandBus(rabbit, local: CommandBus, { preferLocal?, timeoutMs? }?): CommandBus
rabbitMqQueryBus(rabbit, local: QueryBus, { preferLocal?, timeoutMs? }?): QueryBus
```

## @kronos-ts/kronosdb — smart hub: server-side routing
```ts
kronosDbConnection({ host, port, componentName }): Promise<KronosDbConnection>   // one channel; start()/close()
kronosDbEventStore(kdb, context: string): EventStore
kronosDbSnapshotStore(kdb, context: string): SnapshotStore
kronosDbCommandBus(kdb, local: CommandBus): CommandBus      // inbound runs through YOUR local bus
kronosDbQueryBus(kdb, local: QueryBus): QueryBus
kronosDbControlPlane(kdb, processors): ControlPlane
```

## @kronos-ts/axon-server — same family as kronosdb
```ts
axonServerConnection(…) · axonServerEventStore(conn, ctx) · axonServerSnapshotStore(conn, ctx)
axonServerCommandBus(conn, local) · axonServerQueryBus(conn, local) · axonServerControlPlane(conn, processors)
```

## @kronos-ts/postgres — the FULL persistence family, no ORM required
```ts
postgresPool(connectionString | adapter): PostgresResource   // start()/close()
postgresEventStore(pg, { serializer, tagResolver }): EventStore
postgresSnapshotStore(pg): SnapshotStore
postgresUnitOfWork(pg, make: () => UnitOfWork): () => UnitOfWork      // lazy tx — its honest default
postgresTokenStore(pg): TokenStore                    // joins the SAME tx as your raw-sql writes
postgresDeadLetterQueue(pg): SequencedDeadLetterQueue
postgresTransaction(uow) · activePostgresTransaction(uow)
postgresHandler(handler, pg): handler                 // wraps the FUNCTION; ctx gains sql(): Sql | Tx
interface PostgresContext extends HandlerContext { sql(): Sql | Tx }
postgresEventScheduler(pg, …): EventScheduler
```
PRINCIPLE: persistence families are keyed by TRANSACTION IDENTITY — the token
store/DLQ must write through the same client handle the handlers write through.
Every persistence package (postgres, drizzle, knex, kysely, prisma, typeorm)
implements the same seven-function family for its client type. Never mix
families within one processor.

## @kronos-ts/drizzle (knex · kysely · prisma · typeorm: identical family)
```ts
drizzleTokenStore(db): TokenStore
drizzleDeadLetterQueue(db): SequencedDeadLetterQueue        // seam carries the group per call
drizzleUnitOfWork(db, make: () => UnitOfWork): () => UnitOfWork   // eager tx; delegate EXPLICIT
drizzleTransaction(uow): Promise<Tx>            // opens; REJECTS on a non-drizzle uow
activeDrizzleTransaction(uow): Tx | undefined   // observes, never opens
drizzleHandler(handler, db): handler            // ONE generic wrapper: ctx gains db()
interface DrizzleContext extends HandlerContext { db(): Db | Tx }    // + Event/Query variants
```
HANDLER WRAPPERS ARE FUNCTION-LEVEL — `(next, ...config) => (message, ctx) => result`,
with `<M, C, R>` inferred and no entry type anywhere. The host wraps by spreading
the entry itself, and anything a wrapper would have read off the entry it reads
off the MESSAGE instead:
```ts
const instrumented = <M extends Message, C extends DrizzleCapability & { unitOfWork: UnitOfWork }, R>(
  next: (m: M, c: C) => R,
) => drizzleHandler(otlpMetricsHandler(otlpHandler(next, exporter), exporter), db)

kronos({
  commandHandlers: slices.flatMap((s) => s.commandHandlers)
    .map((h) => ({ ...h, handler: instrumented(h.handler) }))
    .map((h) => ({ ...h, commandBus, queryBus, eventStore })),
})
```
The NAMES are unchanged — `<pkg>Handler` still, because a shared-package export
must carry its provenance. What changed is the LEVEL: the argument is the handler
function, not the entry. The erasure is DIRECTIONAL — `db()` in, base ctx out —
so a wrapper ordered wrong is a compile error. Wrappers that supply nothing
(tracing, metering) erase nothing and compose on either side. The per-kind names
(`drizzleCommandHandler` ×3), the entry-constraint types (`DrizzleHandlerEntry`,
`WithDrizzleSupplied`, `PostgresHandlerDefinition`, `Supplied`, `OtlpHandlerEntry`,
and the knex · kysely · prisma · typeorm equivalents) are gone, and no wrapper
reads the entry's `name` field any more — that field stays what it always was, a
diagnostics label kronos groups by.

## @kronos-ts/otlp — the protocol, not the ecosystem
```ts
otlpExporter({ endpoint, serviceName, flushIntervalMs? }): OtlpExporter   // resource: batches, close()
otlpCommandBus(bus, exporter): CommandBus       // span per dispatch; W3C traceparent into metadata
otlpQueryBus(bus, exporter): QueryBus
otlpHandler(handler, exporter, label?): handler        // parents (command/query msgs) / links (event msgs)
otlpMetricsHandler(handler, exporter, label?): handler // duration / throughput / failure counters
  // label?: (message: Message) => string — ABSENT = the message's qualified name.
  // Parent-vs-link and SERVER-vs-CONSUMER come from `message.kind`; there is no
  // kind argument and no entry to ask, so both wrappers are pre-appliable.
```
Zero `@opentelemetry/*` dependencies. No SDK, no global tracer, no patching.
otel-js interop = the consumer writes wrappers over the same public shapes.

## @kronos-ts/test — a test is a VALUE, run at a SITE
```ts
// ── the vocabulary: one constructor per message kind, doubling as the expectation
event(descriptor, payload, metadata?): EventValue        // a fact, an arrival, or an expectation
command(descriptor, payload, metadata?): CommandValue    // the act, or an expected dispatch
query(descriptor, payload, metadata?): QueryValue        // the act (a read is never a consequence)
  // payloads are compile-checked against the descriptor; `any()` widens, never disables
any(schema?: z.ZodType): Any                             // the positional payload hole — renders as `*`
type Duration = number                                   // ms, the unit Clock reads

// ── assertion values for `then` (each CATEGORY judged only if mentioned)
result(value)                     // command → handler return · query → the answer · event act = fixture error
error(matcher: string | RegExp | ((e: unknown) => boolean))    // thrown only; asserting it DECLINES the throw
noEvents() · noCommands()         // the empty case of the list, said out loud
scheduled(event(...), after: Duration) · cancelled(event(...))

// ── the grammar: order is real, so the joints are a pipe
given(...events): ScenarioStart          // free-standing entry; given() empty ≡ scenario()
scenario(): ScenarioStart
  .wait(duration)  → same shape          // chainable at any joint, repeatable
  .when(action)    → ScenarioActed       // EXACTLY ONE command | query | event (singular)
  .wait(duration)  → ScenarioActed
  .then(...assertions) → Scenario        // terminal
interface Scenario { steps · then · description }   // "given X, wait 1000ms, when Y, then Z"
  // pure and immutable: each step returns a NEW value, so one prefix finishes many ways,
  // and one finished Scenario runs against as many fixtures as you like

// ── the site
testFixture(
  scope: (eventStore: EventStore, snapshotStore: SnapshotStore) => lists,
  opts?: { within?: Duration; clock?: Clock; realTime?: boolean },
): { run(scenario, opts?): Promise<{ result, events, commands }> }
FIXTURE_EPOCH                     // where the clock starts when nobody says otherwise
type PartialProcessor = (eventStore, tokenStore, unitOfWork, deadLetterQueue) => EventProcessor
ScenarioAssertionError            // message IS the diff
```
The fixture CREATES the resources and hands them to the scope, which is a
function of them — the same function a process deploys. It owns
`recordingEventStore(inMemoryEventStore())`, `inMemorySnapshotStore()`,
`inMemoryTokenStore()`, `inMemoryDeadLetterQueue()`,
`recordingCommandBus(simpleCommandBus(uow))`, `recordingQueryBus(simpleQueryBus(uow))`
and a `controllableScheduler(clock)` sharing the fixture clock, calls the scope
FULL-HANDED (a shorter parameter list declines the rest), completes any
`PartialProcessor` on an event-handler entry with its own resources, and wires the
result kronos-style.

Semantics, pinned:
- `given` events APPEND straight to the log; every processor FAST-FORWARDS its
  cursor past them WITHOUT invoking a handler. History is history — the
  automations that would have fired already fired, long ago.
- `when(command | query)` dispatches through the real bus. `when(event)` means the
  event ARRIVES: appended, and the processors DO react. That is the automation
  shape.
- After the act and after each `wait`: QUIESCE — drain dispatched commands, drive
  every processor to the head of the log IT reads, settle, repeat until nothing
  moves. Deterministic; no sleeps.
- `wait(d)` jumps the fixture clock, fires the schedules now due (in FIRE-TIME
  order, each event stamped with the instant it fires), then quiesces. If the
  scope brought resources the fixture does not own, `wait` throws a clear error
  unless `opts.realTime`, where it genuinely elapses (`setTimeout d`).
- `then`: `event()`/`noEvents()` ⇒ the EXACT ORDERED list of new events, nothing
  missing or extra; `command()`/`noCommands()` the same over dispatches (the act's
  own command excluded). `result()` is deep-strict with `any()` holes. `error()`
  matches the throw. `metadata` on a then-value is a SUBSET claim over the keys it
  names. Against a real-infrastructure scope the claims are re-judged until
  `opts.within` (default 5s); an all-fixture scope is judged once, because
  determinism makes waiting theatre.
- A fixture instance is ONE timeline: consecutive `run` calls continue it (sagas),
  and each reports only what it caused.

```ts
// recorders: thing-first decorators, same shape + a readable log + reset()
recordingEventStore(store): EventStore & { appended; reset() }
recordingCommandBus(bus):   CommandBus & { dispatched; reset() }
recordingQueryBus(bus):     QueryBus   & { queried;    reset() }
controllableScheduler(clock): EventScheduler & { schedules; due(); reset() }   // no timer, no sink
```
Unit level needs no fixture at all: folds are reduces over evolve tuples;
handlers are functions called with an inline ctx record.

## Deleted to reach this surface (from the current worktree)
`kronosDbContext` · `distributedCommandBus`/`distributedQueryBus` + connector
interfaces + `SubscriptionRegistry` · `correlatingCommandBus`/`correlatingQueryBus`
+ `MetadataProvider` + `CorrelationDataProvider` · variadic interceptor params ·
`amqpConnection` name · the `postgres()` bundle · `SpanFactory`/`MetricsRecorder`
seams + `tracingHandler`/`meteringHandler` in core · `@kronos-ts/opentelemetry`
package · `trackingProcessor`/`subscribingProcessor` builders + `SequencingPolicy`
objects · processors as a kronos list · on-commit delivery + any eventBus ·
`drizzleCommandHandler`×3 (one generic) · the ENTRY level of the handler wrappers
(same names, now taking `h.handler`) and the entry-constraint types they needed
(`DrizzleHandlerEntry`, `WithDrizzleSupplied`, `PostgresHandlerDefinition`, `Supplied`,
`KnexHandlerEntry`, `WithKnexSupplied`, `KyselyHandlerEntry`, `WithKyselySupplied`,
`PrismaHandlerEntry`, `WithPrismaSupplied`, `TypeormHandlerEntry`, `WithTypeormSupplied`,
`OtlpHandlerEntry`) · DLQ constructor table/group params ·
`AnyTagCriteria` · the five-package core split · the `extensions/` directory ·
the test package's triple-record fixture (`run({ given, when, then })`, `Slice`,
`EventFact`, `CommandAct`, `ErrorExpectation`, `ThenRecord`) and its
zero-argument `recordingEventStore()`.


## Consumer idiom (karma's — NOT the framework; documented as the reference style)
- `slice({...})`: host constructor normalizing the four arrays; slices are plain
  exported VALUES. Event handler entries carry `processor` as a PARTIALLY APPLIED
  function — the slice closes out its semantics (name, sequence, DLQ-or-not as its
  parameter list), the module calls it full-handed; shorter parameter lists decline
  trailing arguments by TS assignability.
- `module(eventStore, snapshotStore)`: creates its db, names its five resources as
  consts, contributes them via plain flatMap/map chains.
- Slice FOLDER convention: wire edges live beside domain code, not in it —
  `slice/controller.ts` (oRPC today; pubsub or anything later), `slice/slice.ts`
  (state → handlers, top-down), `slice/index.ts` ties them. Wire stuff stays at
  the edge; the slice never imports a transport.
- The edge stamps per-request metadata on BOTH verbs: send AND query carry actor.
