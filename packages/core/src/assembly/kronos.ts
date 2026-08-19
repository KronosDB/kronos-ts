import { qualifiedNameToString } from "../primitives/qualified-name.js"
import { eventSourcedRepository } from "../state/event-sourced-repository.js"
import { inMemorySnapshotStore, type SnapshotStore } from "../stores/snapshot-store.js"
import { descriptorBasedTagResolver, type TagResolver } from "../stores/tag-resolver.js"
import type { EventStore } from "../stores/event-store.js"
import type { SnapshotPolicy } from "../state/snapshot-policy.js"
import type { CommandBus } from "../buses/command-bus.js"
import type { QueryBus } from "../buses/query-bus.js"
import type { CommandHandlerDefinition } from "../handlers/command-handler.js"
import type { QueryHandlerDefinition } from "../handlers/query-handler.js"
import type { EventHandlerDefinition } from "../handlers/event-handler.js"
import { subscribeCommandHandlers } from "../handlers/command-handling-module.js"
import { subscribeQueryHandlers } from "../handlers/query-handling-module.js"
import type { EventProcessor, RunningProcessor } from "../processor/event-processor.js"
import type { EventScheduler } from "../processor/event-scheduler.js"
import { runEventProcessor, type ProcessorHandlerEntry } from "../processor/running-processor.js"
import { stateManager, type StateManager } from "../state/state-manager.js"
import type { StateModule } from "../state/state.js"

// ---------------------------------------------------------------------------
// An app is FOUR LISTS. That is the whole surface.
//
// There is no module, no container, no registry, no lifecycle framework and no
// slot resolution. An entry says what it handles (`descriptor`) and names the
// shared things it runs against — its buses, its log, its processor — as bare
// properties. `kronos` follows those references and wires the result. It never
// counts: two entries naming the same object are one group because it is the
// same object, not because anybody declared a group.
// ---------------------------------------------------------------------------

/**
 * What the host attaches to an entry at composition — bare properties, each
 * named for the thing it is.
 *
 * Attached by the HOST, not by the handler's author, because which log a slice
 * lives in is a deployment decision and the slice should not care:
 *
 * ```ts
 * const eventStore = postgresEventStore(pool)
 * kronos({
 *   commandHandlers: billing.map((h) => ({ ...h, eventStore, commandBus, queryBus })),
 * })
 * ```
 *
 * There is no `stores: { ... }` record. A record is only worth its name when
 * something is behind it — one connection, one transaction, one lifecycle — and
 * nothing was: it was four unrelated objects in a bag, so the bag is gone and
 * each rides under its own name.
 *
 * Entries sharing one `eventStore` OBJECT share one repository set and one
 * stream — identity is the grouping key, so the spread above costs nothing per
 * entry.
 */
export interface HandlerSite {
  /** The log this entry's state is sourced from and appended to. */
  readonly eventStore?: EventStore
  /** Snapshot cache. Omit and one is made per log. */
  readonly snapshotStore?: SnapshotStore
  /** Tag resolution at append time. Defaults to descriptor-derived tags. */
  readonly tagResolver?: TagResolver
  /**
   * Backs `ctx.schedule` / `ctx.scheduleAfter` / `ctx.cancelSchedule`. Absent and
   * those capabilities throw — a scheduler is durable infrastructure with a
   * worker behind it, so there is nothing honest to default it to.
   */
  readonly eventScheduler?: EventScheduler
  /** Diagnostics label — used in boot errors. */
  readonly name?: string
}

/** An entry plus whatever the host attached to it. */
export type Sited<T> = T & HandlerSite

/**
 * Per-state persistence options. Everything here is about how ONE state's
 * repository is built: two states on the same log legitimately want different
 * snapshot policies, because they have different event volumes.
 *
 * Omit it and the state runs on its log's snapshot store with no policy — i.e.
 * snapshots are never written, which is the safe default.
 */
export interface StateOptions {
  /** When to write a snapshot for this state. Default: never. */
  readonly snapshotPolicy?: SnapshotPolicy
  /** A snapshot store for THIS state only. Defaults to the log's. */
  readonly snapshotStore?: SnapshotStore
}

/**
 * A state, optionally with the options its repository is built from.
 *
 * ```ts
 * kronos({ states: [
 *   [{ ...Course, eventStore }, { snapshotPolicy: afterEvents(3) }],  // state + options
 *   { ...Student, eventStore },                                       // bare state
 * ]})
 * ```
 */
export type StateEntry<Id = any, S = any> =
  | Sited<StateModule<Id, S>>
  | readonly [state: Sited<StateModule<Id, S>>, options: StateOptions]

/**
 * A command handler `kronos` subscribes onto its command bus.
 *
 * Widened to `<any, any>` on purpose: these are function-carrying objects, so a
 * narrower message type is checked contravariantly and a CONCRETELY typed
 * handler would not be assignable to the defaults.
 */
export type CommandHandlerEntry = Sited<CommandHandlerDefinition<any, any>> & {
  /** The bus this handler is subscribed on, and the one `ctx.send` dispatches through. */
  readonly commandBus: CommandBus
  /** The bus `ctx.query` and `ctx.emitUpdate` reach. */
  readonly queryBus: QueryBus
}

/** A query handler `kronos` subscribes onto its query bus. */
export type QueryHandlerEntry = Sited<QueryHandlerDefinition<any, any>> & {
  /** The bus this handler is subscribed on, and the one `ctx.query` reaches. */
  readonly queryBus: QueryBus
}

/**
 * An event handler, and the processor that delivers to it.
 *
 * `processor` is REQUIRED and it is a VALUE — the same object (or an equal one
 * under the same name) shared by every handler in that delivery. Handlers
 * naming one processor share one cursor, one batch, one unit of work; handlers
 * naming different processors are independent deliveries that happen to read
 * the same log.
 */
export type EventHandlerEntry = Sited<EventHandlerDefinition<any, any>> & {
  /** Backs `ctx.send`. */
  readonly commandBus: CommandBus
  /** Backs `ctx.query` and `ctx.emitUpdate`. */
  readonly queryBus: QueryBus
  /** The delivery this handler belongs to. */
  readonly processor: EventProcessor
}

/** A state paired with the options its repository is built from. */
interface PartitionedState {
  readonly state: Sited<StateModule<any, any>>
  readonly options: StateOptions
}

/** Everything that names ONE event store, resolved. */
interface LogGroup {
  readonly eventStore: EventStore
  readonly snapshotStore: SnapshotStore
  readonly tagResolver: TagResolver
  readonly states: PartitionedState[]
  readonly commands: CommandHandlerEntry[]
  readonly queries: QueryHandlerEntry[]
}

/** One delivery: a processor value plus every handler that named it. */
interface ProcessorGroup {
  processor: EventProcessor
  /** How the first entry naming this processor identifies itself, for errors. */
  readonly firstLabel: string
  readonly handlers: ProcessorHandlerEntry[]
}

/** How an entry names itself when a boot error has to point at it. */
function labelOf(entry: { name?: string; kind?: string; descriptor?: any }): string {
  if (entry.name) return entry.name
  const descriptorName = entry.descriptor?.name
  if (descriptorName) {
    return typeof descriptorName === "string"
      ? descriptorName
      : `${descriptorName.namespace ?? ""}.${descriptorName.name ?? descriptorName.localName ?? ""}`
  }
  return entry.kind ?? "<unnamed entry>"
}

/**
 * An entry that cannot work without a log, and was not given one.
 *
 * Named rather than counted: "some handler is missing an event store" sends you
 * reading the whole list, and the whole point of a flat list is that each entry
 * stands on its own.
 */
function requireEventStore(entry: Sited<{ kind?: string; descriptor?: any }>, why: string): EventStore {
  if (!entry.eventStore) {
    throw new Error(
      `kronos: entry "${labelOf(entry)}" ${why}, but no event store was attached. ` +
        `Attach one at the composition root, e.g. \`{ ...handler, eventStore }\`.`,
    )
  }
  return entry.eventStore
}

/**
 * A state's `name` is DURABLE SNAPSHOT IDENTITY and nothing else — the key its
 * snapshots are written under. Everything else the framework needs is keyed on
 * the definition's `identity`, which `state()` assigns. So `name` is optional
 * right up until the moment something would actually write a snapshot, and this
 * is that moment: the state was handed a snapshot policy or a snapshot store of
 * its own.
 *
 * The error has to name a state that, by construction, has no name — so it says
 * where in `states` it sits and which events it folds, which is enough to find
 * it in a file.
 */
function requireSnapshotName(
  state: Sited<StateModule<any, any>>,
  options: StateOptions,
  index: number,
): void {
  if (state.name !== undefined) return
  if (!options.snapshotPolicy && !options.snapshotStore) return
  const folded = state.evolvers
    .map(([descriptor]) => qualifiedNameToString(descriptor.name))
    .join(", ")
  const configured = [
    options.snapshotPolicy ? "a snapshot policy" : undefined,
    options.snapshotStore ? "a snapshot store" : undefined,
  ]
    .filter(Boolean)
    .join(" and ")
  throw new Error(
    `kronos: the state at index ${index} of \`states\` (folds ${folded || "no events"}) ` +
      `is configured with ${configured}, but has no \`name\`. Snapshots are keyed on the ` +
      `state's durable name — add \`name: "..."\` to its \`state({ ... })\` definition, or ` +
      `drop the snapshot configuration.`,
  )
}

/**
 * Which fields of two processor values disagree.
 *
 * Objects and functions compare by IDENTITY, because that is what they mean
 * here: two token stores are the same cursor table only if they are the same
 * object, and two `sequence` functions lane identically only if they are the
 * same function. `batchSize` compares by value because a number IS its value.
 */
function processorConflicts(a: EventProcessor, b: EventProcessor): string[] {
  const conflicts: string[] = []
  if (a.eventStore !== b.eventStore) conflicts.push("eventStore")
  if (a.tokenStore !== b.tokenStore) conflicts.push("tokenStore")
  if (a.unitOfWork !== b.unitOfWork) conflicts.push("unitOfWork")
  if (a.sequence !== b.sequence) conflicts.push("sequence")
  if (a.deadLetterQueue !== b.deadLetterQueue) conflicts.push("deadLetterQueue")
  if ((a.batchSize ?? 1) !== (b.batchSize ?? 1)) conflicts.push("batchSize")
  return conflicts
}

export interface App {
  /**
   * The LIVE processors, keyed by name — the durable identity they were
   * grouped under. Distributed control planes (Axon Server, KronosDB) need
   * these to honour pause/start/split/merge and to report status; tests need
   * them to await a projection catching up.
   */
  readonly processors: ReadonlyMap<string, RunningProcessor>
  stop(): Promise<void>
}

/**
 * Wire an app.
 *
 * Four lists. Nothing else — no buses at this level, no stores, no unit of
 * work, no enhancer, no modules. Every shared thing rides on the entries that
 * use it, under its own name, and `kronos` groups by the OBJECT (stores) or by
 * the DURABLE NAME (processors, whose tokens outlive the process).
 *
 * ```ts
 * const eventStore = inMemoryEventStore()
 * const commandBus = interceptingCommandBus(simpleCommandBus(unitOfWork), lineage)
 * const queryBus = interceptingQueryBus(simpleQueryBus(unitOfWork), lineage)
 * const projection = eventProcessor({ name: "courses", eventStore, tokenStore, unitOfWork })
 *
 * const app = kronos({
 *   states: [{ ...Course, eventStore }],
 *   commandHandlers: billing.map((h) => ({ ...h, eventStore, commandBus, queryBus })),
 *   queryHandlers: reads.map((h) => ({ ...h, queryBus })),
 *   eventHandlers: views.map((h) => ({ ...h, commandBus, queryBus, processor: projection })),
 * })
 * ```
 */
export function kronos(opts: {
  /** Handlers subscribed onto their own command bus. */
  commandHandlers?: ReadonlyArray<CommandHandlerEntry>
  /** Handlers subscribed onto their own query bus. */
  queryHandlers?: ReadonlyArray<QueryHandlerEntry>
  /** Handlers delivered to by their own processor. */
  eventHandlers?: ReadonlyArray<EventHandlerEntry>
  /** Event-sourced states, each becoming a repository on its own store. */
  states?: ReadonlyArray<StateEntry>
}): App {
  const groups = new Map<EventStore, LogGroup>()
  /** Query handlers with no log — legitimate: a read model need not be event-sourced. */
  const storelessQueries: QueryHandlerEntry[] = []

  /**
   * Find or create the group for an entry, keyed on its event store OBJECT.
   *
   * The leaf caches come from the FIRST entry seen for a log, because a snapshot
   * store is a cache OF that log and a tag resolver is a property of what gets
   * written to it — sharing the log means sharing both.
   */
  function groupFor(entry: HandlerSite, eventStore: EventStore): LogGroup {
    const existing = groups.get(eventStore)
    if (existing) return existing
    const created: LogGroup = {
      eventStore,
      snapshotStore: entry.snapshotStore ?? inMemorySnapshotStore(),
      tagResolver: entry.tagResolver ?? descriptorBasedTagResolver(),
      states: [],
      commands: [],
      queries: [],
    }
    groups.set(eventStore, created)
    return created
  }

  // ---- Sort into log groups ----------------------------------------------
  // Plainly-typed loops. There is no union to narrow, no `kind` read and no
  // array-vs-record predicate: the caller already said which kind of thing each
  // one is by choosing the field, so nothing here has to guess.
  let stateIndex = 0
  for (const entry of opts.states ?? []) {
    const [state, options] = Array.isArray(entry)
      ? (entry as readonly [Sited<StateModule<any, any>>, StateOptions])
      : ([entry as Sited<StateModule<any, any>>, {}] as const)
    requireSnapshotName(state, options ?? {}, stateIndex++)
    const eventStore = requireEventStore(state, "is a state and must be sourced from a log")
    groupFor(state, eventStore).states.push({ state, options: options ?? {} })
  }

  for (const handler of opts.commandHandlers ?? []) {
    // Command handlers always need a log: the handler's UnitOfWork flushes its
    // appended events at PREPARE_COMMIT, and there is nowhere to flush to.
    const eventStore = requireEventStore(handler, "is a command handler and appends to a log")
    groupFor(handler, eventStore).commands.push(handler)
  }

  for (const handler of opts.queryHandlers ?? []) {
    // A query handler is the only one that legitimately has no log — a read
    // model served from a projection table needs no event store to answer.
    if (handler.eventStore) groupFor(handler, handler.eventStore).queries.push(handler)
    else storelessQueries.push(handler)
  }

  // ---- Group event handlers by PROCESSOR NAME ----------------------------
  // Not by object identity: a token persists under the processor's NAME, so two
  // entries naming "courses" ARE one delivery even when a module built its own
  // equal value. Equal configs merge; conflicting ones are a boot error naming
  // both entries, because the alternative is two processors fighting over one
  // cursor row.
  /** What one event-handler entry contributes to its delivery. */
  function processorEntry(handler: EventHandlerEntry): ProcessorHandlerEntry {
    return {
      definition: handler,
      commandBus: handler.commandBus,
      queryBus: handler.queryBus,
      ...(handler.eventScheduler !== undefined
        ? { eventScheduler: handler.eventScheduler }
        : {}),
    }
  }

  const processorGroups = new Map<string, ProcessorGroup>()
  for (const handler of opts.eventHandlers ?? []) {
    const label = labelOf(handler)
    const processor = handler.processor
    if (!processor) {
      throw new Error(
        `kronos: event handler "${label}" has no processor, so nothing would ever deliver ` +
          `to it. Attach one at the composition root, e.g. ` +
          `\`{ ...handler, commandBus, queryBus, processor }\` where \`processor\` came from ` +
          `\`eventProcessor({ name, eventStore, tokenStore, unitOfWork })\`.`,
      )
    }
    const existing = processorGroups.get(processor.name)
    if (!existing) {
      processorGroups.set(processor.name, {
        processor,
        firstLabel: label,
        handlers: [processorEntry(handler)],
      })
      continue
    }
    if (existing.processor !== processor) {
      const conflicts = processorConflicts(existing.processor, processor)
      if (conflicts.length > 0) {
        throw new Error(
          `kronos: two event handlers name a processor called "${processor.name}" but ` +
            `configure it differently — they disagree on ${conflicts.join(", ")}. A processor's ` +
            `name is its DURABLE identity (its token is stored under it), so these two would ` +
            `fight over one cursor. Entries: "${existing.firstLabel}" and "${label}". Share one ` +
            `\`eventProcessor({ ... })\` value between them, or give them different names.`,
        )
      }
    }
    existing.handlers.push(processorEntry(handler))
  }

  // ---- Build -------------------------------------------------------------
  const stateManagers = new Map<EventStore, StateManager>()

  for (const group of groups.values()) {
    const manager = stateManager()
    for (const { state, options } of group.states) {
      // Per-state options win over the group's. `snapshotPolicy` has no
      // group-level counterpart on purpose: "how often does THIS state
      // snapshot" is a property of the state's event volume, not of the store
      // it happens to be written to.
      manager.register(
        state,
        eventSourcedRepository(
          state,
          group.eventStore,
          options.snapshotStore ?? group.snapshotStore,
          options.snapshotPolicy,
        ),
      )
    }
    stateManagers.set(group.eventStore, manager)

    // What a handler in this group can reach, passed straight through. There
    // is no component map and no lookup at dispatch: the invocation closes over
    // these objects and builds each context from them. Each entry brings its
    // OWN buses, so they are read per entry rather than once per group.
    for (const handler of group.commands) {
      subscribeCommandHandlers([handler], {
        commandBus: handler.commandBus,
        queryBus: handler.queryBus,
        stateManager: manager,
        eventStore: group.eventStore,
        tagResolver: group.tagResolver,
        ...(handler.eventScheduler !== undefined
          ? { eventScheduler: handler.eventScheduler }
          : {}),
      })
    }
    for (const handler of group.queries) {
      subscribeQueryHandlers([handler], { queryBus: handler.queryBus, stateManager: manager })
    }
  }

  // Log-less query handlers: no repositories, so no state manager to give them.
  for (const handler of storelessQueries) {
    subscribeQueryHandlers([handler], { queryBus: handler.queryBus })
  }

  // TWO-PHASE START. Every handler is subscribed before ANY processor runs, so
  // an automation replaying from a cold store can never dispatch to a command
  // whose handler is not yet subscribed. With one app per process this removes
  // boot ordering as a concern entirely.
  const processors = new Map<string, RunningProcessor>()
  const started: RunningProcessor[] = []
  for (const group of processorGroups.values()) {
    const running = runEventProcessor({
      processor: group.processor,
      handlers: group.handlers,
      ...(stateManagers.has(group.processor.eventStore)
        ? { stateManager: stateManagers.get(group.processor.eventStore)! }
        : {}),
    })
    processors.set(group.processor.name, running)
    void running.start()
    started.push(running)
  }

  return {
    processors,
    async stop() {
      for (const p of started) p.stop()
    },
  }
}
