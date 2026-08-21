import { descriptorBasedTagResolver, type TagResolver } from "./event-sourcing/tag-resolver.js"
import type { EventStore } from "./event-sourcing/event-store.js"
import type { CommandBus } from "./command-handling/bus.js"
import type { QueryBus } from "./query-handling/bus.js"
import type { CommandHandler } from "./command-handling/handler.js"
import type { QueryHandler } from "./query-handling/handler.js"
import type { EventHandler } from "./event-processing/handler.js"
import type { HandlerContext } from "./command-handling/context.js"
import type { EventHandlerContext } from "./event-processing/context.js"
import type { QueryHandlerContext } from "./query-handling/context.js"
import type { UnitOfWork } from "./unit-of-work/unit-of-work.js"
import { subscribeCommandHandlers } from "./command-handling/subscribe.js"
import { subscribeQueryHandlers } from "./query-handling/subscribe.js"
import type { EventProcessor, RunningProcessor } from "./event-processing/processor.js"
import { runEventProcessor, type ProcessorHandlerEntry } from "./event-processing/running-processor.js"

// ---------------------------------------------------------------------------
// An app is THREE LISTS. That is the whole surface.
//
// Three, not four, because `kronos` registers BEHAVIOUR — the things that have
// to be subscribed onto a bus or delivered to by a processor — and a state is
// not behaviour, it is data. A state value says which events it folds and how;
// `ctx.load(Course, id)` names it at the call site and the entry's `eventStore`
// says which log to fold. Nothing about that arrangement wants announcing in
// advance, so nothing announces it.
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
 * Entries sharing one `eventStore` OBJECT share the folds built over it, and
 * nobody arranged that: `ctx.load` builds a state's repository on first use and
 * remembers it against the store it was built from (`repositoryFor`, a weak
 * per-site cache in `event-sourcing/repository.ts`). Identity is the key, so
 * the spread above costs nothing per entry — and losing the cache would cost
 * nothing but a rebuild, which is the difference between a cache and a
 * registry.
 */
export type HandlerSite<E extends EventStore = EventStore> = {
  /**
   * The log this entry's state is sourced from and appended to — ONE store
   * object, capabilities and all.
   *
   * There is no `snapshotStore` beside it, no `eventScheduler` beside it, and
   * nothing left a host can wire HALF of. A site that caches folds is a site
   * whose log was wrapped; so is a site that can arm a deadline:
   * `postgresSchedulingEventStore(postgresSnapshottingEventStore(postgresEventStore(pg, …), pg, …), pg, …)`
   * is still one object under one name, and every capability it carries rides
   * into `ctx` in this field's TYPE.
   */
  readonly eventStore?: E
  /** Tag resolution at append time. Defaults to descriptor-derived tags. */
  readonly tagResolver?: TagResolver
  /** Diagnostics label — used in boot errors. */
  readonly name?: string
}

/** An entry plus whatever the host attached to it. */
export type Sited<T, E extends EventStore = EventStore> = T & HandlerSite<E>

/**
 * A command handler `kronos` subscribes onto its command bus.
 *
 * Widened to `<any, any>` on purpose: these are function-carrying objects, so a
 * narrower message type is checked contravariantly and a CONCRETELY typed
 * handler would not be assignable to the defaults.
 */
export type CommandHandlerEntry<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = Sited<CommandHandler<any, any, HandlerContext<U, E>>, E> & {
  /** The bus this handler is subscribed on, and the one `ctx.send` dispatches through. */
  readonly commandBus: CommandBus<U>
  /** The bus `ctx.query` and `ctx.emitUpdate` reach. */
  readonly queryBus: QueryBus<U>
}

/** A query handler `kronos` subscribes onto its query bus. */
export type QueryHandlerEntry<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = Sited<QueryHandler<any, any, QueryHandlerContext<U, E>>, E> & {
  /** The bus this handler is subscribed on, and the one `ctx.query` reaches. */
  readonly queryBus: QueryBus<U>
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
export type EventHandlerEntry<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
> = Sited<EventHandler<any, EventHandlerContext<U, E>>, E> & {
  /** Backs `ctx.send`. */
  readonly commandBus: CommandBus<U>
  /** Backs `ctx.query` and `ctx.emitUpdate`. */
  readonly queryBus: QueryBus<U>
  /** The delivery this handler belongs to. */
  readonly processor: EventProcessor<U>
}

/** One delivery: a processor value plus every handler that named it. */
type ProcessorGroup<U extends UnitOfWork, E extends EventStore> = {
  processor: EventProcessor<U>
  /** How the first entry naming this processor identifies itself, for errors. */
  readonly firstLabel: string
  readonly handlers: ProcessorHandlerEntry<U, E>[]
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
function requireEventStore<E extends EventStore>(
  entry: Sited<{ kind?: string; descriptor?: any }, E>,
  why: string,
): E {
  if (!entry.eventStore) {
    throw new Error(
      `kronos: entry "${labelOf(entry)}" ${why}, but no event store was attached. ` +
        `Attach one at the composition root, e.g. \`{ ...handler, eventStore }\`.`,
    )
  }
  return entry.eventStore
}

/**
 * Which fields of two processor values disagree.
 *
 * Objects and functions compare by IDENTITY, because that is what they mean
 * here: two token stores are the same cursor table only if they are the same
 * object, and two `sequence` functions lane identically only if they are the
 * same function. `batchSize` compares by value because a number IS its value.
 */
function processorConflicts(a: EventProcessor<any>, b: EventProcessor<any>): string[] {
  const conflicts: string[] = []
  if (a.eventStore !== b.eventStore) conflicts.push("eventStore")
  if (a.tokenStore !== b.tokenStore) conflicts.push("tokenStore")
  if (a.unitOfWork !== b.unitOfWork) conflicts.push("unitOfWork")
  if (a.sequence !== b.sequence) conflicts.push("sequence")
  if (a.deadLetterQueue !== b.deadLetterQueue) conflicts.push("deadLetterQueue")
  if ((a.batchSize ?? 1) !== (b.batchSize ?? 1)) conflicts.push("batchSize")
  return conflicts
}

export type App = {
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
 * Three lists. Nothing else — no states, no buses at this level, no stores, no
 * unit of work, no enhancer, no modules. Every shared thing rides on the
 * entries that use it, under its own name, and `kronos` groups by the DURABLE
 * NAME where a name is what identifies a thing (processors, whose tokens
 * outlive the process) and not at all where nothing needs grouping.
 *
 * ```ts
 * const eventStore = inMemoryEventStore()
 * const commandBus = interceptingCommandBus(localCommandBus(unitOfWork), correlation)
 * const queryBus = interceptingQueryBus(localQueryBus(unitOfWork), correlation)
 * const projection = eventProcessor({ name: "courses", eventStore, tokenStore, unitOfWork })
 *
 * const app = kronos({
 *   commandHandlers: billing.map((h) => ({ ...h, eventStore, commandBus, queryBus })),
 *   queryHandlers: reads.map((h) => ({ ...h, eventStore, queryBus })),
 *   eventHandlers: views.map((h) => ({ ...h, commandBus, queryBus, processor: projection })),
 * })
 * ```
 *
 * The states those handlers fold appear in NO list. `ctx.load(Course, id)`
 * takes the value straight from the module that declared it.
 */
export function kronos<
  U extends UnitOfWork = UnitOfWork,
  E extends EventStore = EventStore,
>(opts: {
  /** Handlers subscribed onto their own command bus. */
  commandHandlers?: ReadonlyArray<CommandHandlerEntry<U, E>>
  /** Handlers subscribed onto their own query bus. */
  queryHandlers?: ReadonlyArray<QueryHandlerEntry<U, E>>
  /** Handlers delivered to by their own processor. */
  eventHandlers?: ReadonlyArray<EventHandlerEntry<U, E>>
}): App {
  // ---- Subscribe the two synchronous kinds -------------------------------
  // Plainly-typed loops over flat lists. Each entry brings its OWN buses and
  // its OWN site, so there is nothing to group, nothing to resolve and nothing
  // to look up at dispatch: the invocation closes over exactly these objects.
  for (const handler of opts.commandHandlers ?? []) {
    // Command handlers always need a log: the handler's UnitOfWork flushes its
    // appended events at PREPARE_COMMIT, and there is nowhere to flush to.
    const eventStore = requireEventStore(handler, "is a command handler and appends to a log")
    subscribeCommandHandlers([handler], {
      commandBus: handler.commandBus,
      queryBus: handler.queryBus,
      eventStore,
      tagResolver: handler.tagResolver ?? descriptorBasedTagResolver(),
    })
  }

  for (const handler of opts.queryHandlers ?? []) {
    // A query handler is the only one that legitimately has no log — a read
    // model served from a projection table needs no event store to answer.
    subscribeQueryHandlers([handler], {
      queryBus: handler.queryBus,
      ...(handler.eventStore !== undefined ? { eventStore: handler.eventStore } : {}),
    })
  }

  // ---- Group event handlers by PROCESSOR NAME ----------------------------
  // Not by object identity: a token persists under the processor's NAME, so two
  // entries naming "courses" ARE one delivery even when a module built its own
  // equal value. Equal configs merge; conflicting ones are a boot error naming
  // both entries, because the alternative is two processors fighting over one
  // cursor.
  /** What one event-handler entry contributes to its delivery. */
  function processorEntry(handler: EventHandlerEntry<U, E>): ProcessorHandlerEntry<U, E> {
    return {
      definition: handler,
      commandBus: handler.commandBus,
      queryBus: handler.queryBus,
      ...(handler.eventStore !== undefined ? { eventStore: handler.eventStore } : {}),
    }
  }

  const processorGroups = new Map<string, ProcessorGroup<U, E>>()
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
