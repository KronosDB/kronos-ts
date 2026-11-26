// Upcasting: events stored under an outdated descriptor are handed out as a
// newer one. It happens where the log hands events back, so a state's fold and
// a processor's handlers both get the newer form, and so does a test over the
// in-memory store. Nothing is rewritten in the log: an append passes through.

import {
  qualifiedNameToString,
  type EventDescriptor,
  type EventMessage,
} from "../messaging/messages.js"
import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import type { SequencedEvent } from "../event-processing/source.js"
import type { AppendCondition } from "../event-sourcing/append-condition.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { queryItems, resolveTypeName, type EventQuery, type QueryItem } from "../event-sourcing/dcb-query.js"

/**
 * One step from an outdated event to a newer one. An event stored as `from`
 * (its name and version) is handed out as `to`, with the payload `map` gives.
 */
export type Upcast<From extends StandardSchemaV1 = any, To extends StandardSchemaV1 = any> = {
  readonly from: EventDescriptor<From>
  readonly to: EventDescriptor<To>
  readonly map: (payload: InferOutput<From>, event: EventMessage<InferOutput<From>>) => InferOutput<To>
}

/**
 * An {@link Upcast} from one descriptor to another. Declare the outdated
 * version as a descriptor of its own, and the payload is typed on both sides:
 *
 * ```ts
 * const CourseCreatedV1 = event({
 *   name: qn("university", "CourseCreated"),
 *   version: "1.0",
 *   payload: z.object({ courseId: z.string() }),
 *   tags: { courseId: (p) => p.courseId },
 * })
 *
 * const capacityAdded = upcast(CourseCreatedV1, CourseCreated, (payload) => ({ ...payload, capacity: 30 }))
 * ```
 *
 * The two descriptors may differ in version, in name, or in both. `map` is
 * also handed the stored event, for its metadata and timestamp.
 */
export function upcast<From extends StandardSchemaV1, To extends StandardSchemaV1>(
  from: EventDescriptor<From>,
  to: EventDescriptor<To>,
  map: (payload: InferOutput<From>, event: EventMessage<InferOutput<From>>) => InferOutput<To>,
): Upcast<From, To> {
  return { from, to, map }
}

/**
 * Wrap an event store so the events it hands back are upcast.
 *
 * ```ts
 * const eventStore = upcastingEventStore(postgresEventStore(pg), [
 *   upcast(CourseCreatedV1, CourseCreated, (payload) => ({ ...payload, capacity: 30 })),
 *   upcast(RentalOverdue, RentalLapsed, (payload) => payload),
 * ])
 * ```
 *
 * `source()` and `open()` hand back upcast events. An event goes through every
 * step that applies to it in turn, so one stored at version 1 reaches version
 * 3 through an upcast from 1 to 2 and another from 2 to 3. An event no step is
 * declared for is handed back as it is.
 *
 * Where a read, a stream or an append condition names a type, the log is asked
 * for that type and for every name an upcast to it is declared from, so events
 * stored under an old name are found. What was not asked for is dropped again
 * after upcasting.
 *
 * At most one upcast may be declared from a descriptor, and the steps may not
 * lead back to where they started; either is refused here.
 *
 * What comes back has the type of the store that went in, so a store that
 * caches snapshots or schedules events still does.
 */
export function upcastingEventStore<E extends EventStore>(next: E, upcasts: ReadonlyArray<Upcast>): E {
  const steps = stepsOf(upcasts)
  const storedNames = storedNamesOf(upcasts)

  /** `event` after every step that applies to it. */
  function lift(event: EventMessage): EventMessage {
    let step = steps.get(qualifiedNameToString(event.name))?.get(event.version)
    while (step !== undefined) {
      event = { ...event, name: step.to.name, version: step.to.version, payload: step.map(event.payload, event) }
      step = steps.get(qualifiedNameToString(event.name))?.get(event.version)
    }
    return event
  }

  /** The names events of `type` may be stored under: its own and every renamed one. */
  const storedOf = (type: string): ReadonlyArray<string> => storedNames.get(type) ?? [type]

  /** The query as the log is asked it: each named type with the names it may be stored under. */
  function storedQuery(query: EventQuery): EventQuery {
    if (storedNames.size === 0) return query
    const items = queryItems(query).map((item): QueryItem => {
      if (item.types === undefined || item.types.length === 0) return item
      return { ...item, types: [...new Set(item.types.flatMap((type) => storedOf(resolveTypeName(type))))] }
    })
    return items.length === 1 ? items[0]! : items
  }

  function storedCondition(condition: AppendCondition | undefined): AppendCondition | undefined {
    if (condition === undefined || storedNames.size === 0) return condition
    return {
      ...condition,
      query: storedQuery(condition.query),
      ...(condition.reads !== undefined
        ? { reads: condition.reads.map((read) => ({ ...read, query: storedQuery(read.query) })) }
        : {}),
    }
  }

  return {
    ...next,

    async source(condition) {
      if (steps.size === 0) return next.source(condition)
      // With no renames the log is asked what the reader asked, and an upcast
      // changes no name, so everything it hands back was asked for.
      if (storedNames.size === 0) {
        const result = await next.source(condition)
        return { ...result, events: result.events.map(lift) }
      }
      const asked = askedItems(queryItems(condition.query), storedOf)
      refuseUndecidable(asked, upcasts)
      const result = await next.source({ ...condition, query: storedQuery(condition.query) })
      const events: EventMessage[] = []
      for (const stored of result.events) {
        const event = lift(stored)
        const storedName = qualifiedNameToString(stored.name)
        const type = qualifiedNameToString(event.name)
        const answer = isAsked(asked, storedName, type)
        if (answer === undefined) throw undecidable(storedName, type)
        if (answer) events.push(event)
      }
      return { ...result, events }
    },

    append(events, condition, uow) {
      return next.append(events, storedCondition(condition), uow)
    },

    open(condition) {
      if (steps.size === 0) return next.open(condition)
      const inner = next.open(
        condition.query === undefined ? condition : { ...condition, query: storedQuery(condition.query) },
      )
      const lifted = (s: SequencedEvent | undefined) => (s === undefined ? undefined : { ...s, event: lift(s.event) })
      return {
        next: () => lifted(inner.next()),
        peek: () => lifted(inner.peek()),
        hasNextAvailable: () => inner.hasNextAvailable(),
        isCompleted: () => inner.isCompleted(),
        error: () => inner.error(),
        setCallback: (cb) => inner.setCallback(cb),
        close: () => inner.close(),
        // Where the stream has looked is a position, which an upcast does not change.
        ...(inner.progressToken !== undefined ? { progressToken: () => inner.progressToken!() } : {}),
      }
    },
  } as E
}

/** The upcasts by the name and then the version they are declared from. */
function stepsOf(upcasts: ReadonlyArray<Upcast>): ReadonlyMap<string, ReadonlyMap<string, Upcast>> {
  const steps = new Map<string, Map<string, Upcast>>()
  const described = (d: EventDescriptor<any>) => `${qualifiedNameToString(d.name)} ${d.version}`
  for (const step of upcasts) {
    const name = qualifiedNameToString(step.from.name)
    const versions = steps.get(name) ?? new Map<string, Upcast>()
    if (versions.has(step.from.version)) {
      throw new Error(`upcastingEventStore: more than one upcast is declared from ${described(step.from)}.`)
    }
    versions.set(step.from.version, step)
    steps.set(name, versions)
  }
  for (const first of upcasts) {
    const passed = new Set<string>()
    for (let step: Upcast | undefined = first; step !== undefined; ) {
      if (passed.has(described(step.from))) {
        throw new Error(`upcastingEventStore: the upcasts from ${described(first.from)} lead back to it.`)
      }
      passed.add(described(step.from))
      step = steps.get(qualifiedNameToString(step.to.name))?.get(step.to.version)
    }
  }
  return steps
}

/**
 * For each name an upcast leads to from another name: that name and every name
 * events handed out under it may be stored under. Empty when no upcast renames.
 */
function storedNamesOf(upcasts: ReadonlyArray<Upcast>): ReadonlyMap<string, ReadonlyArray<string>> {
  const renamedFrom = new Map<string, Set<string>>()
  for (const { from, to } of upcasts) {
    const stored = qualifiedNameToString(from.name)
    const handedOut = qualifiedNameToString(to.name)
    if (stored === handedOut) continue
    const sources = renamedFrom.get(handedOut) ?? new Set<string>()
    sources.add(stored)
    renamedFrom.set(handedOut, sources)
  }
  const storedNames = new Map<string, ReadonlyArray<string>>()
  for (const name of renamedFrom.keys()) {
    const stored = new Set([name])
    for (const reached of stored) for (const source of renamedFrom.get(reached) ?? []) stored.add(source)
    storedNames.set(name, [...stored])
  }
  return storedNames
}

/**
 * One item of a query as the read sees it: the tags the log matches, the types
 * asked for, and the names the log is asked for. No `types` is every type.
 */
type AskedItem = {
  readonly tags: Readonly<Record<string, string>>
  readonly types: ReadonlySet<string> | undefined
  readonly stored: ReadonlySet<string> | undefined
}

function askedItems(
  items: ReadonlyArray<QueryItem>,
  storedOf: (type: string) => ReadonlyArray<string>,
): ReadonlyArray<AskedItem> {
  return items.map((item) => {
    const tags = item.tags ?? {}
    if (item.types === undefined || item.types.length === 0) return { tags, types: undefined, stored: undefined }
    const types = new Set(item.types.map(resolveTypeName))
    return { tags, types, stored: new Set([...types].flatMap((type) => storedOf(type))) }
  })
}

/**
 * Whether an event stored as `storedName` and handed out as `type` is in the
 * query. The log returned it for one of the items that ask for its stored
 * name; which one is not known, because the event comes back without the tags
 * it matched on.
 *
 * It is in the query when every such item asks for `type`, or has tags that
 * include the tags of one that does, so the event matched those too. It is not
 * when none asks for `type`. Otherwise the answer depends on the event's tags,
 * and `undefined` comes back.
 */
function isAsked(items: ReadonlyArray<AskedItem>, storedName: string, type: string): boolean | undefined {
  const through = items.filter((item) => item.stored === undefined || item.stored.has(storedName))
  const asking = through.filter((item) => item.types === undefined || item.types.has(type))
  if (asking.length === 0) return false
  const within = (tags: AskedItem["tags"], of: AskedItem["tags"]) =>
    Object.entries(tags).every(([key, value]) => of[key] === value)
  return through.every((item) => asking.some((one) => within(one.tags, item.tags))) ? true : undefined
}

/**
 * Refuse a read before it is made when {@link isAsked} could not answer for an
 * event it may return: every stored name the query asks the log for, under
 * each name an upcast hands it out as, and under its own when an item asks for
 * that.
 */
function refuseUndecidable(items: ReadonlyArray<AskedItem>, upcasts: ReadonlyArray<Upcast>): void {
  if (items.length === 1) return
  const handedOutAs = new Map<string, Set<string>>()
  for (const { from, to } of upcasts) {
    const stored = qualifiedNameToString(from.name)
    const names = handedOutAs.get(stored) ?? new Set<string>()
    names.add(qualifiedNameToString(to.name))
    handedOutAs.set(stored, names)
  }
  const askedFor = new Set(items.flatMap((item) => [...(item.types ?? [])]))
  const askedOfLog = new Set(items.flatMap((item) => [...(item.stored ?? [])]))
  for (const storedName of askedOfLog) {
    const types = new Set(handedOutAs.get(storedName) ?? [])
    for (const reached of types) for (const further of handedOutAs.get(reached) ?? []) types.add(further)
    if (askedFor.has(storedName)) types.add(storedName)
    for (const type of types) {
      if (isAsked(items, storedName, type) === undefined) throw undecidable(storedName, type)
    }
  }
}

function undecidable(storedName: string, type: string): Error {
  return new Error(
    `upcastingEventStore: this read cannot tell which item of its query an event stored as "${storedName}" ` +
      `and handed out as "${type}" was read for: items with different tags ask for it. ` +
      "Read them with separate queries.",
  )
}
