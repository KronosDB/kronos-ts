// UPCASTING IS INTERCEPTION AT THE LOG BOUNDARY.
//
// It is the THIRD mechanism, and deliberately the same shape as the other two,
// one boundary over. `Intercept` is `(message) => message` and runs where a bus
// hands a message on. `Upcast` is `(event) => event` and runs where the LOG
// hands an event back. Same idea, same signature, different boundary: an
// intercepting function that runs and converts messages.
//
// Which is also why there is nothing here for commands and queries. A command
// crossing versions is a message crossing a BUS, and transforming a message at
// a bus is what `interception/` already is — `interceptingCommandBus(bus,
// intercept)`. Only events have the second boundary, because only events are
// kept: a command from 2019 does not arrive today, but an event from 2019 is
// read today, every time somebody folds a state.
//
// WHY THE STORE AND NOT THE SERIALIZER. Four reasons, all of them the same
// reason from different sides:
//
//   - The serializer never sees the domain form. It sees bytes and a type name,
//     so an upcaster written there is written against raw JSON and cannot say
//     `event.tags` or `event.timestamp`. Here an `Upcast` is handed an
//     `EventMessage` — the thing your handlers actually receive.
//   - An in-memory store has no serializer at all. Put upcasting in the
//     serializer and every test that uses `inMemoryEventStore()` silently skips
//     it, which is exactly the code path you most want covered.
//   - One placement covers BOTH readers. A processor's deliveries come off
//     `open()`; a `ctx.load` fold comes off `source()`. Both are this store,
//     so both get the same treatment without either knowing.
//   - Validation happens against CURRENT schemas. A validating serializer under
//     an upcaster would judge the 2019 payload against the 2026 schema and
//     reject it before anything could fix it. Upcast at the store and the old
//     shape never meets the new schema.
//
// YOU NEVER REWRITE THE LOG. Every write member passes through untouched: what
// was appended is what is stored, forever. Upcasting is a REINTERPRETATION on
// the way out, which is what makes it safe to change one and leave the other.
//
// TOTALITY IS THE WHOLE DESIGN. An upcaster is not asked whether it applies —
// "not mine" is "return it unchanged", exactly as an `Intercept` with no
// opinion returns its argument. Plurality is composed in FUNCTION space, by the
// host, like everything else:
//
//   upcastingEventStore(store, (e) => v3(v2(v1(e))))

import { qualifiedNameToString, type EventMessage } from "../messaging/messages.js"
import type { SequencedEvent } from "../event-processing/source.js"
import type { AppendCondition } from "../event-sourcing/append-condition.js"
import type { EventStore } from "../event-sourcing/event-store.js"
import { queryItems, resolveTypeName, type EventQuery, type QueryItem } from "../event-sourcing/dcb-query.js"

/**
 * One conversion at the log boundary. TOTAL: identity for anything it does not
 * concern.
 *
 * It works in the DOMAIN form — an `EventMessage`, payload and tags and
 * timestamp and all — not in any wire form, so an upcaster is written the way a
 * handler is written. There is no shipped constructor: writing the match is the
 * whole lesson, and `is()` makes it a typed switch. Declare the OUTDATED
 * version as its own descriptor and the compiler knows what the payload looked
 * like back then:
 *
 * ```ts
 * const CourseCreatedV1 = ns.event("CourseCreated", {
 *   version: "1.0",
 *   payload: z.object({ courseId: z.string() }),          // no capacity back then
 *   tags: { courseId: (p) => p.courseId },
 * })
 *
 * const capacityAdded: Upcast = (e) => {
 *   if (is(e, CourseCreatedV1)) {
 *     return { ...e, version: CourseCreated.version, payload: { ...e.payload, capacity: 30 } }
 *   }
 *   return e
 * }
 * ```
 *
 * The target version is read off the CURRENT descriptor, never restated — a
 * version written twice is a version that can disagree with itself.
 *
 * TAGS ARE NOT RE-DERIVED. They are how this event was INDEXED when it was
 * written, and the query that just matched it matched those tags — rewriting
 * them on the way out would make the event disagree with the read that found
 * it. An upcaster changes what an event MEANS, not where it lives.
 *
 * Compose several by composing functions — there is no chain type, because
 * there is nothing a chain could do that `(e) => b(a(e))` does not.
 */
export type Upcast = (event: EventMessage) => EventMessage

/**
 * What the wrapper cannot know about an upcaster, said as data.
 *
 * A reader may ask for only some TYPES (a processor asks for the ones it
 * handles, a state for the ones it folds), and the store it asks keeps the
 * names events were WRITTEN under. An upcaster is a total function — there is no way to run it backwards
 * — so which stored types become a type the reader asked for has to be told.
 */
export type UpcastingOptions = {
  /**
   * The stored type names that upcast to `type` — `type` itself included when
   * events are still written under it. Called once per type a read, a stream
   * or an append condition asks for.
   *
   * ```ts
   * storedTypes: (type) => (type === "shop.OrderCreated" ? [type, "shop.OrderPlaced"] : [type])
   * ```
   *
   * ABSENT MEANS THE FILTER IS WIDENED, NOT TRUSTED. Without it a type
   * restriction is dropped (its tag restriction stays, tags are not re-derived
   * by an upcast): `source()` drops what was not asked for after upcasting, a
   * stream's reader discards it itself, and an append condition checks every
   * event carrying its tags. Nothing is missed, but nothing is saved either. The alternative —
   * assuming no upcaster renames anything — would lose every renamed event
   * with no error anywhere, which is not a default to hand out.
   *
   * An upcaster that only changes a version or a payload keeps the name, so
   * `(type) => [type]` is the whole answer.
   *
   * A log that hands events back without their tags (`returnsTags: false`)
   * leaves `source()` only the type to narrow by. That is enough for a query
   * of one item, and for items that ask for the same types or whose tags
   * include one another's. Any other query is refused until this is given.
   */
  readonly storedTypes?: (type: string) => ReadonlyArray<string>
}

/**
 * Wrap an event store so everything it hands back passes through an upcaster.
 *
 * THING-FIRST, like every other decorator in this codebase: the store being
 * wrapped comes first, the behaviour being added second. What comes back is an
 * `EventStore` of exactly the same shape, so it drops into a state entry, a
 * handler entry or an `eventProcessor` wherever the bare one went.
 *
 * READ PATHS:
 * - `source()` — the DCB read a `ctx.load` fold is built from
 * - `open()` — the stream a processor's deliveries come off
 *
 * A type filter on either is translated to the stored names (see
 * {@link UpcastingOptions}), and so is the query of an append condition passed
 * to `append` or `appendEvents`, so a write is checked against the same events
 * the read returned. The events being appended pass through untouched, as does
 * every other member: nothing this wrapper does can change what is in the log.
 *
 * ```typescript
 * const eventStore = upcastingEventStore(
 *   postgresEventStore(pg),
 *   (e) => capacityAdded(departmentAdded(e)),
 * )
 * ```
 */
export function upcastingEventStore<E extends EventStore>(
  next: E,
  upcast: Upcast,
  options: UpcastingOptions = {},
): E {
  const { storedTypes } = options

  /** The query as the LOG understands it: the types events are stored under. */
  function storedQuery(query: EventQuery): EventQuery {
    const items = queryItems(query).map((item): QueryItem => {
      if (item.types === undefined || item.types.length === 0) return item
      const { types: _asked, ...rest } = item
      if (storedTypes === undefined) return rest
      const stored = new Set<string>()
      for (const type of item.types) for (const name of storedTypes(resolveTypeName(type))) stored.add(name)
      return { ...rest, types: [...stored] }
    })
    return items.length === 1 ? items[0]! : items
  }

  /** An append condition checked against the names events are stored under. */
  function storedCondition(condition: AppendCondition | undefined): AppendCondition | undefined {
    if (condition === undefined) return undefined
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
      const asked = queryItems(condition.query)
      // The log was asked for the stored names, or for every type when it was
      // not told them, so what is handed back is narrowed to the query as asked.
      if (next.returnsTags !== false) {
        const result = await next.source({ ...condition, query: storedQuery(condition.query) })
        const events = result.events.map(upcast).filter((event) => asked.some((item) => matchesItem(event, item)))
        return { ...result, events }
      }
      // The events come back without tags, so they are narrowed by type alone.
      const items = untaggedItems(asked, storedTypes)
      refuseUntold(items, storedTypes)
      const result = await next.source({ ...condition, query: storedQuery(condition.query) })
      const events: EventMessage[] = []
      for (const stored of result.events) {
        const event = upcast(stored)
        const storedName = qualifiedNameToString(stored.name)
        const type = qualifiedNameToString(event.name)
        const kept = keptUntagged(items, storedName, type)
        if (kept === undefined) throw untold(type, storedName, storedTypes !== undefined)
        if (kept) events.push(event)
      }
      return { ...result, events }
    },

    appendEvents(events, condition, uow) {
      return next.appendEvents(events, storedCondition(condition), uow)
    },

    append(events, condition, uow) {
      return next.append(events, storedCondition(condition), uow)
    },

    open(condition) {
      const inner = next.open(condition.query === undefined ? condition : { ...condition, query: storedQuery(condition.query) })
      const lift = (s: SequencedEvent | undefined) => (s === undefined ? undefined : { ...s, event: upcast(s.event) })
      return {
        next: () => lift(inner.next()),
        peek: () => lift(inner.peek()),
        hasNextAvailable: () => inner.hasNextAvailable(),
        isCompleted: () => inner.isCompleted(),
        error: () => inner.error(),
        setCallback: (cb) => inner.setCallback(cb),
        close: () => inner.close(),
        // Where the stream has looked is a position, and positions are not
        // changed by an upcast.
        ...(inner.progressToken !== undefined ? { progressToken: () => inner.progressToken!() } : {}),
      }
    },
    // CAPABILITY-PRESERVING, and the type says so. `E` in, `E` out: a store
    // that could cache folds still can after upcasting is added, and a
    // `ctx.load` of a snapshotting state still typechecks against it. Typed
    // `(EventStore) => EventStore` this function would LAUNDER — the spread
    // above keeps every member at run time while the signature threw the
    // capability away, so a genuinely capable configuration would be rejected
    // by a compile-time demand for a capability it actually has.
  } as E
}

/**
 * Whether an upcast event matches one item of the query a reader asked with.
 * Tags are the stored ones, which the log already matched; the type is the
 * upcast one.
 */
function matchesItem(event: EventMessage, item: QueryItem): boolean {
  if (item.types !== undefined && item.types.length > 0) {
    const type = qualifiedNameToString(event.name)
    if (!item.types.some((asked) => resolveTypeName(asked) === type)) return false
  }
  if (item.tags === undefined) return true
  return Object.entries(item.tags).every(([key, value]) =>
    event.tags.some((tag) => tag.key === key && tag.value === value),
  )
}

/**
 * One item of a query as a read of a log that returns no tags sees it: the
 * tags the log matched, the types asked for, and the names the log was asked
 * for. No `types` is every type, no `stored` is every stored name.
 */
type UntaggedItem = {
  readonly tags: Readonly<Record<string, string>>
  readonly types: ReadonlySet<string> | undefined
  readonly stored: ReadonlySet<string> | undefined
}

function untaggedItems(
  asked: ReadonlyArray<QueryItem>,
  storedTypes: UpcastingOptions["storedTypes"],
): ReadonlyArray<UntaggedItem> {
  return asked.map((item) => {
    const tags = item.tags ?? {}
    if (item.types === undefined || item.types.length === 0) return { tags, types: undefined, stored: undefined }
    const types = new Set(item.types.map(resolveTypeName))
    if (storedTypes === undefined) return { tags, types, stored: undefined }
    return { tags, types, stored: new Set([...types].flatMap((type) => storedTypes(type))) }
  })
}

/**
 * Whether an event stored as `storedName` and upcast to `type` is in the query,
 * for an event that carries no tags. The log returned it for one of the items
 * it could have matched; which one is not known.
 *
 * It is kept when every such item keeps it: the item asks for `type`, or an
 * item that does has tags the first one's include, so the event matched those
 * too. It is dropped when none asks for `type`. Otherwise the answer depends on
 * the event's tags, and `undefined` comes back.
 */
function keptUntagged(items: ReadonlyArray<UntaggedItem>, storedName: string, type: string): boolean | undefined {
  const through = items.filter((item) => item.stored === undefined || item.stored.has(storedName))
  const keeping = through.filter((item) => item.types === undefined || item.types.has(type))
  if (keeping.length === 0) return false
  const within = (tags: UntaggedItem["tags"], of: UntaggedItem["tags"]) =>
    Object.entries(tags).every(([key, value]) => of[key] === value)
  return through.every((item) => keeping.some((keeper) => within(keeper.tags, item.tags))) ? true : undefined
}

/**
 * Refuse a read before it is made when the query alone shows that some event
 * could not be told apart: every type the query asks for, under every name it
 * may be stored as, and a type no item names when an item asks for every type.
 */
function refuseUntold(items: ReadonlyArray<UntaggedItem>, storedTypes: UpcastingOptions["storedTypes"]): void {
  if (items.length === 1) return
  const types = new Set(items.flatMap((item) => [...(item.types ?? [])]))
  for (const type of types) {
    for (const storedName of storedTypes?.(type) ?? [type]) {
      if (keptUntagged(items, storedName, type) === undefined) throw untold(type, storedName, storedTypes !== undefined)
    }
  }
  // A type and a stored name no item names.
  if (storedTypes === undefined && keptUntagged(items, "", "") === undefined) throw untold(undefined, "", false)
}

function untold(type: string | undefined, storedName: string, toldStoredTypes: boolean): Error {
  const what = type === undefined ? "an event" : `an event of type "${type}"`
  return new Error(
    `upcastingEventStore: this log hands events back without their tags, so ${what} cannot be matched to the ` +
      "item of the query it was read for. " +
      (toldStoredTypes
        ? `"${storedName}" is a stored name of types that items with different tags ask for.`
        : "Pass `storedTypes`, which is `(type) => [type]` when no upcaster renames an event."),
  )
}
