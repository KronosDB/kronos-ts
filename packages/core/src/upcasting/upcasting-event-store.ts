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

import type { EventMessage } from "../messaging/messages.js"
import type { EventStore } from "../event-sourcing/event-store.js"

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
 * Wrap an event store so everything it hands back passes through an upcaster.
 *
 * THING-FIRST, like every other decorator in this codebase: the store being
 * wrapped comes first, the behaviour being added second. What comes back is an
 * `EventStore` of exactly the same shape, so it drops into a state entry, a
 * handler entry or an `eventProcessor` wherever the bare one went.
 *
 * READ PATHS ONLY:
 * - `source()` — the DCB read a `ctx.load` fold is built from
 * - `open()` — the stream a processor's deliveries come off
 * - `subscribe()` — push delivery, which is a read that arrives at you
 *
 * Everything else is passed straight to the wrapped store: `append`,
 * `appendEvents`, `publish`, and the token/position members, which carry no
 * events at all. Nothing this wrapper does can change what is in the log.
 *
 * ```typescript
 * const eventStore = upcastingEventStore(
 *   postgresEventStore(pg, { tagResolver }),
 *   (e) => capacityAdded(departmentAdded(e)),
 * )
 * ```
 */
export function upcastingEventStore<E extends EventStore>(next: E, upcast: Upcast): E {
  return {
    ...next,

    async source(condition) {
      const result = await next.source(condition)
      return { ...result, events: result.events.map(upcast) }
    },

    open(condition) {
      return next
        .open(condition)
        .map((sequenced) => ({ ...sequenced, event: upcast(sequenced.event) }))
    },

    subscribe(handler) {
      return next.subscribe((events) => handler(events.map(upcast)))
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
