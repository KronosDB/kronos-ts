import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  qualifiedNameToString,
  type EventMessage,
  type EventDescriptor,
} from "../messaging/messages.js"
import type { EventQuery, QueryItem } from "./dcb-query.js"
import type { SnapshotConfig } from "./snapshot.js"
/**
 * A named record mapping field names to schemas — any Standard Schema, so zod,
 * valibot, arktype or a hand-written one.
 * Used to define state IDs with explicit field names.
 *
 * ```typescript
 * // Simple ID
 * { courseId: z.string() }
 *
 * // Composite ID
 * { courseId: z.string(), studentId: z.string() }
 * ```
 */
export type IdSchema = Record<string, StandardSchemaV1>

/**
 * Infers the runtime type from an ID schema record.
 *
 * `{ courseId: z.string() }` → `{ courseId: string }`
 * `{ courseId: z.string(), studentId: z.string() }` → `{ courseId: string, studentId: string }`
 */
export type InferIdFromSchema<T extends IdSchema> = {
  [K in keyof T]: InferOutput<T[K]>
}

/** One tag set: every entry must be present on an event for it to match. */
export type TagRecord = Record<string, string>

/**
 * The tag sets a state is scoped by, as PLAIN DATA.
 *
 * ONE RECORD IS THE ANSWER, INCLUDING FOR MULTI-STREAM DCB STATES. Write every
 * tag the state is scoped by — `{ courseId, studentId }` — and the derivation
 * scopes each key to the event types that actually declare it. A course event
 * declaring only `courseId` and a faculty enrolment declaring only `studentId`
 * both feed one subscription state, from that single record, with no OR written
 * by hand. See {@link state} for the derivation rules.
 *
 * AN ARRAY OF RECORDS IS AN OVERRIDE, and rarely the right tool. It replaces
 * the per-type derivation with a blunt OR: every record is paired with EVERY
 * folded event type, tag-key intersection is not consulted, and the
 * unmatchable-fold check is skipped. Reach for it only when a state genuinely
 * needs a scope the intersection cannot express — e.g. matching the SAME event
 * type under two alternative tag sets, or deliberately claiming a WIDER
 * conflict window than the events' own keys imply. If you are reaching for it
 * because "this state spans two streams", you want the plain record.
 *
 * The array branch is a NON-EMPTY tuple rather than a plain array for two
 * reasons. An OR of zero tag sets is meaningless. And `Record<string, string>`
 * has a string index signature, which an array literal also satisfies — with a
 * plain `Record | Record[]` union, TypeScript contextually types the elements
 * of `[{ courseId }, { studentId }]` as `string` and reports a confusing error
 * on each element. The tuple makes the array branch the only candidate for an
 * array literal, so the OR form just works.
 */
export type StateTags = TagRecord | readonly [TagRecord, ...TagRecord[]]

/**
 * Lifecycle hooks for state transitions.
 */
export type StateLifecycle<Id = unknown, S = unknown> = {
  /** Called when the first event transitions from initial state. */
  onCreate?: (state: S, id: Id) => void | Promise<void>
  /** Called when the state transitions to a deleted state. */
  onDelete?: (state: S, id: Id) => void | Promise<void>
  /** Called after each evolver application when state changes. */
  onStateChange?: (from: S, to: S, event: EventMessage, id: Id) => void | Promise<void>
  /** Predicate that detects deleted state. */
  isDeleted?: (state: S) => boolean
}

/**
 * One `evolve` entry: an event descriptor paired with the function that folds
 * it into state. The pairing is DATA, not a builder call — the same
 * correlated-tuple technique `ContextAppendFunction` uses for its batch form
 * (see `command-handling/context.ts`), so `msg.payload` narrows
 * to THIS event's payload without an `(s: S)` annotation anywhere.
 */
export type EvolverEntry<S = unknown, P extends StandardSchemaV1 = StandardSchemaV1> = readonly [
  event: EventDescriptor<P>,
  evolve: (state: S, message: EventMessage<InferOutput<P>>) => S | Promise<S>,
]

/**
 * What a fold is made of, positionally: a SEED, then one event descriptor per
 * case. It is the constraint on the inference variable — the shape TypeScript
 * infers `evolve` INTO — never a shape a host writes.
 *
 * ELEMENT ZERO IS DELIBERATELY `unknown` HERE, and that is not laxity: an initial state
 * that READS ITS ID is a context-sensitive function expression, which
 * TypeScript cannot type until it has fixed `E`, and fixing `E` against a
 * constraint that DEMANDS a function at position zero collapses the whole tuple
 * to that constraint — every case's descriptor with it. Leaving position zero
 * open lets the reverse-mapping keep the CASES while the initial state is still being
 * worked out. What an initial state must actually be is stated in {@link EvolveTuple},
 * which is the type the argument is checked against.
 */
export type EvolveShape = readonly [initial: unknown, ...cases: EventDescriptor<any>[]]

/**
 * The state type a fold produces, read off ELEMENT ZERO — the fold's type comes
 * from the fold. It is the FALLBACK for `S`: an initial state that declines the
 * id is not context-sensitive, so TypeScript settles it in the first inference
 * pass and `S` is read back off it here. One that takes the id infers `S`
 * directly from its return instead, and this is never consulted.
 */
export type InitialState<E extends EvolveShape> = E[0] extends (id: any) => infer S ? S : never

/**
 * The whole fold, as ONE positional value: the initial state, then the cases.
 *
 * ELEMENT ZERO IS THE INITIAL STATE — the evolver of nothing; it may read the
 * identity it is folded for. The fold is `cases.reduce(...)` starting from
 * `evolve[0](id)`, so it belongs in the same list as the evolvers of something
 * rather than in a field beside it. It
 * is handed the IDENTITY it is being folded for: nothing has happened yet, so
 * the id is the one thing the zeroth state can honestly know, and a fold that
 * carries its own key no longer has to wait for an event to tell it what it
 * already is. An initial state that does not care DECLINES the argument by writing none —
 * `() => ({ … })` stays assignable to `(id) => S` by TypeScript's arity rule,
 * which is why most folds never mention an id. The grammar is POSITIONAL and
 * statically typed: the tuple is destructured once, `const [initial, ...cases] =
 * evolve`, and nothing at runtime ever has to ask which shape an element is.
 *
 * Per-element mapped-tuple: `E` is inferred from what was passed, so each
 * position's evolver is checked against THAT position's descriptor — not a
 * union of all of them — and against the `S` element zero declared. That is
 * what makes a wrong `msg.payload` access or a wrong return value a compile
 * error AT THAT CASE. The mapped type must be the WHOLE property type and every
 * position must mention `E[K]`, which is why position zero is an INTERSECTION
 * rather than either half alone: `(id: Id) => S` is what gives the initial state's `id`
 * a type and `S` an inference site, `E[K]` is what keeps the reverse-mapping —
 * and the cases — alive. A rest-tuple spelling (`[initial, ...Cases<E>]`) loses
 * the cases' contextual types entirely; either half of the intersection alone
 * loses one of the two sources of inference.
 */
export type EvolveTuple<E extends EvolveShape, Id = unknown, S = InitialState<E>> = {
  [K in keyof E]: K extends "0"
    ? ((id: Id) => S) & E[K]
    : readonly [
        event: E[K],
        evolve: (
          state: S,
          message: EventMessage<InferOutput<E[K] extends EventDescriptor<infer P> ? P : never>>,
        ) => S | Promise<S>,
      ]
}

/**
 * A state — a self-contained definition of state sourced from events.
 *
 * The `Id` type is always a named record (e.g., `{ courseId: string }`),
 * enforced at compile time by requiring an {@link IdSchema} definition.
 * This ensures field names are always available for tags, evolvers,
 * and the initial function.
 *
 * `Snap` SAYS WHETHER THIS FOLD IS CACHED, in the type. It is not decoration
 * and it is not a second field: it is what {@link State.snapshot} is typed BY,
 * so `State<Id, S, true>` is a state that carries a `SnapshotConfig` and
 * `State<Id, S>` is one whose config is `undefined` and can never be anything
 * else. `state()` reads it off the definition you wrote — pass a `snapshot`
 * config and you get `true` back — and `ctx.load` reads it off the value you
 * hand it, which is what the repository reads to decide whether to ask the
 * wired log for a snapshot. Nothing on a handler's context names the tier.
 */
export type State<
  Id = unknown,
  S = unknown,
  Snap extends boolean = false,
> = {
  readonly kind: "state-module"
  /**
   * The framework's handle on THIS definition — process-unique, assigned by
   * `state()`, never durable and never written anywhere.
   *
   * A property rather than the object reference itself because hosts spread
   * states to attach stores (`{ ...Course, stores }`), so the object a handler
   * calls `ctx.load` with is not the object `kronos` registered. The identity
   * rides along through the spread; the reference does not.
   */
  readonly identity: string
  /** The ID schema — maps field names to Standard Schemas. */
  readonly idSchema: IdSchema
  /**
   * `evolve[0]` — the zeroth state, the evolver of nothing.
   *
   * It is handed the id it is being folded for. Nothing has happened yet, so
   * the identity is the ONLY thing the zeroth state can honestly know, and a
   * fold that wants to carry its own key does not have to wait for an event to
   * tell it what it already is.
   */
  readonly initial: (id: Id) => S
  /** The tags this state is scoped by, as plain data. */
  readonly tags: (id: Id) => StateTags
  /** The derived DCB query: {@link tags} narrowed to the folded event types. */
  readonly query: (id: Id) => EventQuery
  /** `evolve.slice(1)` — the cases, initial state excluded. */
  readonly evolvers: ReadonlyArray<EvolverEntry<S, any>>
  /**
   * Where this state's snapshots are filed and when one is written — `{ key,
   * when }`. Absent means this state is never cached.
   *
   * It rides on the STATE because both halves are properties of the fold: which
   * cache this fold reads, and how often caching it pays. Where a snapshot
   * LANDS is a site fact — the entry's `eventStore`, which must have been
   * wrapped in its family's `…SnapshottingEventStore` for anything to be read
   * or written.
   *
   * ITS TYPE IS THE DEMAND'S FOOTHOLD. `Snap extends true` and this is the
   * config; otherwise it is `undefined` and nothing else fits. `ctx.load`
   * against a bare log asks for a field that a `SnapshotConfig` does not have,
   * so a state that declares a policy is refused there and a state that does
   * not is waved through without ever meeting the concept.
   */
  readonly snapshot?: Snap extends true ? SnapshotConfig : undefined
  readonly lifecycle?: StateLifecycle<Id, S>
}

/**
 * Process-unique identity counter. Not durable, not stable across runs —
 * see {@link State.identity}.
 */
let stateSequence = 0

/**
 * Derives the DCB query for a state scoped by ONE tag record — the granular
 * path, and the reason `tags` is a single record even for multi-stream states.
 *
 * Per folded event type, the state's tag record is intersected with the KEYS
 * that event type declares (`EventDescriptor.tagKeys`). The DISTINCT
 * intersections become the ITEMS of the query — items are ORed — and each event
 * type joins every item whose tag set it declares in full. Event types with the
 * same intersection share an item, so a single-stream state still derives
 * exactly ONE item rather than one per evolver.
 *
 * This is the data-first equivalent of Axon Framework 5's
 * `AnnotationBasedEventCriteriaResolver`: the same "each event type is matched
 * on the tags IT declares" resolution, driven by the descriptor and the fold
 * rather than by annotations on a class.
 *
 * Two facts make the intersection sound rather than a guess. `tagKeys` is
 * exhaustive and payload-independent by construction (see `event()`), and an
 * EMPTY intersection is an error, not an empty filter — a state that folds an
 * event sharing none of its tags has declared a fold it can never source, which
 * is a modelling mistake worth failing on.
 *
 * The derived query can never be match-all: every item carries at least one
 * tag, because an empty intersection throws before an item is built.
 */
function deriveGranularQuery(
  label: string,
  record: TagRecord,
  evolvers: ReadonlyArray<EvolverEntry<any, any>>,
): EventQuery {
  const stateKeys = Object.keys(record)

  // No evolvers: tags only, no type filter. An empty fold means "all types",
  // never "no types" — and the tag record still bounds it, so this cannot
  // degrade into a match-all query.
  if (evolvers.length === 0) {
    if (stateKeys.length === 0) {
      throw new Error(
        `State ${label} has neither tags nor evolvers, so its query would match EVERY event in the store. ` +
        "Give it a tag record that scopes it, or evolvers that name the event types it folds.",
      )
    }
    return { tags: record }
  }

  // Step 1 — what each folded type can actually be matched on: the state's tag
  // keys intersected with the keys that type declares, kept in the state's own
  // key order so everything downstream is deterministic.
  const folds: Array<{ type: string; shared: readonly string[] }> = []

  for (const [descriptor] of evolvers) {
    const type = qualifiedNameToString(descriptor.name)
    const declared = descriptor.tagKeys

    if (declared === undefined) {
      throw new Error(
        `State ${label} folds "${type}", but "${type}" does not declare its tag keys, ` +
        "so the state's query cannot be scoped to it. " +
        "Give that event's `tags` as a record of extractors — " +
        "`tags: { courseId: (p) => p.courseId }` — or, if it needs the function form, " +
        "declare `tagKeys: [...]` next to it.",
      )
    }

    const shared = stateKeys.filter((key) => declared.includes(key))

    if (shared.length === 0) {
      throw new Error(
        `State ${label} folds "${type}", but they share no tag key: the state is scoped by ` +
        `${formatKeys(stateKeys)} and "${type}" carries ${formatKeys(declared)}. ` +
        "That fold can never fire — no event of that type can match this state's query. " +
        "Either scope the state by a key that event carries, tag the event with one of the " +
        "state's keys, or drop the evolver.",
      )
    }

    folds.push({ type, shared })
  }

  // Step 2 - the candidate tag sets are the DISTINCT intersections, in
  // first-seen order. Each becomes one item of the query.
  const candidates: Array<readonly string[]> = []
  for (const { shared } of folds) {
    if (!candidates.some((c) => sameKeys(c, shared))) candidates.push(shared)
  }

  // Step 3 - a type joins EVERY item whose tag set it declares in full, not
  // only the item matching its own intersection exactly.
  //
  // This is the step that lets ONE plain record span streams. Take a
  // subscription scoped by `{ courseId, studentId }` folding a course event
  // (declares courseId), a faculty enrolment (studentId) and a subscription
  // event (both). The subscription event declares `{ courseId }` in full, so it
  // also rides the courseId item - which is what lets the state see OTHER
  // students' subscriptions to the same course, exactly what a capacity check
  // needs. Pinning it to `courseId AND studentId` instead would silently drop
  // every other student's event: an under-sourced fold, AND an append condition
  // too narrow to catch the very conflict it exists to catch.
  //
  // Where nothing forces the wider read - no sibling type declares a proper
  // subset - a type keeps its full intersection, so a state scoped by
  // `{ tenantId, orderId }` folding only order events still ANDs both keys and
  // never sources a whole tenant.
  const items: QueryItem[] = candidates.map((keys) => ({
    tags: Object.fromEntries(keys.map((key) => [key, record[key]!])) as TagRecord,
    types: [...new Set(folds.filter((f) => isSubset(keys, f.shared)).map((f) => f.type))],
  }))

  // Step 4 - drop any item that a strictly broader-matching sibling already
  // covers. Fewer tags matches strictly more events, so if every type under
  // item K also appears under some K' whose keys are a proper subset of K's,
  // then K adds nothing. Pure minimisation: it never changes which events
  // match, it just keeps the derived query small.
  const kept = items.filter((item, i) => {
    const keys = candidates[i]!
    return !items.some((other, j) => {
      if (i === j) return false
      const otherKeys = candidates[j]!
      return (
        otherKeys.length < keys.length &&
        isSubset(otherKeys, keys) &&
        item.types!.every((type) => other.types!.includes(type))
      )
    })
  })

  return kept.length === 1 ? kept[0]! : kept
}

/** Key-list equality. Both sides are always built in the state's key order. */
function sameKeys(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((key, i) => key === b[i])
}

/** Is every key of `subset` present in `of`? */
function isSubset(subset: readonly string[], of: readonly string[]): boolean {
  return subset.every((key) => of.includes(key))
}

function formatKeys(keys: readonly string[]): string {
  return keys.length === 0 ? "no tags" : `{ ${keys.join(", ")} }`
}

/**
 * Derives a state's scope into its DCB query — granular for the single-record
 * form, a blunt OR for the array override. See {@link StateTags}.
 */
function deriveQuery(
  label: string,
  scope: StateTags,
  evolvers: ReadonlyArray<EvolverEntry<any, any>>,
): EventQuery {
  if (!Array.isArray(scope)) return deriveGranularQuery(label, scope as TagRecord, evolvers)

  // OVERRIDE: every record against every folded type, intersection not
  // consulted. Deliberately keeps the pre-granular behaviour.
  const foldedTypes = evolvers.map(([descriptor]) => qualifiedNameToString(descriptor.name))
  return (scope as ReadonlyArray<TagRecord>).map((tags) => ({ tags, types: foldedTypes }))
}

/**
 * A value stood in for an id field when the derivation is checked at BOOT,
 * before any real id exists. Only the KEYS of the resulting tag record are
 * read, never the values.
 */
const BOOT_PROBE_ID_VALUE = "kronos:boot-probe"

/**
 * Defines a state — state sourced from events, scoped by an ID.
 *
 * The `id` parameter must be a named record mapping field names to schemas.
 * A bare schema (e.g., `z.string()`) will not compile — you must name
 * the field (e.g., `{ courseId: z.string() }`).
 *
 * The state type is inferred from the SEED's return type — no separate type
 * definition needed.
 *
 * THE INITIAL STATE IS PART OF THE FOLD. `evolve` is one tuple whose FIRST element is
 * the initial state and whose remaining elements are correlated-tuple DATA — the
 * `[EventDescriptor, (state, message) => state]` pairs. The fold is
 * `cases.reduce(...)` starting from `evolve[0](id)`: the initial state is the evolver of
 * nothing, so it sits in the same list rather than in a field beside it, and
 * the grammar is positional — element zero, always. Because `S` is fixed by the
 * initial state before the cases are checked, and each pair is checked against ITS OWN
 * descriptor (not a union of all of them), a wrong `msg.payload` access or a
 * wrong return value is reported AT THAT PAIR. When the initial state under-specifies
 * `S` (empty arrays, unions), annotate the one source of truth:
 * `(): CourseState => (...)`.
 *
 * THE INITIAL STATE MAY READ THE IDENTITY IT IS BEING FOLDED FOR. It is handed the id —
 * the same inferred record `tags` takes — so a zeroth state can carry its own
 * key without waiting for an event to tell it what it already is:
 * `(id): SubscriptionState => ({ courseId: id.courseId, … })`. Nothing has
 * happened yet, so the identity is the only thing it can honestly know. An initial state
 * that does not care declines the argument by writing none — `() => ({ … })` is
 * assignable to `(id) => S`, which is why most folds never mention an id.
 *
 * `tags` is plain data — the record the state is scoped by. The event-TYPE half
 * of the query is derived from the CASES — `evolve.slice(1)`, the initial state
 * naming no event type — and you never write it.
 *
 * THE QUERY IS DERIVED PER EVENT TYPE, not once for the whole state. For each
 * case, the state's tag record is intersected with the tag keys
 * that event type declares, and that type is scoped to just the shared keys;
 * several shared keys are ANDed within one query item, and the query is the OR
 * across items. So a state scoped by `{ courseId, studentId }` folding a course event
 * (which carries only `courseId`) and an enrolment event (only `studentId`)
 * derives exactly `courseId`-on-course-events OR `studentId`-on-enrolments —
 * the multi-stream DCB scope, from one plain record.
 *
 * Two consequences worth knowing. Because the SAME query is the append
 * condition, conflict windows narrow to the tags each event type is actually
 * matched on, rather than every folded type being claimed under every tag. And
 * an evolver whose event shares NO key with the state's tags is a boot error:
 * that fold can never fire, so it is a modelling mistake, not a no-op.
 *
 * This requires each folded event to declare its tag keys, which the record
 * form of `event({ tags })` does for free. See {@link StateTags} for the array
 * override, and `EventDescriptor.tagKeys` for the function-form escape hatch.
 *
 * THE FIELD ORDER IS THE READING ORDER — `id · tags · evolve · snapshot? ·
 * lifecycle?`. NOTHING NAMES A STATE: a state that caches its fold says WHERE
 * under `snapshot: { key, when }`, in a string you wrote, and diagnostics name a
 * state by its process identity and the events it folds. Each field is a
 * function of what came before it: tags are derived from the id, and `evolve`
 * carries its own initial state at position zero.
 *
 * ```typescript
 * const Course = state({
 *   id: { courseId: z.string() },
 *   tags: (id) => ({ courseId: id.courseId }),
 *   evolve: [
 *     () => ({ created: false, name: "", capacity: 0 }),
 *     [CourseCreated, (s, { payload }) => ({ ...s, created: true, name: payload.name })],
 *   ],
 * })
 * ```
 */
export function state<
  IS extends IdSchema,
  E extends EvolveShape,
  // The initial state's return, when it reads its id — a context-sensitive
  // literal, inferred in the second pass. An initial state that declines the id is
  // settled in the FIRST pass and contributes no candidate here, so the
  // default reads `S` back off element zero instead. Both routes end at the
  // same place: the fold's type comes from the fold.
  S = InitialState<E>,
  // WHETHER THIS FOLD IS CACHED, read off the definition. Writing a `snapshot`
  // config gives `C` that config's type and the return says `State<…, true>`;
  // writing none leaves `C` at its default and the return says `State<…,
  // false>`. The repository reads that inference at runtime to decide whether
  // to ask the wired log for a snapshot, and a host writes nothing to get it.
  C extends SnapshotConfig | undefined = undefined,
>(def: {
  id: IS
  tags: (id: InferIdFromSchema<IS>) => StateTags
  /**
   * The whole fold: the INITIAL STATE at position zero, then one case per event
   * type. See {@link EvolveTuple} — the fold is `cases.reduce(...)` starting
   * from `evolve[0](id)`, and the initial state is the evolver of nothing.
   */
  evolve: EvolveTuple<E, InferIdFromSchema<IS>, S>
  /**
   * Where snapshots of this state are filed, and when one is written —
   * `{ key, when }`. Absent = never cached. Changing `key` orphans every old
   * entry, which is the whole invalidation story.
   *
   * WRITING THIS FIELD IS AN OBLIGATION ON YOUR WIRING. Every entry whose
   * handler loads this state must carry a log that can serve a cached fold —
   * an `eventStore` wrapped in its family's `…SnapshottingEventStore` — and the
   * compiler holds you to it at the `ctx.load` call rather than at run time.
   */
  snapshot?: C
  lifecycle?: StateLifecycle<InferIdFromSchema<IS>, S>
}): State<InferIdFromSchema<IS>, S, C extends SnapshotConfig ? true : false> {
  // ONE destructure, and the shapes are settled for good. No `Array.isArray`
  // anywhere downstream: position zero IS the initial state, by grammar.
  const [initial, ...cases] = def.evolve as unknown as readonly [
    (id: InferIdFromSchema<IS>) => S,
    ...ReadonlyArray<EvolverEntry<S, any>>,
  ]
  const evolvers = cases as ReadonlyArray<EvolverEntry<S, any>>
  const identity = `state#${++stateSequence}`
  /**
   * How a state is NAMED IN A DIAGNOSTIC, now that nothing names one by hand.
   * Its process identity plus the events it folds — which is what a reader
   * needs to find the definition, and is derived like everything else.
   */
  const foldedTypes = evolvers.map(([descriptor]) =>
    `${qualifiedNameToString(descriptor.name)}@${descriptor.version ?? "1.0"}`,
  )
  const label = foldedTypes.length === 0
    ? identity
    : `${identity} (folds ${foldedTypes.map((t) => t.split("@")[0]).join(", ")})`

  /**
   * BOOT CHECK. The derivation's failure modes — an event type that never
   * declared its tag keys, an evolver that can never fire — are properties of
   * the DEFINITION, not of any particular id, so they are worth reporting at
   * definition time rather than on the first `load`.
   *
   * The id is stood in for, because at boot there isn't one. If `tags` cannot
   * survive a probe id (it destructures a value, parses it, anything), the
   * probe is abandoned rather than turned into a spurious boot failure — the
   * same derivation runs on every `query(id)` call, so nothing escapes, it
   * just gets reported slightly later.
   */
  let probeScope: StateTags | undefined
  try {
    const probeId = Object.fromEntries(
      Object.keys(def.id).map((field) => [field, BOOT_PROBE_ID_VALUE]),
    ) as InferIdFromSchema<IS>
    probeScope = def.tags(probeId)
  } catch {
    probeScope = undefined
  }
  if (probeScope !== undefined) deriveQuery(label, probeScope, evolvers)

  // The one cast in this function, and it is the conditional return type meeting
  // the plain record that satisfies it: `snapshot` is `C`, and the declared
  // return types it as `SnapshotConfig` exactly when `C` is one. TypeScript
  // cannot check a value against an unresolved conditional, so the equivalence
  // is asserted here and PINNED by the type probe.
  return {
    kind: "state-module",
    identity,
    idSchema: def.id,
    initial,
    tags: def.tags,
    query: (id) => deriveQuery(label, def.tags(id), evolvers),
    evolvers,
    snapshot: def.snapshot,
    lifecycle: def.lifecycle,
  } as State<InferIdFromSchema<IS>, S, C extends SnapshotConfig ? true : false>
}
