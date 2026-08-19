import type { z } from "zod"
import { qualifiedNameToString } from "../primitives/qualified-name.js"
import type { EventQuery, QueryItem } from "../query/event-query.js"
import type { EventMessage } from "../messages/message.js"
import type { EventDescriptor } from "../messages/descriptor.js"

/**
 * A named record mapping field names to Zod schemas.
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
export type IdSchema = Record<string, z.ZodType>

/**
 * Infers the runtime type from an ID schema record.
 *
 * `{ courseId: z.string() }` → `{ courseId: string }`
 * `{ courseId: z.string(), studentId: z.string() }` → `{ courseId: string, studentId: string }`
 */
export type InferIdFromSchema<T extends IdSchema> = {
  [K in keyof T]: z.infer<T[K]>
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
export interface StateLifecycle<Id = unknown, S = unknown> {
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
 * (see `packages/messaging/src/handler-context.ts`), so `msg.payload` narrows
 * to THIS event's payload without an `(s: S)` annotation anywhere.
 */
export type EvolverEntry<S = unknown, P extends z.ZodType = z.ZodType> = readonly [
  event: EventDescriptor<P>,
  evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>,
]

/**
 * Per-element mapped-tuple type for the `evolve` array: `E` is the tuple of
 * event descriptors inferred from what's passed, and each position's evolver
 * function is checked against THAT position's descriptor — not a union of
 * all of them. This is what makes `msg.payload` and the `state` return type
 * independently correct per entry.
 */
export type EvolveEntries<S, E extends readonly EventDescriptor<any>[]> = {
  [K in keyof E]: readonly [
    event: E[K],
    evolve: (
      state: S,
      message: EventMessage<z.infer<E[K] extends EventDescriptor<infer P> ? P : never>>,
    ) => S | Promise<S>,
  ]
}

/**
 * A state module — a self-contained definition of state sourced from events.
 *
 * The `Id` type is always a named record (e.g., `{ courseId: string }`),
 * enforced at compile time by requiring an {@link IdSchema} definition.
 * This ensures field names are always available for tags, evolvers,
 * and the initial function.
 */
export interface StateModule<
  Id = unknown,
  S = unknown,
> {
  readonly kind: "state-module"
  /**
   * DURABLE SNAPSHOT IDENTITY — the key snapshots are stored under, and
   * nothing else. Optional, because a state that never snapshots has nothing
   * durable to name; `kronos` refuses to boot a state that was given a
   * snapshot policy or snapshot store without one.
   *
   * It is NOT the framework's handle on this definition — see {@link identity}.
   */
  readonly name?: string
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
  /** The ID schema — maps field names to Zod types. */
  readonly idSchema: IdSchema
  readonly create: (id: Id) => S
  /** The tags this state is scoped by, as plain data. */
  readonly tags: (id: Id) => StateTags
  /** The derived DCB query: {@link tags} narrowed to the folded event types. */
  readonly query: (id: Id) => EventQuery
  readonly evolvers: ReadonlyArray<EvolverEntry<S, any>>
  readonly lifecycle?: StateLifecycle<Id, S>
}

/**
 * Process-unique identity counter. Not durable, not stable across runs —
 * see {@link StateModule.identity}.
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
 * Defines a state module — state sourced from events, scoped by an ID.
 *
 * The `id` parameter must be a named record mapping field names to Zod types.
 * A bare Zod type (e.g., `z.string()`) will not compile — you must name
 * the field (e.g., `{ courseId: z.string() }`).
 *
 * The state type is inferred from the `initial` function's return type —
 * no separate type definition needed.
 *
 * Evolvers are correlated-tuple DATA, not a callback DSL: `evolve` is an
 * array of `[EventDescriptor, (state, message) => state]` pairs. Because `S`
 * is fixed by `initial` before the evolve array is checked, and each pair is
 * checked against ITS OWN descriptor (not a union of all of them), a wrong
 * `msg.payload` access or a wrong return value is reported AT THAT PAIR.
 * When `initial` under-specifies `S` (empty arrays, unions), annotate the one
 * source of truth: `initial: (id): CourseState => (...)`.
 *
 * `tags` is plain data — the record the state is scoped by. The event-TYPE half
 * of the query is derived from `evolve`; you never write it.
 *
 * THE QUERY IS DERIVED PER EVENT TYPE, not once for the whole state. For each
 * entry in `evolve`, the state's tag record is intersected with the tag keys
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
 * `name` is optional. Its only job is durable snapshot identity, so supply it
 * when (and only when) this state is configured with a snapshot policy or
 * snapshot store — `kronos` refuses to boot if you didn't.
 *
 * ```typescript
 * const Course = state({
 *   id: { courseId: z.string() },
 *   initial: () => ({ created: false, name: "", capacity: 0 }),
 *   tags: (id) => ({ courseId: id.courseId }),
 *   evolve: [
 *     [CourseCreated, (s, { payload }) => ({ ...s, created: true, name: payload.name })],
 *   ],
 * })
 * ```
 */
export function state<IS extends IdSchema, S, E extends readonly EventDescriptor<any>[]>(def: {
  name?: string
  id: IS
  initial: (id: InferIdFromSchema<IS>) => S
  tags: (id: InferIdFromSchema<IS>) => StateTags
  evolve: EvolveEntries<S, E>
  lifecycle?: StateLifecycle<InferIdFromSchema<IS>, S>
}): StateModule<InferIdFromSchema<IS>, S> {
  const evolvers = def.evolve as unknown as ReadonlyArray<EvolverEntry<S, any>>
  const identity = `state#${++stateSequence}${def.name ? `(${def.name})` : ""}`
  const label = def.name ? `"${def.name}" (${identity})` : `${identity}`

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

  return {
    kind: "state-module",
    name: def.name,
    identity,
    idSchema: def.id,
    create: def.initial,
    tags: def.tags,
    query: (id) => deriveQuery(label, def.tags(id), evolvers),
    evolvers,
    lifecycle: def.lifecycle,
  }
}
