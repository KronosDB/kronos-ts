import { type Tag, tagsFromRecord } from "../messaging/tag.js"
import {
  type QualifiedName,
  qualifiedNameToString,
  type EventDescriptor,
} from "../messaging/messages.js"
// ---------------------------------------------------------------------------
// Two shapes, one direction.
//
// An `EventQuery` is what a CALLER writes: plain query items, ORed together.
// `EventCriteria` is what a STORE reads: the tagged union the in-memory matcher,
// the Postgres WHERE builder and the KronosDB / Axon Server criterion converters
// all switch on. `compileQuery()` is the one direction between them, and it is a
// compiler over data — there is no fluent builder, nothing to chain, and nothing
// that has to be constructed before it can be inspected.
//
// The caller-facing vocabulary is the DCB specification's (dcb.events): a Query
// is a list of Query Items; within an item `types` is an ANY-OF and `tags` an
// ALL-OF; the items themselves are ORed. `EventCriteria` and the `criteria*`
// names beneath the boundary are Axon's, and stay store-internal.
// ---------------------------------------------------------------------------

/**
 * ONE ITEM of an event query — the unit callers write.
 *
 * Both fields are optional and mean what their absence says: no `tags` matches
 * every event, no `types` applies no type restriction. Within an item the two
 * combine as `types` ANY-OF **and** `tags` ALL-OF, per the DCB specification.
 *
 * ```typescript
 * { tags: { courseId: "cs-101" } }
 * { tags: { courseId: "cs-101" }, types: [CourseCreated] }
 * ```
 */
export type QueryItem = {
  /** Tags an event must carry ALL of. Omit to match every event. */
  readonly tags?: Record<string, string>
  /** Event types to narrow to — ANY of them matches. Omit for no restriction. */
  readonly types?: ReadonlyArray<EventDescriptor<any> | QualifiedName | string>
}

/**
 * An event query as PLAIN DATA — ONE {@link QueryItem}, or an ARRAY of items
 * ORed together. Used for both sourcing conditions (which events to load) and
 * append conditions (which events define the consistency boundary), because in
 * a DCB model those are the same query.
 *
 * There is no constructor. A query IS the literal you write:
 *
 * ```typescript
 * eventStore.source({ query: { tags: { courseId: "cs-101" } } })
 * eventStore.source({ query: { tags: { courseId }, types: [CourseCreated] } })
 * eventStore.source({ query: [{ tags: { courseId } }, { tags: { studentId } }] })  // OR
 * ```
 *
 * States do NOT write one: `state({ tags })` supplies the tag record and the
 * type half is derived from the fold (see `event-sourcing/state.ts`).
 * This is for the places that genuinely query ad hoc — a direct
 * `eventStore.source(...)`, an explicit `appendCondition` override, a benchmark
 * building a DCB conflict shape by hand.
 */
export type EventQuery = QueryItem | ReadonlyArray<QueryItem>

/**
 * The STORE-facing query representation — an internal shape, produced only by
 * {@link compileQuery} at the store boundary. Callers write an
 * {@link EventQuery} instead; this union is what the in-memory matcher, the
 * Postgres WHERE builder and the gRPC criterion converters switch on.
 *
 * Plain, inert data: every member is a literal a store can also write by hand
 * when it needs a shape no query compiles to (`{ kind: "any-tag" }` is the only
 * such shape today).
 */
export type EventCriteria =
  | TagCriteria
  | TypeRestrictedCriteria
  | EitherCriteria
  | AnyTagCriteria

export type TagCriteria = {
  readonly kind: "tags"
  readonly tags: ReadonlyArray<Tag>
}

export type TypeRestrictedCriteria = {
  readonly kind: "type-restricted"
  readonly inner: TagCriteria | AnyTagCriteria
  readonly types: ReadonlyArray<string>
}

export type EitherCriteria = {
  readonly kind: "either"
  readonly criteria: ReadonlyArray<EventCriteria>
}

export type AnyTagCriteria = {
  readonly kind: "any-tag"
}

/**
 * Normalize an {@link EventQuery} to its ITEMS — the single place the one-item
 * and array shapes converge, and the single place a malformed query is
 * rejected. Everything downstream (the compiler below, the append-condition
 * combination in `event-flush.ts`) works on items and never re-tests the shape.
 */
export function queryItems(query: EventQuery): ReadonlyArray<QueryItem> {
  if (!Array.isArray(query)) {
    if (query === null || typeof query !== "object") {
      throw new Error(
        `An event query must be a query item — { tags?, types? } — or an array of them, but got ${describe(query)}.`,
      )
    }
    return [query as QueryItem]
  }

  const items = query as ReadonlyArray<QueryItem>
  if (items.length === 0) {
    throw new Error(
      "An event query cannot be an EMPTY array: the items of a query are ORed, so zero items " +
      "matches no event at all. Pass the one item you mean — `{ tags: { … } }` — or, to match " +
      "every event, an item with no tags: `{}`.",
    )
  }
  for (const [index, item] of items.entries()) {
    if (item === null || typeof item !== "object" || Array.isArray(item)) {
      throw new Error(
        `Item ${index} of an event query must be a query item — { tags?, types? } — but got ${describe(item)}. ` +
        "An array of queries is NOT nested: write one flat array of items.",
      )
    }
  }
  return items
}

/**
 * Compile a plain-data {@link EventQuery} into the store-facing
 * {@link EventCriteria}. Called ONCE per read, at the store boundary; several
 * items compile to a logical OR.
 */
export function compileQuery(query: EventQuery): EventCriteria {
  const items = queryItems(query)
  if (items.length === 1) return compileItem(items[0]!)
  return { kind: "either", criteria: items.map(compileItem) }
}

function compileItem(item: QueryItem): EventCriteria {
  const inner: TagCriteria = { kind: "tags", tags: tagsFromRecord(item.tags ?? {}) }
  if (!item.types || item.types.length === 0) return inner
  return { kind: "type-restricted", inner, types: item.types.map(resolveTypeName) }
}

/**
 * Resolve an event type to its wire name. Accepts an event descriptor, a
 * qualified name or an already-qualified string.
 */
export function resolveTypeName(t: EventDescriptor<any> | QualifiedName | string): string {
  if (typeof t === "string") return t
  if ("kind" in t && t.kind === "event") return qualifiedNameToString(t.name)
  if ("namespace" in t && "name" in t) return qualifiedNameToString(t as QualifiedName)
  return String(t)
}

/** A value as it should read inside a query error message. */
function describe(value: unknown): string {
  if (value === null) return "null"
  if (Array.isArray(value)) return "an array"
  return typeof value
}
