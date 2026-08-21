/**
 * EventCriteria → SQL WHERE clause builder.
 *
 * Maps the discriminated-union criteria from @kronos-ts/core into a
 * parameterised WHERE fragment + parameter array. The caller (Plan 04
 * source() + the append SP body) splices this into a larger query.
 *
 * Tag semantics: `@>` (contains-all). NEVER `&&` (overlap). The reference
 * is packages/eventsourcing/src/in-memory-event-store.ts:matchesTags —
 * `criteria.tags.every(requiredTag => event.tags.some(...))` is exactly
 * the meaning of `tags @> $required`.
 *
 * Tag encoding: each `{key, value}` is serialised as `${key}${value}`.
 * Stored events use the same encoding (Plan 04 Task 3 — appendEvents flattens
 * tags via the same scheme) so `@>` works literally against text[].
 */

import type { EventCriteria } from "@kronos-ts/core"

export type CriteriaSQL = {
  /** SQL WHERE fragment (no leading "WHERE"). Always truthy — empty
   *  criteria collapse to `"true"`. */
  readonly where: string
  /** Parameters in $-positional order. */
  readonly params: ReadonlyArray<unknown>
  /** Next available $N index for chaining into a larger query. */
  readonly nextParamIndex: number
}

export const TAG_DELIMITER = "" // ASCII Unit Separator (U+001F) — prevents key/value boundary collisions in encoded tag strings

export function encodeTag(key: string, value: string): string {
  return `${key}${TAG_DELIMITER}${value}`
}

export function buildCriteriaWhere(
  criteria: EventCriteria,
  startIndex: number,
): CriteriaSQL {
  switch (criteria.kind) {
    case "any-tag":
      return {
        where: "cardinality(tags) > 0",
        params: [],
        nextParamIndex: startIndex,
      }

    case "tags": {
      if (criteria.tags.length === 0) {
        // Empty contains-all matches everything (the in-memory store's
        // `tags.every(...)` over an empty array is vacuously true).
        return { where: "true", params: [], nextParamIndex: startIndex }
      }
      const encoded = criteria.tags.map((t) => encodeTag(t.key, t.value))
      return {
        where: `tags @> $${startIndex}::text[]`,
        params: [encoded],
        nextParamIndex: startIndex + 1,
      }
    }

    case "type-restricted": {
      const inner = buildCriteriaWhere(criteria.inner, startIndex)
      const typeParam = inner.nextParamIndex
      return {
        where: `(${inner.where}) AND type = ANY($${typeParam}::text[])`,
        params: [...inner.params, criteria.types],
        nextParamIndex: typeParam + 1,
      }
    }

    case "either": {
      const parts: string[] = []
      const params: unknown[] = []
      let next = startIndex
      for (const sub of criteria.criteria) {
        const built = buildCriteriaWhere(sub, next)
        parts.push(`(${built.where})`)
        params.push(...built.params)
        next = built.nextParamIndex
      }
      return {
        where: parts.join(" OR "),
        params,
        nextParamIndex: next,
      }
    }
  }
}
