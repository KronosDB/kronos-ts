import type { Tag, QualifiedName } from "@kronos-ts/common"
import { qualifiedNameToString, tagsFromRecord } from "@kronos-ts/common"
import type { EventDescriptor } from "./descriptor.js"

/**
 * Criteria for selecting events from the event store.
 * Used for both sourcing conditions (which events to load) and
 * append conditions (which events define the consistency boundary).
 *
 * Criteria are composable via `or()` and restrictable via `ofTypes()`.
 */
export type EventCriteria =
  | TagCriteria
  | TypeRestrictedCriteria
  | EitherCriteria
  | AnyTagCriteria

export interface TagCriteria {
  readonly kind: "tags"
  readonly tags: ReadonlyArray<Tag>
}

export interface TypeRestrictedCriteria {
  readonly kind: "type-restricted"
  readonly inner: TagCriteria | AnyTagCriteria
  readonly types: ReadonlyArray<string>
}

export interface EitherCriteria {
  readonly kind: "either"
  readonly criteria: ReadonlyArray<EventCriteria>
}

export interface AnyTagCriteria {
  readonly kind: "any-tag"
}

/**
 * A tag or any-tag criteria that allows further restriction by event types.
 */
export type RestrictableEventCriteria = (TagCriteria | AnyTagCriteria) & {
  /**
   * Restrict matched events to the given types.
   * Accepts event descriptors or qualified name strings.
   */
  ofTypes(...types: Array<EventDescriptor<any> | QualifiedName | string>): EventCriteria
}

function resolveTypeName(t: EventDescriptor<any> | QualifiedName | string): string {
  if (typeof t === "string") return t
  if ("kind" in t && t.kind === "event") return qualifiedNameToString(t.name)
  if ("namespace" in t && "name" in t) return qualifiedNameToString(t as QualifiedName)
  return String(t)
}

function makeRestrictable(criteria: TagCriteria | AnyTagCriteria): RestrictableEventCriteria {
  return Object.assign(criteria, {
    ofTypes(...types: Array<EventDescriptor<any> | QualifiedName | string>): EventCriteria {
      return {
        kind: "type-restricted" as const,
        inner: criteria,
        types: types.map(resolveTypeName),
      }
    },
  })
}

export const EventCriteria = {
  /**
   * Match events having all the specified tags.
   *
   * Accepts individual Tag objects or a record of key-value pairs:
   * ```typescript
   * EventCriteria.havingTags({ courseId: id.courseId })
   * EventCriteria.havingTags(tag("courseId", id.courseId))
   * ```
   */
  havingTags(...args: Tag[] | [Record<string, string>]): RestrictableEventCriteria {
    if (args.length === 1 && typeof args[0] === "object" && !("key" in args[0])) {
      return makeRestrictable({ kind: "tags", tags: tagsFromRecord(args[0] as Record<string, string>) })
    }
    return makeRestrictable({ kind: "tags", tags: args as Tag[] })
  },

  /**
   * Match events having any tag (i.e., all tagged events).
   */
  havingAnyTag(): RestrictableEventCriteria {
    return makeRestrictable({ kind: "any-tag" })
  },

  /**
   * Match events matching any of the given criteria (logical OR).
   */
  either(...criteria: EventCriteria[]): EventCriteria {
    return { kind: "either", criteria }
  },
} as const

// ---------------------------------------------------------------------------
// Standalone shorthand functions
// ---------------------------------------------------------------------------

/**
 * Match events having all the specified tags.
 *
 * Shorthand for `EventCriteria.havingTags()`. Supports `.ofTypes()` chaining.
 *
 * ```typescript
 * tags({ courseId: id.courseId })
 * tags({ courseId: id.courseId }).ofTypes(CourseCreated, CourseCapacityChanged)
 * ```
 */
export function tags(...args: Tag[] | [Record<string, string>]): RestrictableEventCriteria {
  return EventCriteria.havingTags(...args)
}

/**
 * Match events having any tag.
 *
 * Shorthand for `EventCriteria.havingAnyTag()`. Supports `.ofTypes()` chaining.
 */
export function anyTag(): RestrictableEventCriteria {
  return EventCriteria.havingAnyTag()
}

/**
 * Match events matching any of the given criteria (logical OR).
 *
 * Shorthand for `EventCriteria.either()`.
 *
 * ```typescript
 * either(
 *   tags({ courseId: id.courseId }),
 *   tags({ studentId: id.studentId }).ofTypes(StudentSubscribed),
 * )
 * ```
 */
export function either(...criteria: EventCriteria[]): EventCriteria {
  return EventCriteria.either(...criteria)
}
