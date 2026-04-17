import type { z } from "zod"
import type { EventCriteria, EventMessage } from "@kronos-ts/messaging"
import type { EvolverRegistration } from "@kronos-ts/messaging"

/**
 * A named record mapping field names to Zod schemas.
 * Used to define entity IDs with explicit field names.
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

/**
 * Lifecycle hooks for entity state transitions.
 */
export interface EntityLifecycle<Id = unknown, S = unknown> {
  /** Called when the first event transitions the entity from initial state. */
  onCreate?: (state: S, id: Id) => void | Promise<void>
  /** Called when the entity transitions to a deleted state. */
  onDelete?: (state: S, id: Id) => void | Promise<void>
  /** Called after each evolver application when state changes. */
  onStateChange?: (from: S, to: S, event: EventMessage, id: Id) => void | Promise<void>
  /** Predicate that detects deleted entity state. */
  isDeleted?: (state: S) => boolean
}

/**
 * An entity module — a self-contained definition of an event-sourced entity.
 *
 * The `Id` type is always a named record (e.g., `{ courseId: string }`),
 * enforced at compile time by requiring an {@link IdSchema} definition.
 * This ensures field names are always available for criteria, evolvers,
 * and the initial function.
 */
export interface EntityModule<
  Id = unknown,
  S = unknown,
> {
  readonly kind: "entity-module"
  readonly name: string
  /** The ID schema — maps field names to Zod types. */
  readonly idSchema: IdSchema
  readonly create: (id: Id) => S
  readonly criteria: (id: Id) => EventCriteria
  readonly evolvers: ReadonlyArray<EvolverRegistration<S, any>>
  readonly lifecycle?: EntityLifecycle<Id, S>
}

/**
 * Defines an event-sourced entity module.
 *
 * The `id` parameter must be a named record mapping field names to Zod types.
 * A bare Zod type (e.g., `z.string()`) will not compile — you must name
 * the field (e.g., `{ courseId: z.string() }`).
 *
 * The state type is inferred from the `initial` function's return type —
 * no separate type definition needed.
 *
 * ```typescript
 * const CourseEntity = eventSourcedEntity({
 *   name: "Course",
 *   id: { courseId: z.string() },
 *   initial: () => ({ created: false, name: "", capacity: 0 }),
 *   criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
 *   evolve: [
 *     on(CourseCreated, (state, event) => ({ ...state, created: true })),
 *   ],
 * })
 * ```
 */
export function eventSourcedEntity<IS extends IdSchema, S>(def: {
  name: string
  id: IS
  initial: (id: InferIdFromSchema<IS>) => S
  criteria: (id: InferIdFromSchema<IS>) => EventCriteria
  evolve: Array<EvolverRegistration<S, any>>
  lifecycle?: EntityLifecycle<InferIdFromSchema<IS>, S>
}): EntityModule<InferIdFromSchema<IS>, S>

export function eventSourcedEntity<IS extends IdSchema, S>(def: {
  name: string
  id: IS
  initial: (id: InferIdFromSchema<IS>) => S
  criteria: (id: InferIdFromSchema<IS>) => EventCriteria
  evolve: Array<EvolverRegistration<S, any>>
  lifecycle?: EntityLifecycle<InferIdFromSchema<IS>, S>
}): EntityModule<InferIdFromSchema<IS>, S> {
  return {
    kind: "entity-module",
    name: def.name,
    idSchema: def.id,
    create: def.initial,
    criteria: def.criteria,
    evolvers: def.evolve,
    lifecycle: def.lifecycle,
  }
}
