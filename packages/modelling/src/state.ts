import type { z } from "zod"
import { on } from "@kronos-ts/messaging"
import type {
  EventCriteria,
  EventMessage,
  EvolverRegistration,
  EventDescriptor,
} from "@kronos-ts/messaging"

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
 * The state-scoped `on` handed to an `evolve` builder.
 *
 * It is the same `on` as the top-level export, but with the state type `S`
 * already bound. Because `S` is fixed before the evolver callbacks are
 * checked, each callback receives `state: S` contextually — no `(s: S)`
 * annotation, and a wrong return is still rejected against `S`.
 */
export type EvolverBuilder<S> = <P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>,
) => EvolverRegistration<S, P>

/**
 * A state module — a self-contained definition of state sourced from events.
 *
 * The `Id` type is always a named record (e.g., `{ courseId: string }`),
 * enforced at compile time by requiring an {@link IdSchema} definition.
 * This ensures field names are always available for criteria, evolvers,
 * and the initial function.
 */
export interface StateModule<
  Id = unknown,
  S = unknown,
> {
  readonly kind: "state-module"
  readonly name: string
  /** The ID schema — maps field names to Zod types. */
  readonly idSchema: IdSchema
  readonly create: (id: Id) => S
  readonly criteria: (id: Id) => EventCriteria
  readonly evolvers: ReadonlyArray<EvolverRegistration<S, any>>
  readonly lifecycle?: StateLifecycle<Id, S>
}

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
 * Evolvers are registered through a builder: `evolve: (on) => [...]`, where
 * `on` is bound to the state type. Because `S` is fixed by `initial` before
 * the evolvers are checked, each callback receives `state: S` contextually —
 * no `(s: S)` annotation, and a wrong return is still rejected against `S`.
 * When `initial` under-specifies `S` (empty arrays, unions), annotate the one
 * source of truth: `initial: (id): CourseState => (...)`.
 *
 * ```typescript
 * const Course = state({
 *   name: "Course",
 *   id: { courseId: z.string() },
 *   initial: () => ({ created: false, name: "", capacity: 0 }),
 *   criteria: (id) => EventCriteria.havingTags({ courseId: id.courseId }),
 *   evolve: (on) => [
 *     on(CourseCreated, (s, { payload: event }) => ({ ...s, created: true })),
 *   ],
 * })
 * ```
 */
export function state<IS extends IdSchema, S>(def: {
  name: string
  id: IS
  initial: (id: InferIdFromSchema<IS>) => S
  criteria: (id: InferIdFromSchema<IS>) => EventCriteria
  evolve: (on: EvolverBuilder<S>) => Array<EvolverRegistration<S, any>>
  lifecycle?: StateLifecycle<InferIdFromSchema<IS>, S>
}): StateModule<InferIdFromSchema<IS>, S> {
  return {
    kind: "state-module",
    name: def.name,
    idSchema: def.id,
    create: def.initial,
    criteria: def.criteria,
    evolvers: def.evolve(on as unknown as EvolverBuilder<S>),
    lifecycle: def.lifecycle,
  }
}
