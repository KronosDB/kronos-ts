import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type {
  CommandDescriptor,
  EventDescriptor,
  QueryDescriptor,
} from "./descriptor.js"

// ---------------------------------------------------------------------------
// Handler context shapes — DELETED (Plan 04-02, D-41)
// CommandHandlerContext / EventHandlerContext / QueryHandlerContext removed.
// LoadFunction / AppendFunction / SendFunction / EmitUpdateFunction removed.
// Consumers import load/append from @kronos-ts/eventsourcing and
// send/dispatch/emitUpdate from @kronos-ts/messaging directly.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Registration types
// ---------------------------------------------------------------------------

/** A paired event descriptor + handler function, used in handler arrays. */
export interface EventHandlerRegistration<P extends z.ZodType = z.ZodType> {
  readonly kind: "event-handler"
  readonly descriptor: EventDescriptor<P>
  readonly handler: (
    event: z.infer<P>,
    metadata: Metadata,
  ) => Promise<void> | void
}

/**
 * A paired event descriptor + evolver function, used in entity evolve arrays.
 *
 * Evolvers can be sync or async. Async evolvers don't block the event loop
 * during state reconstruction.
 */
export interface EvolverRegistration<
  S = unknown,
  P extends z.ZodType = z.ZodType,
> {
  readonly kind: "evolver"
  readonly descriptor: EventDescriptor<P>
  readonly evolve: (state: S, event: z.infer<P>, id: unknown) => S | Promise<S>
}

/** A paired query descriptor + handler function, with result type on the handler. */
export interface QueryHandlerRegistration<
  Q extends z.ZodType = z.ZodType,
  R = unknown,
> {
  readonly kind: "query-handler"
  readonly descriptor: QueryDescriptor<Q>
  readonly handler: (
    query: z.infer<Q>,
    metadata: Metadata,
  ) => Promise<R> | R
}

// ---------------------------------------------------------------------------
// on() — universal registration
// ---------------------------------------------------------------------------

/**
 * Universal registration function.
 * Pairs a descriptor with its handler for use in handler/evolve arrays.
 *
 * Usage:
 * - Event handlers:  `on(CourseCreated, async (event, ctx) => { ... })`
 * - Query handlers:  `on(GetCourse, async (query, ctx) => { return { ... } })`
 * - Evolvers:        `on(CourseCreated, (state, event) => ({ ...state, name: event.name }))`
 *
 * The overload is resolved by the descriptor kind and how many arguments
 * the callback declares. Evolvers receive `(state, event)` or `(state, event, id)`,
 * while event handlers receive `(event, context)`.
 *
 * In practice the distinction is enforced by the array type:
 * - `evolve: [on(...)]` expects `EvolverRegistration`
 * - `handlers: [on(...)]` expects `EventHandlerRegistration`
 */

// Overload: evolver (event descriptor + state evolve function)
export function on<S, P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  evolve: (state: S, event: z.infer<P>, id: unknown) => S | Promise<S>,
): EvolverRegistration<S, P>

// Overload: event handler (event descriptor + handler function)
export function on<P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  handler: (event: z.infer<P>, metadata: Metadata) => Promise<void> | void,
): EventHandlerRegistration<P>

// Overload: query handler
export function on<Q extends z.ZodType, R>(
  descriptor: QueryDescriptor<Q>,
  handler: (
    query: z.infer<Q>,
    metadata: Metadata,
  ) => Promise<R> | R,
): QueryHandlerRegistration<Q, R>

export function on(
  descriptor: EventDescriptor | QueryDescriptor,
  handler: (...args: any[]) => any,
): EventHandlerRegistration | EvolverRegistration | QueryHandlerRegistration {
  if (descriptor.kind === "event") {
    // For event descriptors, the same `on()` call is used for both event handlers
    // and evolvers. We return an object that satisfies both interfaces — the
    // consumer's array type (evolve: EvolverRegistration[] vs handlers: EventHandlerRegistration[])
    // enforces correct usage at compile time.
    return {
      kind: "event-handler" as any,
      descriptor: descriptor as EventDescriptor,
      handler,
      evolve: handler,
    }
  }
  return {
    kind: "query-handler",
    descriptor: descriptor as QueryDescriptor,
    handler,
  }
}

/**
 * @deprecated Use `on()` instead. `onEvent` is kept for backward compatibility.
 */
export function onEvent<S, P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  evolve: (state: S, event: z.infer<P>, id: unknown) => S | Promise<S>,
): EvolverRegistration<S, P> {
  return { kind: "evolver", descriptor, evolve }
}
