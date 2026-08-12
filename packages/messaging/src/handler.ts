import type { z } from "zod"
import type { EventDescriptor, QueryDescriptor } from "./descriptor.js"
import type { EventMessage, QueryMessage } from "./message.js"

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
  readonly evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>
}

/** A paired query descriptor + handler function, with result type on the handler. */
export interface QueryHandlerRegistration<
  Q extends z.ZodType = z.ZodType,
  R = unknown,
> {
  readonly kind: "query-handler"
  readonly descriptor: QueryDescriptor<Q>
  readonly handler: (message: QueryMessage<z.infer<Q>>) => Promise<R> | R
}

// ---------------------------------------------------------------------------
// on() — evolver / query-handler registration
// ---------------------------------------------------------------------------

/**
 * Registration function for evolvers and query handlers.
 * Pairs a descriptor with its function for use in evolve/handler arrays.
 *
 * Usage:
 * - Evolvers:       `on(CourseCreated, (state, { payload }) => ({ ...state, name: payload.name }))`
 * - Query handlers: `on(GetCourse, async ({ payload, metadata }) => { return { ... } })`
 *
 * The overload is resolved by the descriptor kind: event descriptors produce an
 * {@link EvolverRegistration}, query descriptors a {@link QueryHandlerRegistration}.
 *
 * `on()` does **not** produce event handlers — an evolver `(state, event) => state`
 * and an event handler `(event, ctx) => void` are different things. Declare event
 * handlers with {@link import("./event-handler.js").eventHandler} and register them
 * via `.eventHandlers(...)` on a processor builder.
 */

// Overload: evolver (event descriptor + state evolve function)
export function on<S, P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>,
): EvolverRegistration<S, P>

// Overload: query handler
export function on<Q extends z.ZodType, R>(
  descriptor: QueryDescriptor<Q>,
  handler: (message: QueryMessage<z.infer<Q>>) => Promise<R> | R,
): QueryHandlerRegistration<Q, R>

export function on(
  descriptor: EventDescriptor | QueryDescriptor,
  fn: (...args: any[]) => any,
): EvolverRegistration | QueryHandlerRegistration {
  if (descriptor.kind === "event") {
    return {
      kind: "evolver",
      descriptor: descriptor as EventDescriptor,
      evolve: fn,
    }
  }
  return {
    kind: "query-handler",
    descriptor: descriptor as QueryDescriptor,
    handler: fn,
  }
}
