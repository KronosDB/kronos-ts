import type { z } from "zod"
import type {
  CommandDescriptor,
  EventDescriptor,
  QueryDescriptor,
} from "./descriptor.js"
import type { EventMessage, QueryMessage, SequencedEventMessage } from "./message.js"
import type { EventHandlerContext } from "./handler-context.js"

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
 * A paired event descriptor + handler function, used in handler arrays.
 *
 * The handler is invoked with the sequenced event and an
 * {@link EventHandlerContext}. Handlers declared with fewer parameters (the
 * `on(E, async (message) => ...)` style) remain assignable — the context is
 * simply not observed. Note that `on()` itself keeps its single-parameter
 * handler callback: its evolver overload is distinguished by callback arity,
 * so a context-receiving handler is declared via `eventHandler(...)` instead.
 */
export interface EventHandlerRegistration<P extends z.ZodType = z.ZodType> {
  readonly kind: "event-handler"
  readonly descriptor: EventDescriptor<P>
  readonly handler: (
    message: SequencedEventMessage<z.infer<P>>,
    context: EventHandlerContext,
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
// on() — universal registration
// ---------------------------------------------------------------------------

/**
 * Universal registration function.
 * Pairs a descriptor with its handler for use in handler/evolve arrays.
 *
 * Usage:
 * - Event handlers:  `on(CourseCreated, async ({ payload, metadata }) => { ... })`
 * - Query handlers:  `on(GetCourse, async ({ payload, metadata }) => { return { ... } })`
 * - Evolvers:        `on(CourseCreated, (state, { payload }) => ({ ...state, name: payload.name }))`
 *
 * The overload is resolved by the descriptor kind and how many arguments
 * the callback declares. Event and query handlers receive the full typed message.
 * Evolvers receive the current state plus the full typed event message.
 *
 * In practice the distinction is enforced by the array type:
 * - `evolve: [on(...)]` expects `EvolverRegistration`
 * - `handlers: [on(...)]` expects `EventHandlerRegistration`
 */

// Overload: evolver (event descriptor + state evolve function)
export function on<S, P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>,
): EvolverRegistration<S, P>

// Overload: event handler (event descriptor + handler function)
export function on<P extends z.ZodType>(
  descriptor: EventDescriptor<P>,
  handler: (message: SequencedEventMessage<z.infer<P>>) => Promise<void> | void,
): EventHandlerRegistration<P>

// Overload: query handler
export function on<Q extends z.ZodType, R>(
  descriptor: QueryDescriptor<Q>,
  handler: (message: QueryMessage<z.infer<Q>>) => Promise<R> | R,
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
  evolve: (state: S, message: EventMessage<z.infer<P>>) => S | Promise<S>,
): EvolverRegistration<S, P> {
  return { kind: "evolver", descriptor, evolve }
}
