import type { z } from "zod"
import type { Metadata } from "@kronos-ts/common"
import type {
  CommandDescriptor,
  EventDescriptor,
  QueryDescriptor,
} from "./descriptor.js"

// ---------------------------------------------------------------------------
// Handler context shapes — what each handler type receives
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Context function types — flat functions, no wrapper objects
// ---------------------------------------------------------------------------

/** Load event-sourced entity state. State type inferred from the entity module. */
export interface LoadFunction {
  <Id, S>(entity: { kind: "entity-module"; name: string; create: (id: Id) => S }, id: Id): Promise<S>
  <S>(entity: { name: string }, id: unknown): Promise<S>
}

/** Append events, buffered until commit. */
export interface AppendFunction {
  <P extends z.ZodType>(event: EventDescriptor<P>, payload: z.infer<P>): void
  <P extends z.ZodType>(
    event: EventDescriptor<P>,
    payload: z.infer<P>,
    metadata: Metadata,
  ): void
}

/** Send a command within the current processing context. */
export interface SendFunction {
  <P extends z.ZodType>(
    command: CommandDescriptor<P>,
    payload: z.infer<P>,
  ): Promise<unknown>
}

/** Emit updates to active subscription queries. */
export interface EmitUpdateFunction {
  <Q extends z.ZodType>(
    query: QueryDescriptor<Q>,
    filter: (query: z.infer<Q>) => boolean,
    update: unknown,
  ): void
}

// ---------------------------------------------------------------------------
// Handler context shapes — what each handler type receives
// ---------------------------------------------------------------------------

/**
 * Context available to command handlers.
 *
 * Plan 03-04 (CTX-04 / D-34): the `processingContext` field is gone.
 * Lifecycle / resource access uses module-level accessors from
 * `processing-state.js` (e.g. `getResource`, `setResource`, `on`,
 * `onError`, `whenComplete`).
 */
export interface CommandHandlerContext {
  /** Load event-sourced entity state. */
  load: LoadFunction
  /** Append events (buffered until commit). */
  append: AppendFunction
  /** Message metadata (includes correlationId etc). */
  metadata: Metadata
}

/**
 * Context available to event handlers.
 *
 * Plan 03-04 (CTX-04 / D-34): the `processingContext` field is gone.
 */
export interface EventHandlerContext {
  /** Load event-sourced entity state. */
  load: LoadFunction
  /** Send a command within the current processing context. */
  send: SendFunction
  /** Emit updates to active subscription queries. */
  emitUpdate: EmitUpdateFunction
  /** Message metadata. */
  metadata: Metadata
}

/**
 * Context available to query handlers.
 *
 * Plan 03-04 (CTX-04 / D-34): the `processingContext` field is gone.
 */
export interface QueryHandlerContext {
  /** Message metadata. */
  metadata: Metadata
}

// ---------------------------------------------------------------------------
// Registration types
// ---------------------------------------------------------------------------

/** A paired event descriptor + handler function, used in handler arrays. */
export interface EventHandlerRegistration<P extends z.ZodType = z.ZodType> {
  readonly kind: "event-handler"
  readonly descriptor: EventDescriptor<P>
  readonly handler: (
    event: z.infer<P>,
    context: EventHandlerContext,
  ) => Promise<void> | void
}

/**
 * A paired event descriptor + evolver function, used in entity evolve arrays.
 *
 * Evolvers can be sync or async. Async evolvers don't block the event loop
 * during state reconstruction.
 *
 * Aligned with AF5's {@code @EventSourcingHandler} which goes through the
 * same handler pipeline as {@code @EventHandler}.
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
    context: QueryHandlerContext,
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
  handler: (event: z.infer<P>, context: EventHandlerContext) => Promise<void> | void,
): EventHandlerRegistration<P>

// Overload: query handler
export function on<Q extends z.ZodType, R>(
  descriptor: QueryDescriptor<Q>,
  handler: (
    query: z.infer<Q>,
    context: QueryHandlerContext,
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
