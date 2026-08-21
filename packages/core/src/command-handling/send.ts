import type { InferOutput, StandardSchemaV1 } from "../messaging/standard-schema.js"
import {
  emptyMetadata,
  type Metadata,
  type CommandDescriptor,
  type InferResult,
} from "../messaging/messages.js"
import { generateIdentifier } from "../messaging/identifier.js"
import type { CommandBus } from "./bus.js"
import { requireInvocation, type UnitOfWork } from "../unit-of-work/unit-of-work.js"

// ---------------------------------------------------------------------------
// THE TWO BIRTHS OF A COMMAND, in one file.
//
// A command is born either at the EDGE — `send(bus, D, p)`, a request arriving
// from outside with nothing around it — or INSIDE A HANDLING, as `ctx.send`,
// where a task is already open and owns the instant. Same message, same bus
// call, two lifetimes; the only difference between the two functions below is
// which one of them has a `uow` to stamp from.
// ---------------------------------------------------------------------------

/** `ctx.send` — dispatch a command from inside a handler. */
export type CommandDispatchFunction = <P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  descriptor: CommandDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
) => Promise<unknown>

/**
 * Build the `send` capability for ONE invocation, closed over that
 * invocation's unit of work and command bus.
 *
 * Internal — not exported from the package barrel. Handlers reach the result
 * as `ctx.send`.
 *
 * AF5-aligned semantics: every command is handled in its own fresh UnitOfWork
 * (`commandBus.dispatch` always starts a new one — see `localCommandBus`).
 * The command handler is therefore its own atomic boundary: it loads state,
 * decides, appends events, and commits once — independent of the caller's
 * UnitOfWork.
 *
 * THE METADATA IS EXACTLY WHAT THE CALLER PASSED. This verb carries nothing
 * over from the message the handler is handling — not the correlation pair, not
 * an `actor`, not anything. Core has no carrying mechanism, because carrying is
 * a policy (WHAT jumps from a message to its children) and a policy belongs to
 * a host, not to a primitive. `correlatingHandler(next, from)` is the
 * mechanism: it wraps this verb and overlays whatever `from` said to carry
 * through this very parameter. A caller that names a key wins over that
 * overlay.
 *
 * The `timestamp` is the one thing the verb does settle, from `uow.now()` —
 * the task's instant, so everything one task gives birth to agrees about when.
 */
export function sendFunction(deps: {
  uow: UnitOfWork
  commandBus?: CommandBus
}): CommandDispatchFunction {
  return async (descriptor, payload, metadata) => {
    const uow = requireInvocation(deps.uow)
    const bus = deps.commandBus
    if (!bus) throw new Error("No command bus configured")
    return bus.dispatch({
      kind: "command",
      identifier: generateIdentifier(),
      name: descriptor.name,
      payload,
      metadata: metadata ?? emptyMetadata(),
      timestamp: uow.now(),
    })
  }
}

// ---------------------------------------------------------------------------
// The EDGE verb — build the message, hand it to the bus. Nothing named
// "gateway".
//
// A gateway was an object with one method that closed over a bus. This is the
// same operation with the bus as its first argument: a library function of ALL
// its real arguments, which a host can partially apply itself if it wants the
// bus fixed. There is nothing to construct and nothing to hold.
// ---------------------------------------------------------------------------

/**
 * Build a command message from `descriptor` + `payload` and dispatch it.
 *
 * ```ts
 * const result = await send(commandBus, CreateCourse, { courseId: "cs-101" })
 * //    ^ inferred from the descriptor's `result` schema
 * ```
 *
 * `metadata` is where PER-REQUEST data enters — the actor, the tenant, the
 * trace header. It enters HERE because this is where the message is born, and
 * a message's metadata cannot be reconstructed anywhere downstream.
 *
 * The `timestamp` does NOT enter here, and that is the one asymmetry worth
 * knowing: the BUS owns unit-of-work entry (`localCommandBus` opens a fresh one
 * per dispatch) and a unit of work carries the clock, so the bus stamps the
 * instant from `uow.now()`. This verb establishes nothing and reads no clock.
 */
export async function send<P extends StandardSchemaV1, R extends StandardSchemaV1 | undefined = undefined>(
  bus: CommandBus,
  descriptor: CommandDescriptor<P, R>,
  payload: InferOutput<P>,
  metadata?: Metadata,
): Promise<InferResult<R>> {
  return bus.dispatch({
    kind: "command",
    identifier: generateIdentifier(),
    name: descriptor.name,
    payload,
    metadata: metadata ?? emptyMetadata(),
  }) as Promise<InferResult<R>>
}
