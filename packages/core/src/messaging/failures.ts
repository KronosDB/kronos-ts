/**
 * THE FAILURES A CALLER HANDLES DIFFERENTLY, and nothing else.
 *
 * Every event store, and every bus that carries a handler's failure back to
 * its caller, reports these the same way, whatever it runs on:
 *
 * - `conflict`: the append condition was violated. Another writer changed
 *   what the decision read between its load and its append. Nothing was
 *   written. Load again and decide again: an edge answers 409, a processor
 *   retries.
 * - `unavailable`: the store or bus could not serve the request now, and the
 *   same request later may succeed: unreachable, failing over, shutting down,
 *   over capacity, or timed out. Retry later: an edge answers 503, a
 *   processor backs off. A request that was sent and never answered may have
 *   taken effect, so a resent command must recognise its own earlier attempt.
 *
 * Any other failure carries no kind: it is a bug, a misconfiguration or
 * something for an operator, and retrying it does not help.
 *
 * ACROSS A BUS THE KIND IS KEPT. A handler that fails with a kind on another
 * instance fails the caller's `send` or `query` with the same kind: each
 * transport sends it in its own vocabulary (a protocol error code, or
 * `SerializedError.kind`) and rebuilds a contract error from it.
 *
 * ASK WITH THE GUARD, NOT `instanceof`. Each store package pins the core it
 * was built against, so an app can hold two copies of core, and an error
 * built by one is not an `instanceof` the other's class. The guard reads the
 * `kind` field, which any copy, and an error rebuilt from a wire code, carries
 * the same way. The classes exist so that a store cannot forget the field.
 */
export type FailureKind = "conflict" | "unavailable"

/**
 * The append condition was violated: nothing was written. Thrown by every
 * store, directly or as a subclass that adds the store's own detail.
 */
export class AppendConditionError extends Error {
  readonly kind: "conflict" = "conflict"

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = "AppendConditionError"
  }
}

/** Whether `error` is a conflict: an append whose condition was violated, here or on another instance. */
export function isConflict(error: unknown): error is Error & { readonly kind: "conflict" } {
  return typeof error === "object" && error !== null && (error as { kind?: unknown }).kind === "conflict"
}

/**
 * The store or bus could not serve the request now; the same request later may
 * succeed. A store whose own errors have a hierarchy of their own gives those
 * the `unavailable` kind instead of extending this.
 */
export class UnavailableError extends Error {
  readonly kind: "unavailable" = "unavailable"

  constructor(message: string, options?: ErrorOptions) {
    super(message, options)
    this.name = "UnavailableError"
  }
}

/** Whether `error` says to retry later: the store or bus could not serve the request now. */
export function isUnavailable(error: unknown): error is Error & { readonly kind: "unavailable" } {
  return typeof error === "object" && error !== null && (error as { kind?: unknown }).kind === "unavailable"
}
