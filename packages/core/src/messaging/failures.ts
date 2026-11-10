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
 *
 * Any other failure carries no kind: it is a bug, a misconfiguration or
 * something for an operator, and retrying it does not help.
 *
 * ASK WITH THE GUARD, NOT `instanceof`. Each store package pins the core it
 * was built against, so an app can hold two copies of core, and an error
 * built by one is not an `instanceof` the other's class. The guard reads the
 * `kind` field, which any copy, and an error rebuilt from a wire code, carries
 * the same way. The classes exist so that a store cannot forget the field.
 */
export type FailureKind = "conflict"

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
