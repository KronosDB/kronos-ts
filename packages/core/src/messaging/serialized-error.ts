import type { FailureKind } from "./failures.js"

/**
 * A failure reduced to the fields that survive a wire hop.
 *
 * Transports serialize a thrown `Error` into this on the way out and rebuild an
 * `Error` from it on the way in. `kind` is the failure's contract kind, when it
 * has one, and the error rebuilt from it carries the same kind, so `isConflict`
 * answers on the caller's side as it would have on the handler's. It lives beside the other primitives because
 * every transport needs the same shape and none of them should have to invent
 * it — but core itself never produces one: nothing in-process has to flatten an
 * error to cross a boundary.
 */
export type SerializedError = {
  readonly name?: string
  readonly message: string
  readonly stack?: string
  readonly kind?: FailureKind
}
