/**
 * A failure reduced to the three fields that survive a wire hop.
 *
 * Transports serialize a thrown `Error` into this on the way out and rebuild an
 * `Error` from it on the way in. It lives beside the other primitives because
 * every transport needs the same shape and none of them should have to invent
 * it — but core itself never produces one: nothing in-process has to flatten an
 * error to cross a boundary.
 */
export type SerializedError = {
  readonly name?: string
  readonly message: string
  readonly stack?: string
}
