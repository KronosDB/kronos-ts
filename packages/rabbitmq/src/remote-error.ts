import { AppendConditionError, isConflict, type SerializedError } from "@kronos-ts/core"

/**
 * A handling failure as it goes on the wire: its name, message and stack, and
 * its contract kind when it has one.
 */
export function serializeError(error: unknown): SerializedError {
  const kind = isConflict(error) ? "conflict" : undefined
  if (error instanceof Error) {
    return { name: error.name, message: error.message, stack: error.stack, ...(kind ? { kind } : {}) }
  }
  return { message: String(error), ...(kind ? { kind } : {}) }
}

/**
 * The error a remote failure is rebuilt as: core's contract class when the
 * failure carries a kind, a plain `Error` otherwise. Either keeps the remote
 * name, message and stack.
 */
export function deserializeError(error: SerializedError | undefined, fallback: { name: string; message: string }): Error {
  const message = error?.message ?? fallback.message
  const result = error?.kind === "conflict" ? new AppendConditionError(message) : new Error(message)
  result.name = error?.name ?? fallback.name
  if (error?.stack) result.stack = error.stack
  return result
}
