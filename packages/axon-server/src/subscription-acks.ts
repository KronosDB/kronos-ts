import type { InstructionAck } from "./generated/common.js"
import { mapErrorCode } from "./errors.js"

/**
 * The subscribe instructions one bus stream has sent that Axon Server has not
 * answered yet.
 *
 * Axon Server answers every instruction that carries an `instructionId` with an
 * `InstructionAck` on the same stream, once the node this client is connected
 * to has registered the handler.
 */
export type SubscriptionAcks = {
  /** A subscribe instruction went out under this instruction id. */
  sent(instructionId: string): void
  /** An acknowledgement arrived. One for an instruction that is not pending is ignored. */
  received(ack: InstructionAck): void
  /** The stream was replaced: what the old one sent will not be answered. */
  reset(): void
  /**
   * Resolves when no subscribe instruction is unanswered, at once if none is.
   * Rejects when the server refuses one.
   */
  acknowledged(): Promise<void>
}

export function subscriptionAcks(): SubscriptionAcks {
  const pending = new Set<string>()
  const waiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = []

  return {
    sent(instructionId) {
      pending.add(instructionId)
    },

    received(ack) {
      if (!pending.delete(ack.instructionId)) return
      if (!ack.success) {
        const error = mapErrorCode(
          ack.error?.errorCode ?? "",
          ack.error?.message || "Axon Server refused a handler subscription",
        )
        for (const waiter of waiters.splice(0)) waiter.reject(error)
        return
      }
      if (pending.size === 0) for (const waiter of waiters.splice(0)) waiter.resolve()
    },

    reset() {
      pending.clear()
    },

    acknowledged() {
      if (pending.size === 0) return Promise.resolve()
      return new Promise<void>((resolve, reject) => waiters.push({ resolve, reject }))
    },
  }
}
