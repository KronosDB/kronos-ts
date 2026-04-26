/**
 * Re-exports from tracking-token.ts plus the isReplay() helper.
 *
 * Plan 03-04 (CTX-04 / D-29): `isReplay()` is a no-arg permissive ALS read.
 * It returns `false` when called outside an active UnitOfWork (replay-state
 * has only ever been set by the tracking processor; absence == not replaying).
 */
import { resourceKey, type ResourceKey } from "@kronos-ts/common"
import { processingStateStorage } from "./processing-state.js"

// Re-export token types and operations
export {
  type TrackingToken,
  type GlobalSequenceToken,
  type ReplayToken,
  globalSequenceToken,
  replayToken,
  isReplayToken,
  isGlobalSequenceToken,
  advanceToken,
  isReplaying,
  unwrapToken,
  wasProcessedBeforeReset,
} from "./tracking-token.js"

/** Resource key for storing replay state in the active UnitOfWork. */
export const REPLAY_STATE_KEY: ResourceKey<{ replaying: boolean }> =
  resourceKey("replayState")

/**
 * Check if the current processing is a replay.
 * Use this in event handlers to skip side effects during replay.
 *
 * Plan 03-04 (D-29): no-arg permissive ALS read. Returns `false` when called
 * outside an active UnitOfWork.
 *
 * ```
 * on(OrderPlaced, async (event) => {
 *   await db.orders.insert(event)
 *   if (!isReplay()) {
 *     await sendConfirmationEmail(event.email)
 *   }
 * })
 * ```
 */
export function isReplay(): boolean {
  const state = processingStateStorage.getStore()
  if (!state) return false
  const replayState = state.resources.get(REPLAY_STATE_KEY.symbol) as
    | { replaying: boolean }
    | undefined
  return replayState?.replaying === true
}
