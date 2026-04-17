/**
 * Re-exports from tracking-token.ts plus the isReplay() ProcessingContext helper.
 *
 * This module exists for the `isReplay()` function which depends on
 * ProcessingContext (and thus ResourceKey), bridging the token layer
 * with the handler context layer.
 */
import { resourceKey, type ResourceKey } from "@kronos-ts/common"

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

/** Resource key for storing replay state in ProcessingContext. */
export const REPLAY_STATE_KEY: ResourceKey<{ replaying: boolean }> =
  resourceKey("replayState")

/**
 * Check if the current processing is a replay.
 * Use this in event handlers to skip side effects during replay.
 *
 * ```
 * on(OrderPlaced, async (event, ctx) => {
 *   // Always update projection
 *   await db.orders.insert(event)
 *   // Skip email during replay
 *   if (!isReplay(ctx.processingContext!)) {
 *     await sendConfirmationEmail(event.email)
 *   }
 * })
 * ```
 */
export function isReplay(ctx: { get<T>(key: ResourceKey<T>): T | undefined }): boolean {
  const state = ctx.get(REPLAY_STATE_KEY)
  return state?.replaying === true
}
