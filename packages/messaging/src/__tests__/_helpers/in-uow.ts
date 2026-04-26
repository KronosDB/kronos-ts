/**
 * Shared test helper: run a function inside an active UnitOfWork (ALS scope).
 *
 * Plan 03-04 (CTX-04): replaces ad-hoc `inUoW` definitions previously
 * duplicated across test files. Mirrors the production runner's ALS-binding
 * without the lifecycle phase machinery — sufficient for tests that only
 * exercise resource accessors / permissive ALS reads.
 */
import { emptyMetadata, type Metadata } from "@kronos-ts/common"
import {
  processingStateStorage,
  createInitialProcessingState,
} from "../../processing-state.js"

export function inUoW<R>(
  fn: () => R | Promise<R>,
  metadata: Metadata = emptyMetadata(),
): Promise<R> {
  return processingStateStorage.run(
    createInitialProcessingState(metadata),
    async () => fn(),
  )
}
