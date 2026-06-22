import type { TokenStore } from "./token-store.js"
import type { UoWRunner } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import type { SequencedDeadLetterQueue } from "./dead-letter-queue.js"

/**
 * Configuration for an individual event processor.
 *
 * All properties are optional — when not specified, the processor
 * uses the global defaults from the messaging configurer.
 *
 * This enables per-processor tuning: different processors can have
 * different batch sizes, token stores, error handlers, etc.
 *
 * ```typescript
 * messagingConfigurer({
 *   eventHandlers: [courseProjection],
 *   processorConfiguration: {
 *     "course-projection": {
 *       batchSize: 50,
 *       initialSegmentCount: 4,
 *       errorHandler: propagatingErrorHandler(),
 *     },
 *   },
 * })
 * ```
 */
/**
 * @deprecated Use `trackingProcessor()` / `subscribingProcessor()` builders
 * with `registerEventProcessor()` instead.
 */
export interface ProcessorConfiguration {
  /** Events per transaction (one UnitOfWork). Default: 1 (Axon parity). */
  batchSize?: number

  /** Number of segments to create on first startup. Default: 1 */
  initialSegmentCount?: number

  /** Polling interval in ms (for TrackingEventProcessor). Default: 500 */
  pollingIntervalMs?: number

  /** Override the token store for this processor. */
  tokenStore?: TokenStore

  /** Override the UnitOfWork runner for this processor. */
  unitOfWorkRunner?: UoWRunner

  /** Override the error handler for this processor. */
  errorHandler?: EventProcessingErrorHandler

  /** Dead letter queue for this processor. */
  deadLetterQueue?: SequencedDeadLetterQueue

  /** How often to extend claims in ms. Default: 5000 */
  claimExtensionThresholdMs?: number

  /** How often to attempt claiming new segments in ms. Default: 5000 */
  tokenClaimIntervalMs?: number
}
