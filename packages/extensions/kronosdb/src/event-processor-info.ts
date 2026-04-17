/**
 * Status of a single event processor, reported to KronosDB.
 */
export interface ProcessorStatus {
  readonly name: string
  readonly running: boolean
  readonly mode: "Tracking" | "Subscribing"
  readonly isStreamingProcessor: boolean
  readonly activeThreads: number
  readonly availableThreads: number
  readonly error: boolean
  readonly errorMessage?: string
  readonly tokenStoreIdentifier: string
  readonly segments: SegmentStatus[]
}

export interface SegmentStatus {
  readonly segmentId: number
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly onePartOf: number
  readonly tokenPosition: bigint
  readonly errorState: string
}

/**
 * Converts a ProcessorStatus to the proto EventProcessorInfo format.
 */
export function toEventProcessorInfo(status: ProcessorStatus): any {
  return {
    processorName: status.name,
    mode: status.mode,
    activeThreads: status.activeThreads,
    running: status.running,
    error: status.error,
    segmentStatus: status.segments.map((seg) => ({
      segmentId: seg.segmentId,
      caughtUp: seg.caughtUp,
      replaying: seg.replaying,
      onePartOf: seg.onePartOf,
      tokenPosition: seg.tokenPosition,
      errorState: seg.errorState,
    })),
    availableThreads: status.availableThreads,
    tokenStoreIdentifier: status.tokenStoreIdentifier,
    isStreamingProcessor: status.isStreamingProcessor,
  }
}

/**
 * Supplier function that returns the current status of all event processors.
 */
export type ProcessorStatusSupplier = () => ProcessorStatus[]
