import type { EventProcessorInfo, EventProcessorInfo_SegmentStatus } from "./generated/control.js"

/**
 * Status of a single event processor, reported to Axon Server.
 * Aligned with Java's EventProcessorInfo proto message.
 */
export type ProcessorStatus = {
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

export type SegmentStatus = {
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
export function toEventProcessorInfo(status: ProcessorStatus): EventProcessorInfo {
  return {
    processorName: status.name,
    mode: status.mode,
    activeThreads: status.activeThreads,
    running: status.running,
    error: status.error,
    segmentStatus: status.segments.map(toSegmentStatus),
    availableThreads: status.availableThreads,
    tokenStoreIdentifier: status.tokenStoreIdentifier,
    isStreamingProcessor: status.isStreamingProcessor,
    loadBalancingStrategyName: "",
  }
}

function toSegmentStatus(seg: SegmentStatus): EventProcessorInfo_SegmentStatus {
  return {
    segmentId: seg.segmentId,
    caughtUp: seg.caughtUp,
    replaying: seg.replaying,
    onePartOf: seg.onePartOf,
    tokenPosition: seg.tokenPosition,
    errorState: seg.errorState,
  }
}

/**
 * Supplier function that returns the current status of all event processors.
 * Registered with the platform connection for periodic reporting.
 */
export type ProcessorStatusSupplier = () => ProcessorStatus[]
