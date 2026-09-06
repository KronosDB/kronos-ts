import type { EventProcessorInfo } from "./generated/control.js"

/**
 * What a processor reports about itself. SIX FIELDS — see the kronosdb
 * package's twin for why the wire's thread counts, token-store identifier and
 * per-segment list are filled with constants here rather than carried.
 */
export type ProcessorStatus = {
  readonly name: string
  readonly running: boolean
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly position: bigint
  readonly error?: string
}

export function toEventProcessorInfo(status: ProcessorStatus): EventProcessorInfo {
  return {
    processorName: status.name,
    mode: "Tracking",
    activeThreads: status.running ? 1 : 0,
    running: status.running,
    error: status.error !== undefined,
    segmentStatus: [
      {
        segmentId: 0,
        caughtUp: status.caughtUp,
        replaying: status.replaying,
        onePartOf: 1,
        tokenPosition: status.position,
        errorState: status.error ?? "",
      },
    ],
    availableThreads: 0,
    tokenStoreIdentifier: "",
    isStreamingProcessor: true,
    loadBalancingStrategyName: "",
  }
}

export type ProcessorStatusSupplier = () => ProcessorStatus[]
