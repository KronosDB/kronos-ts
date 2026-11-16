import type { ProcessorStatus as ProcessorStatusFrame } from "./protocol/kronosdb-protocol.js"

/** One segment this node holds, as the admin UI shows it. */
export type SegmentReport = {
  readonly segment: number
  /** How many segments of this size make up the whole processor: `mask + 1`. */
  readonly onePartOf: number
  readonly position: bigint
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly error?: string
}

/**
 * What a processor reports about itself, and how it goes on the wire.
 *
 * `segments` are the segments this node holds. A processor that does not say
 * goes on the wire as one whole segment carrying its own position. The wire's
 * thread counts, token-store identifier and "streaming" flag are filled here.
 */
export type ProcessorStatus = {
  readonly name: string
  readonly running: boolean
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly position: bigint
  readonly error?: string
  readonly segments?: ReadonlyArray<SegmentReport>
}

export function toEventProcessorInfo(status: ProcessorStatus): ProcessorStatusFrame {
  const segments = status.segments ?? [
    {
      segment: 0,
      onePartOf: 1,
      position: status.position,
      caughtUp: status.caughtUp,
      replaying: status.replaying,
      error: status.error,
    },
  ]
  return {
    processorName: status.name,
    mode: "Tracking",
    activeThreads: status.running ? segments.length : 0,
    availableThreads: 0,
    running: status.running,
    error: status.error !== undefined,
    streaming: true,
    tokenStoreIdentifier: "",
    segments: segments.map((s) => ({
      segmentId: s.segment,
      caughtUp: s.caughtUp,
      replaying: s.replaying,
      onePartOf: s.onePartOf,
      tokenPosition: s.position,
      errorState: s.error ?? "",
    })),
  }
}

export type ProcessorStatusSupplier = () => ProcessorStatus[]
