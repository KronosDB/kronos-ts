import type { ProcessorStatus as ProcessorStatusFrame } from "./protocol/kronosdb-protocol.js"

/**
 * What a processor reports about itself, and how it goes on the wire.
 *
 * SIX FIELDS. The wire frame (`ProcessorStatus`) also carries thread counts, a
 * token-store identifier, a "streaming" flag and a per-segment list. kronos
 * processors have none of those — one lane, one cursor — so the wire fields are
 * filled with the only values they can have, here, once, instead of every
 * processor pretending to hold a per-segment map.
 */
export type ProcessorStatus = {
  readonly name: string
  readonly running: boolean
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly position: bigint
  readonly error?: string
}

export function toEventProcessorInfo(status: ProcessorStatus): ProcessorStatusFrame {
  return {
    processorName: status.name,
    mode: "Tracking",
    activeThreads: status.running ? 1 : 0,
    availableThreads: 0,
    running: status.running,
    error: status.error !== undefined,
    streaming: true,
    tokenStoreIdentifier: "",
    segments: [
      {
        segmentId: 0,
        caughtUp: status.caughtUp,
        replaying: status.replaying,
        onePartOf: 1,
        tokenPosition: status.position,
        errorState: status.error ?? "",
      },
    ],
  }
}

export type ProcessorStatusSupplier = () => ProcessorStatus[]
