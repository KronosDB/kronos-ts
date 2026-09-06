/**
 * What a processor reports about itself, and how it goes on the wire.
 *
 * SIX FIELDS. The wire message (`EventProcessorInfo`, shared with Axon's
 * control protocol) also carries thread counts, a token-store identifier, a
 * "streaming" flag and a per-segment list. kronos processors have none of
 * those — one lane, one cursor — so the wire fields are filled with the only
 * values they can have, here, once, instead of every processor pretending to
 * hold a per-segment map.
 */
export type ProcessorStatus = {
  readonly name: string
  readonly running: boolean
  readonly caughtUp: boolean
  readonly replaying: boolean
  readonly position: bigint
  readonly error?: string
}

export function toEventProcessorInfo(status: ProcessorStatus): any {
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
  }
}

export type ProcessorStatusSupplier = () => ProcessorStatus[]
