/**
 * The Axon Server PLATFORM CONTROL PLANE — remote administration, extracted
 * out of the backend.
 *
 * This is not persistence and it is not transport. It is the pair of duties
 * Axon Server's admin surface needs from a client:
 *
 *   1. inbound — Axon Server pushes processor instructions (pause-processor,
 *      start-processor, release-segment, split-segment, merge-segment) which
 *      have to be routed to the live processor of that name;
 *   2. outbound — the client periodically reports each processor's status so
 *      the Axon Dashboard can render it.
 *
 * It lived inside the backend's `start()` only because it shares the gRPC
 * connection with the data path. It is now separate and OPT-IN: a service that
 * nobody administers remotely simply never builds one, and
 * `axonServerConnection()` is left with an argument-less `start()` readiness
 * barrier and nothing else.
 *
 * ```ts
 * const axon = await axonServerConnection({ ... })
 * const app  = kronos({ commandHandlers, queryHandlers, eventHandlers })
 * await axon.start()                                  // data path only
 *
 * // opt in to remote administration
 * const control = await axonServerControlPlane(axon, app.processors.values())
 * // …
 * await app.stop(); await control.close(); await axon.close()
 * ```
 *
 * ORDERING INVARIANT (the reason this is an async factory rather than a
 * constructor plus a separate `start()`): the instruction handler and the
 * status supplier MUST both be registered BEFORE `platform.start()` sends the
 * `register` frame. An instruction that arrives in the gap between `start()`
 * and `onInstruction(...)` is dropped on the floor, and a status request that
 * arrives before `registerProcessorStatusSupplier(...)` finds no supplier and
 * reports the client as having no processors at all. Register, register, then
 * start — all three inside one function, so there is no call order for a
 * caller to get wrong.
 */
import type { AxonServerPlatformSource } from "./connection.js"
import type { ProcessorStatus, SegmentStatus } from "./event-processor-info.js"

/**
 * A processor Axon Server is allowed to observe and control.
 *
 * The container version reached for `app.processors()` and cast the result to
 * `any` before poking at `start` / `stop` / `releaseSegment` / … — this is that
 * cast, written down. Both `TrackingEventProcessor` and
 * `StreamingEventProcessor` satisfy it structurally; anything else that can
 * name itself and answer some of these calls does too. Every member past the
 * name is optional because Axon Server asks for things a given processor kind
 * may not implement (a subscribing processor has no segments), and the
 * instruction handler simply skips what is absent.
 */
export type ManagedEventProcessor = {
  readonly name: string
  readonly running?: boolean
  readonly replaying?: boolean
  readonly position?: bigint
  start?(): Promise<void> | void
  stop?(): void
  supportsReset?(): boolean
  processingStatus?(): ReadonlyMap<
    number,
    {
      readonly position?: bigint
      readonly caughtUp?: boolean
      readonly replaying?: boolean
      readonly error?: Error
    }
  >
  releaseSegment?(segmentId: number): Promise<unknown> | unknown
  splitSegment?(segmentId: number): Promise<unknown> | unknown
  mergeSegment?(segmentId: number): Promise<unknown> | unknown
}

/**
 * A running control plane. The platform stream is live; instructions are being
 * routed and status is being reported until `close()`.
 */
export type AxonServerControlPlane = {
  /**
   * The processors this control plane addresses, keyed by name — the snapshot
   * taken at construction. Exposed for introspection and tests.
   */
  readonly processors: ReadonlyMap<string, ManagedEventProcessor>
  /** Stop the platform stream. Idempotent, and safe to call before `axon.close()`. */
  close(): Promise<void>
}

/** Map the managed processors into the status shape the platform stream reports. */
function processorStatuses(processors: Iterable<ManagedEventProcessor>): ProcessorStatus[] {
  return Array.from(processors, (proc) => {
    const isStreamingProcessor = proc.supportsReset?.() !== false
    const perSegment = proc.processingStatus?.()
    const segments: SegmentStatus[] = perSegment
      ? Array.from(perSegment.entries()).map(([segmentId, status]) => ({
          segmentId,
          caughtUp: status.caughtUp ?? false,
          replaying: status.replaying ?? false,
          onePartOf: 1,
          tokenPosition: status.position ?? 0n,
          errorState: status.error?.message ?? "",
        }))
      : [
          {
            segmentId: 0,
            caughtUp: true,
            replaying: proc.replaying ?? false,
            onePartOf: 1,
            tokenPosition: proc.position ?? 0n,
            errorState: "",
          },
        ]
    return {
      name: proc.name,
      running: proc.running ?? false,
      mode: isStreamingProcessor ? "Tracking" : "Subscribing",
      isStreamingProcessor,
      activeThreads: proc.running ? 1 : 0,
      availableThreads: 0,
      error: false,
      tokenStoreIdentifier: "",
      segments,
    }
  })
}

/**
 * Wire remote administration onto an Axon Server platform stream and start it.
 *
 * `conn` is the shared connection from `axonServerConnection(...)`. It owns the
 * gRPC channel and the `platformService` tuning, and it builds the platform
 * stream — but it never starts the control-plane half, because starting that is
 * exactly what this function is for.
 *
 * `processors` are the LIVE processor instances, which only exist after
 * `kronos` has built them — that is why this cannot be folded back into the
 * backend factory. Pass `app.processors.values()`.
 *
 * Caveat on that call: `kronos` types `processors` as
 * `ReadonlyMap<string, unknown>`, so `.values()` needs a cast today —
 * `app.processors.values() as Iterable<ManagedEventProcessor>`. Narrowing that
 * map in `@kronos-ts/core` would remove the cast; this package cannot.
 *
 * The collection is SNAPSHOTTED into a name-keyed map here, once. That is
 * deliberate: a one-shot iterator (`Map.values()` is one) would otherwise yield
 * nothing on the second status report. The processor OBJECTS are live, so each
 * report still reads current `running` / `position` / segment state off them —
 * only the membership of the set is fixed at construction.
 */
export async function axonServerControlPlane(
  conn: AxonServerPlatformSource,
  processors: Iterable<ManagedEventProcessor> = [],
): Promise<AxonServerControlPlane> {
  const { platform } = conn
  // Name-keyed view so server-initiated instructions route to the right one.
  const byName = new Map<string, ManagedEventProcessor>()
  for (const proc of processors) byName.set(proc.name, proc)

  platform.onInstruction(async (instruction) => {
    switch (instruction.kind) {
      case "pause-processor":
        byName.get(instruction.processorName)?.stop?.()
        break
      case "start-processor":
        await byName.get(instruction.processorName)?.start?.()
        break
      case "release-segment":
        await byName.get(instruction.processorName)?.releaseSegment?.(instruction.segmentId)
        break
      case "split-segment":
        await byName.get(instruction.processorName)?.splitSegment?.(instruction.segmentId)
        break
      case "merge-segment":
        await byName.get(instruction.processorName)?.mergeSegment?.(instruction.segmentId)
        break
    }
  })

  platform.registerProcessorStatusSupplier(() => processorStatuses(byName.values()))

  // ORDERING: strictly AFTER both handlers — see the file-level JSDoc.
  // An instruction arriving in the gap would be dropped; an early status
  // request would find no supplier.
  await platform.start()

  return {
    processors: byName,
    async close() {
      platform.stop()
    },
  }
}
