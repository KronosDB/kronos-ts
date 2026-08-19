/**
 * The KronosDB platform control plane — remote administration, split out of the
 * backend.
 *
 * This is not persistence and not transport. KronosDB pushes instructions at a
 * connected client (pause-processor, start-processor, release-segment,
 * split-segment, merge-segment) and the client reports processor status back so
 * the admin UI can render it. It only ever lived inside the backend because it
 * shares the same gRPC connection as the data path — so it takes that
 * connection's platform stream as an argument and is otherwise independent.
 *
 * Opt-in: a backend with no control plane is a perfectly good backend. You just
 * cannot administer its processors from KronosDB.
 *
 * ```ts
 * const kdb = await kronosDbConnection({ componentName: "svc" })
 * const app = kronos({
 *   components: inMemoryComponents(kronosDbContext(kdb, { serializer, unitOfWork })),
 *   modules,
 * })
 *
 * // Opt in to remote administration. Create it BEFORE kdb.start() — see the
 * // ordering note on kronosDbControlPlane.
 * const control = kronosDbControlPlane(kdb, app.processors)
 *
 * await kdb.start()
 * // …
 * await app.stop(); await control.close(); await kdb.close()
 * ```
 */
import type { PlatformConnection } from "./platform-service.js"
import type { KronosDbConnectionHandle } from "./kronosdb.js"
import type { ProcessorStatus } from "./event-processor-info.js"

/**
 * The subset of a live processor the control plane drives. Structurally
 * satisfied by TrackingEventProcessor / StreamingEventProcessor; everything past
 * `name` is optional so an instruction is skipped rather than crashing when a
 * processor kind does not implement it.
 */
export interface ManagedEventProcessor {
  readonly name: string
  start?(): Promise<void> | void
  stop?(): void
  releaseSegment?(segmentId: number): Promise<void> | void
  splitSegment?(segmentId: number): Promise<void> | void
  mergeSegment?(segmentId: number): Promise<void> | void
  supportsReset?(): boolean
  processingStatus?(): Map<number, unknown>
  readonly running?: boolean
}

/**
 * Where the control plane reads live processors from.
 *
 * Accepts a plain iterable (an array you built yourself) or a name-keyed map —
 * which is what `app.processors` is, so `kronosDbControlPlane(kdb,
 * app.processors)` compiles with no cast at the call site. Values that do not
 * look like a {@link ManagedEventProcessor} are ignored rather than throwing,
 * because `app.processors` is typed `ReadonlyMap<string, unknown>`.
 *
 * The source is re-read on every instruction and every status report, so a live
 * map view stays authoritative — processors added after the control plane was
 * created are addressable.
 */
export type ManagedProcessorSource =
  | Iterable<ManagedEventProcessor>
  | ReadonlyMap<string, unknown>

/** A running control plane. See {@link kronosDbControlPlane}. */
export interface KronosDbControlPlane {
  /** The platform stream this control plane drives. */
  readonly platform: PlatformConnection
  /** Stop the platform stream (heartbeats, status reporting, instruction intake). */
  close(): Promise<void>
}

function isManagedEventProcessor(value: unknown): value is ManagedEventProcessor {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { name?: unknown }).name === "string"
  )
}

/**
 * Normalises a {@link ManagedProcessorSource} into processors, skipping entries
 * that are not processor-shaped.
 *
 * Map-like is detected by the presence of `get` (arrays have `values` but no
 * `get`), so a `ReadonlyMap` is read by value rather than yielding
 * `[name, processor]` pairs.
 */
function* managedProcessors(source: ManagedProcessorSource): Generator<ManagedEventProcessor> {
  const mapLike = source as { get?: unknown; values?: unknown }
  const values: Iterable<unknown> =
    typeof mapLike.get === "function" && typeof mapLike.values === "function"
      ? (source as ReadonlyMap<string, unknown>).values()
      : (source as Iterable<unknown>)

  for (const value of values) {
    if (isManagedEventProcessor(value)) yield value
  }
}

function findProcessor(
  source: ManagedProcessorSource,
  name: string,
): ManagedEventProcessor | undefined {
  for (const processor of managedProcessors(source)) {
    if (processor.name === name) return processor
  }
  return undefined
}

/** Maps one live processor onto the status shape the admin UI consumes. */
function toProcessorStatus(processor: ManagedEventProcessor): ProcessorStatus {
  const proc = processor as ManagedEventProcessor & {
    replaying?: boolean
    position?: bigint
  }

  return {
    name: proc.name,
    running: proc.running ?? false,
    mode: proc.supportsReset?.() === false ? "Subscribing" : "Tracking",
    isStreamingProcessor: proc.supportsReset?.() !== false,
    activeThreads: proc.running ? 1 : 0,
    availableThreads: 0,
    error: false,
    tokenStoreIdentifier: "",
    segments: proc.processingStatus
      ? Array.from(proc.processingStatus().entries() as Iterable<[number, any]>).map(
          ([segId, status]: [number, any]) => ({
            segmentId: segId,
            caughtUp: status.caughtUp ?? false,
            replaying: status.replaying ?? false,
            onePartOf: 1,
            tokenPosition: status.position ?? 0n,
            errorState: status.error?.message ?? "",
          }),
        )
      : [
          {
            segmentId: 0,
            caughtUp: true,
            replaying: proc.replaying ?? false,
            onePartOf: 1,
            tokenPosition: proc.position ?? 0n,
            errorState: "",
          },
        ],
  }
}

/**
 * Wires KronosDB's remote administration onto the connection's platform stream.
 *
 * Registers the instruction handler and the processor-status supplier, then —
 * and only then — brings the stream live via `platform.start()`.
 *
 * ## Ordering
 *
 * `platform.start()` is called LAST, after both handlers. Doing it the
 * other way round loses any instruction that arrives in the gap and lets an
 * early status request find no supplier. That ordering is owned here, which is
 * why this factory calls `platform.start()` rather than the connection.
 *
 * Create the control plane BEFORE `kdb.start()`. `kdb.start()` is the
 * subscription-ack barrier, and the ack signal only exists on a live platform
 * stream — so the connection starts the stream itself if nothing else has. Getting
 * the order wrong is safe but wasteful: `platform.start()` is idempotent, and
 * instructions that land before this factory registers its handler are buffered
 * by the platform connection and flushed on handler.
 *
 * @param connection The KronosDB connection — its platform stream is what gets
 *                   wired. Narrowed to `{ platform }` so a test can drive this
 *                   with a fake stream and nothing else.
 * @param processors Live processors to address and report on — `app.processors`.
 */
export function kronosDbControlPlane(
  connection: Pick<KronosDbConnectionHandle, "platform">,
  processors: ManagedProcessorSource,
): KronosDbControlPlane {
  const { platform } = connection
  platform.onInstruction(async (instruction) => {
    switch (instruction.kind) {
      case "pause-processor": {
        const proc = findProcessor(processors, instruction.processorName)
        if (proc?.stop) proc.stop()
        break
      }
      case "start-processor": {
        const proc = findProcessor(processors, instruction.processorName)
        if (proc?.start) await proc.start()
        break
      }
      case "release-segment": {
        const proc = findProcessor(processors, instruction.processorName)
        if (proc?.releaseSegment) await proc.releaseSegment(instruction.segmentId)
        break
      }
      case "split-segment": {
        const proc = findProcessor(processors, instruction.processorName)
        if (proc?.splitSegment) await proc.splitSegment(instruction.segmentId)
        break
      }
      case "merge-segment": {
        const proc = findProcessor(processors, instruction.processorName)
        if (proc?.mergeSegment) await proc.mergeSegment(instruction.segmentId)
        break
      }
    }
  })

  platform.registerProcessorStatusSupplier(() =>
    Array.from(managedProcessors(processors), toProcessorStatus),
  )

  // Stream goes live only AFTER the instruction handler and status supplier are
  // registered — see the ordering note above. `start()` resolves without
  // awaiting anything, so the stream is live by the time this returns; the catch
  // is for a synchronous channel failure, which the first data-path call will
  // also surface.
  const started = platform.start().catch((err) => {
    console.error("KronosDB control plane: failed to start the platform stream:", err)
  })

  return {
    platform,
    async close() {
      await started
      platform.stop()
    },
  }
}
