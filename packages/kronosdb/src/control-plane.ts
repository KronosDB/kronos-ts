/**
 * The KronosDB platform control plane — remote administration, split out of the
 * backend.
 *
 * This is not persistence and not transport. KronosDB pushes instructions at a
 * connected client (pause and start a processor; split, merge and release one
 * of its segments) and the client reports each processor's status, with the
 * segments this node holds, so the admin UI can render it. It only ever lived
 * inside the backend because it
 * shares the same connection as the data path — so it takes that
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
import type { ProcessorStatus, SegmentReport } from "./event-processor-info.js"

/**
 * The subset of a live processor the control plane drives and reports on.
 * `RunningProcessor` satisfies it structurally; everything past `name` is
 * optional so a foreign processor-shaped object is skipped rather than crashed on.
 */
export type ManagedEventProcessor = {
  readonly name: string
  readonly running?: boolean
  readonly replaying?: boolean
  readonly position?: bigint
  start?(): Promise<void> | void
  stop?(): Promise<void> | void
  status?(): {
    readonly caughtUp?: boolean
    readonly replaying?: boolean
    readonly position?: bigint
    readonly error?: Error
  }
  /** The segments this node holds. `mask + 1` is how many such segments make the whole. */
  segments?(): ReadonlyArray<{
    readonly segment: number
    readonly mask: number
    readonly position: bigint
    readonly caughtUp: boolean
    readonly replaying: boolean
    readonly error?: Error
  }>
  releaseSegment?(segment: number): Promise<void>
  splitSegment?(segment: number): Promise<void>
  mergeSegment?(segment: number): Promise<void>
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
export type KronosDbControlPlane = {
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
function toProcessorStatus(proc: ManagedEventProcessor): ProcessorStatus {
  const status = proc.status?.()
  return {
    name: proc.name,
    running: proc.running ?? false,
    caughtUp: status?.caughtUp ?? true,
    replaying: status?.replaying ?? proc.replaying ?? false,
    position: status?.position ?? proc.position ?? 0n,
    error: status?.error?.message,
    segments: proc.segments?.().map(
      (s): SegmentReport => ({
        segment: s.segment,
        onePartOf: s.mask + 1,
        position: s.position,
        caughtUp: s.caughtUp,
        replaying: s.replaying,
        error: s.error?.message,
      }),
    ),
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
  // A control that throws (a split of a processor that is not segmented, a
  // merge of a replaying segment) fails the instruction, and the platform
  // stream reports why.
  platform.onInstruction(async (instruction) => {
    switch (instruction.kind) {
      case "pause-processor":
        await findProcessor(processors, instruction.processorName)?.stop?.()
        break
      case "start-processor":
        await findProcessor(processors, instruction.processorName)?.start?.()
        break
      case "release-segment":
        await findProcessor(processors, instruction.processorName)?.releaseSegment?.(instruction.segmentId)
        break
      case "split-segment":
        await findProcessor(processors, instruction.processorName)?.splitSegment?.(instruction.segmentId)
        break
      case "merge-segment":
        await findProcessor(processors, instruction.processorName)?.mergeSegment?.(instruction.segmentId)
        break
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
