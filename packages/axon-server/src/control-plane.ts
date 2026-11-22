/**
 * The Axon Server PLATFORM CONTROL PLANE — remote administration, extracted
 * out of the backend.
 *
 * This is not persistence and it is not transport. It is the pair of duties
 * Axon Server's admin surface needs from a client:
 *
 *   1. inbound — Axon Server pushes processor instructions (pause and start a
 *      processor; split, merge and release one of its segments) which have to
 *      be routed to the live processor of that name;
 *   2. outbound — the client periodically reports each processor's status,
 *      with the segments this node holds, so the Axon Dashboard can render it.
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
 * const control = await axonServerControlPlane(axon, app.processors)
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
import type { ProcessorStatus, SegmentReport } from "./event-processor-info.js"

/**
 * A processor Axon Server is allowed to observe and control. `RunningProcessor`
 * satisfies it structurally; everything past `name` is optional so a foreign
 * processor-shaped object is skipped rather than crashed on.
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
 * A name-keyed map, which is what `app.processors` is, or an iterable such as
 * an array. Values that do not look like a {@link ManagedEventProcessor} are
 * ignored.
 *
 * A map or an array is read again on every instruction and every status
 * report, so a processor added to it after the control plane was created is
 * reported and addressable, and one removed from it is not.
 *
 * An iterator, such as `app.processors.values()`, can be read only once. It is
 * copied when the control plane is created, and processors added afterwards to
 * the map it came from are not seen. Pass the map.
 */
export type ManagedProcessorSource =
  | Iterable<ManagedEventProcessor>
  | ReadonlyMap<string, unknown>

/**
 * A running control plane. The platform stream is live; instructions are being
 * routed and status is being reported until `close()`.
 */
export type AxonServerControlPlane = {
  /**
   * The processors this control plane addresses now, keyed by name. Read from
   * the source at each access. Exposed for introspection and tests.
   */
  readonly processors: ReadonlyMap<string, ManagedEventProcessor>
  /** Stop the platform stream. Idempotent, and safe to call before `axon.close()`. */
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
 * A name-keyed view of a {@link ManagedProcessorSource}. `get`, `values` and
 * `current` each read the source when they are called.
 */
function processorsByName(source: ManagedProcessorSource) {
  const mapLike = source as { get?: unknown; values?: unknown }
  const isMap = typeof mapLike.get === "function" && typeof mapLike.values === "function"
  // An iterator is its own iterable and yields each value once, so it is
  // copied here. Anything else is read again at each call.
  const iterable = source as Iterable<unknown>
  const isIterator = !isMap && (iterable[Symbol.iterator]() as unknown) === iterable
  const readable = isIterator ? Array.from(iterable) : iterable

  const current = (): Map<string, ManagedEventProcessor> => {
    const values = isMap ? (source as ReadonlyMap<string, unknown>).values() : readable
    const byName = new Map<string, ManagedEventProcessor>()
    for (const value of values) {
      if (isManagedEventProcessor(value)) byName.set(value.name, value)
    }
    return byName
  }

  return {
    current,
    get: (name: string) => current().get(name),
    values: () => current().values(),
  }
}

/** Map the managed processors into the status shape the platform stream reports. */
function processorStatuses(processors: Iterable<ManagedEventProcessor>): ProcessorStatus[] {
  return Array.from(processors, (proc) => {
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
 * backend factory. Pass `app.processors`.
 *
 * The source is read again on every instruction and every status report, so
 * a processor added to the map after this call is reported and addressable.
 * Each report reads current `running` / `position` / segment state off the
 * processor objects. An iterator such as `app.processors.values()` is copied
 * once instead — see {@link ManagedProcessorSource}.
 */
export async function axonServerControlPlane(
  conn: AxonServerPlatformSource,
  processors: ManagedProcessorSource = [],
): Promise<AxonServerControlPlane> {
  const { platform } = conn
  // Name-keyed view so server-initiated instructions route to the right one.
  const byName = processorsByName(processors)

  // An instruction for a processor this application does not have throws.
  // The platform stream logs an instruction that threw; it sends Axon Server
  // no result for an instruction.
  const requireProcessor = (name: string): ManagedEventProcessor => {
    const processor = byName.get(name)
    if (!processor) throw new Error(`No event processor named "${name}" in this application`)
    return processor
  }

  platform.onInstruction(async (instruction) => {
    switch (instruction.kind) {
      case "pause-processor": {
        // Not awaited: instructions run inside the stream's read loop, and a
        // stop waits for the batch in flight — heartbeats would wait with it.
        const stopped = requireProcessor(instruction.processorName).stop?.()
        if (stopped) {
          stopped.catch((err) => console.error(`Axon Server control plane: pausing "${instruction.processorName}" failed:`, err))
        }
        break
      }
      case "start-processor":
        await requireProcessor(instruction.processorName).start?.()
        break
      case "release-segment":
        await requireProcessor(instruction.processorName).releaseSegment?.(instruction.segmentId)
        break
      case "split-segment":
        await requireProcessor(instruction.processorName).splitSegment?.(instruction.segmentId)
        break
      case "merge-segment":
        await requireProcessor(instruction.processorName).mergeSegment?.(instruction.segmentId)
        break
    }
  })

  platform.registerProcessorStatusSupplier(() => processorStatuses(byName.values()))

  // ORDERING: strictly AFTER both handlers — see the file-level JSDoc.
  // An instruction arriving in the gap would be dropped; an early status
  // request would find no supplier.
  await platform.start()

  return {
    get processors() {
      return byName.current()
    },
    async close() {
      platform.stop()
    },
  }
}
