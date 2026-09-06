/**
 * The Axon Server PLATFORM CONTROL PLANE — remote administration, extracted
 * out of the backend.
 *
 * This is not persistence and it is not transport. It is the pair of duties
 * Axon Server's admin surface needs from a client:
 *
 *   1. inbound — Axon Server pushes processor instructions (pause-processor,
 *      start-processor) which have to be routed to the live processor of that
 *      name; segment instructions (split, merge, release) are ignored, a kronos
 *      processor has one lane;
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
import type { ProcessorStatus } from "./event-processor-info.js"

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
  stop?(): void
  status?(): {
    readonly caughtUp?: boolean
    readonly replaying?: boolean
    readonly position?: bigint
    readonly error?: Error
  }
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
    const status = proc.status?.()
    return {
      name: proc.name,
      running: proc.running ?? false,
      caughtUp: status?.caughtUp ?? true,
      replaying: status?.replaying ?? proc.replaying ?? false,
      position: status?.position ?? proc.position ?? 0n,
      error: status?.error?.message,
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
      // release/split/merge-segment: a kronos processor has one lane. Ignored.
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
