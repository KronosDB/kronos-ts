/**
 * KronosDB backend for @kronos-ts.
 *
 * The connection is a RESOURCE — one socket, one platform stream, one
 * readiness barrier — so it is named: {@link kronosDbConnection}. Everything
 * else is a FUNCTION over it, one per seam:
 *
 * ```ts
 * const kdb = await kronosDbConnection({ componentName: "university-service", serializer })
 *
 * const eventStore    = kronosDbEventStore(kdb, "billing")
 * const eventStore = kronosDbSnapshottingEventStore(
 *   kronosDbEventStore(kdb, "billing"), kdb, "billing")
 * const commandBus    = interceptingCommandBus(
 *   kronosDbCommandBus(localCommandBus(unitOfWork), kdb), correlation)
 * const queryBus      = interceptingQueryBus(
 *   kronosDbQueryBus(localQueryBus(unitOfWork), kdb), correlation)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
 * await kdb.start()                   // subscription-ack wait, after handlers subscribe
 * // …
 * await app.stop(); await kdb.close()
 * ```
 *
 * There used to be a `kronosDbContext(kdb, options)` in the middle, returning a
 * record of four components at once. It is gone: a context is not a thing you
 * build, it is a STRING two of these functions take. `kronosDbEventStore(kdb,
 * "billing")` and `kronosDbEventStore(kdb, "catalog")` still share the one
 * socket — the context each request names is the whole difference — so a
 * process addressing nineteen contexts still opens one connection, and now the
 * caller who wants only an event store builds only an event store.
 *
 * There is no lifecycle framework and no container: what used to be
 * `onStart("connect")` happens inside `kronosDbConnection`, what used to be
 * `onStart("processors")` is its `start()`, and what used to be
 * `onStop("connect")` is its `close()`.
 *
 * Remote administration — KronosDB pushing pause / start / split / merge at this
 * client's processors, and this client reporting their status back for the admin
 * UI — is NOT part of either. It is opt-in, in `./control-plane.js`:
 *
 * ```ts
 * const control = kronosDbControlPlane(kdb, app.processors)
 * ```
 *
 * Because the connection exists before any component is built, the stores and
 * buses are constructed against a live connection — the lazy proxies and the
 * subscribe()-buffering wrappers the container era needed are gone.
 */
import { withMessagingTimeout, type Serializer } from "@kronos-ts/core"
import { connectToKronosDb, type KronosDbConnection, type KronosDbConnectionConfig } from "./connection.js"
import { platformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"
import { withRetry, type ResilienceConfig } from "./resilience.js"
import type { ShutdownLatch } from "./shutdown-latch.js"

/**
 * Connection-level options: the socket, the platform stream, how payloads are
 * encoded on this wire, and how hard to retry.
 *
 * The SERIALIZER is here rather than on each store and bus because it is a
 * property of this client's wire — one connection, one encoding — and because a
 * store keyed by `(connection, context)` has nowhere honest to put it.
 */
export type KronosDbConnectionOptions = KronosDbConnectionConfig & {
  /** Maximum graceful drain time; transport closes even when this expires. Default: 30000ms. */
  shutdownTimeoutMs?: number
  /** How long `start()` waits for the server to acknowledge this client's handlers. Default: 30000ms. */
  startTimeoutMs?: number
  serializer: Serializer
  platformService?: PlatformServiceOptions
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
}

/**
 * The RESOURCE a KronosDB deployment shares: ONE socket, the platform
 * stream layered on it, and the lifecycle that arms and drains them.
 *
 * Contexts are strings the stores take — `kronosDbEventStore(connection,
 * context)` — so a process addressing nineteen contexts opens one socket, not
 * nineteen.
 */
export type KronosDbConnectionHandle = {
  /** The live connection, for callers that need the native link under it. */
  readonly connection: KronosDbConnection
  /** How payloads are encoded on this wire — shared by every store and bus. */
  readonly serializer: Serializer
  /**
   * The platform stream. Persistence and transport do not use it; it is public
   * so the optional control plane can be handed it:
   * `kronosDbControlPlane(kdb, app.processors)`.
   */
  readonly platform: PlatformConnection
  /**
   * Buses register their drain latches here so {@link close} covers every bus
   * opened on this connection.
   *
   * @internal
   */
  readonly registerShutdownLatch: (latch: ShutdownLatch) => void
  /**
   * Buses register what {@link start} must wait for here: a function that
   * resolves once the server has acknowledged every handler the bus announced.
   *
   * @internal
   */
  readonly registerReadiness: (acknowledged: () => Promise<void>) => void
  /**
   * Wait until KronosDB has acknowledged this client's handlers, i.e. until
   * handler subscriptions are routable. Call AFTER every handler is subscribed
   * (after `kronos`), ONCE, no matter how many contexts you opened — the ack is
   * a property of the connection, not of a context. Idempotent: concurrent and
   * repeat calls share the first barrier. This is the D-102 replacement for the
   * legacy 1-second sleep — it waits exactly long enough, no longer.
   *
   * This is the readiness barrier and nothing else. Remote administration is
   * `kronosDbControlPlane`, which is opt-in and takes no part in startup.
   */
  start(): Promise<void>
  /** Drain every bus's in-flight work, stop the platform stream, close the socket. */
  close(): Promise<void>
}

/**
 * What a bus needs of the connection handle. A bus built by hand against a
 * narrower host (a test, a custom composition) need not provide a readiness
 * registry: it then has no start barrier to join.
 */
export type BusHost = Pick<KronosDbConnectionHandle, "connection" | "serializer" | "registerShutdownLatch"> &
  Partial<Pick<KronosDbConnectionHandle, "registerReadiness">>

/**
 * Open the KronosDB connection.
 *
 * Nothing touches the network here: the socket connects when the first store,
 * bus or platform stream needs it. What used to be a retried connect stage is
 * now each of those users' own business, which is where a failure belongs.
 *
 * ```ts
 * const kdb = await kronosDbConnection({ componentName: "university-service", serializer })
 *
 * const eventStore = kronosDbEventStore(kdb, "billing")
 * const commandBus = interceptingCommandBus(
 *   kronosDbCommandBus(localCommandBus(unitOfWork), kdb), correlation)
 * const queryBus = interceptingQueryBus(
 *   kronosDbQueryBus(localQueryBus(unitOfWork), kdb), correlation)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
 * await kdb.start()                   // subscription-ack wait, after handlers subscribe
 * // …
 * await app.stop(); await kdb.close()
 * ```
 */
export async function kronosDbConnection(
  options: KronosDbConnectionOptions,
): Promise<KronosDbConnectionHandle> {
  const { resilience, serializer } = options
  const busLatches: ShutdownLatch[] = []
  const readiness: Array<() => Promise<void>> = []

  const connection: KronosDbConnection = connectToKronosDb(options)
  const platform = platformConnection(connection, options.platformService)

  // Memoised so N contexts (or a caller who simply calls it twice) share ONE
  // barrier. The ack is connection-scoped: it says KronosDB accepted this
  // client's handlers, which covers every subscription sent on the connection
  // regardless of which bus it named.
  let started: Promise<void> | undefined

  return {
    connection,
    serializer,
    platform,
    registerShutdownLatch(latch) {
      busLatches.push(latch)
    },
    registerReadiness(acknowledged) {
      readiness.push(acknowledged)
    },
    async start() {
      // ASYMMETRY WITH axon-server, ON PURPOSE. The axon backend has a
      // `platform.armConnectionMonitoring()` split — a data-path entry point
      // that opens the platform stream and arms the heartbeat WITHOUT arming
      // processor status reporting. KronosDB does not need one: this
      // connection has always called `platform.start()` itself, and the
      // liveness checks that drive `connection.reconnect()` have always been
      // armed on the data path regardless of whether anyone built a
      // `kronosDbControlPlane`.
      //
      // The cost of not splitting here is one idle timer: `platform.start()`
      // also arms status reporting, and `reportProcessorStatus()` returns
      // immediately while `processorStatusSuppliers` is empty. Harmless, and a
      // control plane created later is picked up live because the reporter reads
      // the supplier list on every tick rather than capturing it.
      //
      // `platform.start()` is idempotent — if a control plane was created first
      // (the recommended order) it already brought the stream live AFTER
      // registering its handlers, and this is a no-op. Instructions that land
      // before a control plane registers are buffered by the platform
      // connection, not dropped.
      started ??= (async () => {
        await platform.start()
        await withRetry(
          async () => {
            const ok = await platform.subscriptionsAcked()
            if (!ok) throw new Error("subscriptions not yet acked")
          },
          { event: "per-operation", ...resilience },
        )
        // Every handler a bus announced has been answered by the server: from
        // here on a command or query for it can be routed.
        await withMessagingTimeout(
          Promise.all(readiness.map((acknowledged) => acknowledged())),
          options.startTimeoutMs ?? 30000,
          "Handler registration",
        )
      })()
      return started
    },
    async close() {
      try {
        await withMessagingTimeout(Promise.all(busLatches.map((l) => l.initiateShutdown())), options.shutdownTimeoutMs ?? 30000, "Messaging shutdown")
      } finally {
        platform.stop()
        connection.close()
      }
    },
  }
}
