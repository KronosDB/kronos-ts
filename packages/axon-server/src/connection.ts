import { UnavailableError, withMessagingTimeout } from "@kronos-ts/core"
import {
  createChannel,
  waitForChannelReady,  createClient,
  type Channel,
  type Client,
  type ChannelCredentials,
} from "nice-grpc"
import { ChannelCredentials as GrpcChannelCredentials } from "@grpc/grpc-js"
import { readFileSync } from "node:fs"
import type { Serializer } from "@kronos-ts/core"
import { withRetry, healthCheck, type ResilienceConfig } from "./resilience.js"
import { PlatformServiceDefinition } from "./generated/control.js"
import { CommandServiceDefinition } from "./generated/command.js"
import { QueryServiceDefinition } from "./generated/query.js"
import { DcbEventStoreDefinition, DcbSnapshotStoreDefinition } from "./generated/dcb.js"
import { shutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"
import {
  platformConnection,
  type PlatformConnection,
  type PlatformServiceOptions,
} from "./platform-service.js"

/**
 * Configuration for connecting to Axon Server.
 */
export type AxonServerConnectionConfig = {
  /**
   * Host of the Axon Server. Defaults to "localhost".
   * For single-server setups.
   */
  host?: string
  /** gRPC port of the Axon Server. Defaults to 8124. */
  port?: number
  /**
   * Multiple server addresses for cluster deployments.
   * Each entry is `"host:port"`. When provided, overrides `host` and `port`.
   * The connector tries servers in order until one connects, and fails over
   * to the next on connection failure.
   *
   * Aligned with Java's `axon.axonserver.servers` property.
   */
  servers?: string[]
  /** The context to connect to. Defaults to "default". */
  context?: string
  /** Name identifying this component to Axon Server. */
  componentName: string
  /** Unique client identifier. Defaults to a generated UUID. */
  clientId?: string
  /** Access token for authentication. Optional. */
  token?: string
  /** Reconnection interval in ms. Defaults to 2000. */
  reconnectIntervalMs?: number
  /** Maximum reconnection attempts before giving up. 0 = unlimited. Defaults to 0. */
  maxReconnectAttempts?: number

  /**
   * gRPC keepalive ping interval in ms.
   * Sends a ping after this period of inactivity to keep the connection alive.
   * Default: 30000. Java's default is 1000ms but @grpc/grpc-js requires
   * a minimum of 10000ms to avoid server-side RST_STREAM rejections.
   */
  keepAliveTimeMs?: number
  /**
   * gRPC keepalive timeout in ms.
   * Connection is considered dead if no response within this window.
   * Default: 10000.
   */
  keepAliveTimeoutMs?: number
  /**
   * Allow keepalive pings even when there are no active RPCs.
   * Default: true.
   */
  keepAlivePermitWithoutCalls?: boolean
  /**
   * How long a read (`source`, the snapshot store's `getLast`, `getHead`) may
   * take before it is cancelled and asked once more. A read whose answer has
   * fully arrived can still never complete under Bun — its http2 client
   * intermittently loses the stream's `end` event, and grpc-js releases an OK
   * status only after it — so without a bound a command handler parks on the
   * read until Axon Server cancels the command (300 s). Reads are idempotent;
   * a lost one is simply repeated. Raise this if a single sourcing read of
   * yours legitimately takes longer. Default: 15000.
   */
  readTimeoutMs?: number

  /**
   * TLS/SSL configuration. When enabled, the connection uses a secure gRPC channel.
   * Aligned with Java's `axon.axonserver.ssl-enabled` and `axon.axonserver.cert-file`.
   */
  ssl?: {
    /** Enable TLS. When true, a secure channel is created. */
    enabled: boolean
    /**
     * Path to the CA certificate file (PEM format) for server verification.
     * If omitted, the system's default trust store is used.
     */
    certFile?: string
    /**
     * Path to the client certificate file (PEM) for mutual TLS.
     * Only needed if Axon Server requires client certificates.
     */
    clientCertFile?: string
    /**
     * Path to the client private key file (PEM) for mutual TLS.
     * Only needed if Axon Server requires client certificates.
     */
    clientKeyFile?: string
  }
}

export type ConnectionState =
  | "disconnected"
  | "connecting"
  | "connected"
  | "reconnecting"
  | "closed"

/**
 * An active connection to Axon Server, providing typed gRPC clients
 * for all services. Supports reconnection on failure.
 */
export type AxonServerConnection = {
  /** The underlying gRPC channel. */
  readonly channel: Channel
  /** Platform service — connection management, topology. */
  readonly platform: Client<typeof PlatformServiceDefinition>
  /** Command service — dispatch and handle commands. */
  readonly commands: Client<typeof CommandServiceDefinition>
  /** Query service — dispatch and handle queries. */
  readonly queries: Client<typeof QueryServiceDefinition>
  /** Event store — event sourcing with Dynamic Consistency Boundaries. */
  readonly eventStore: Client<typeof DcbEventStoreDefinition>
  /** Snapshot store — state snapshots. */
  readonly snapshotStore: Client<typeof DcbSnapshotStoreDefinition>
  /** The resolved configuration. `servers` and `ssl` stay optional — they have no defaults. */
  readonly config: Omit<Required<AxonServerConnectionConfig>, "servers" | "ssl"> & {
    servers?: AxonServerConnectionConfig["servers"]
    ssl?: AxonServerConnectionConfig["ssl"]
  }
  /** Current connection state. */
  readonly state: ConnectionState
  /**
   * Register a callback invoked when the connection is (re)established.
   * Used by buses to re-subscribe handlers after reconnection.
   */
  onReconnect(callback: () => void): void
  /**
   * Register a callback invoked when the connection is lost.
   */
  onDisconnect(callback: (error?: Error) => void): void
  /** Close the connection permanently. No reconnection after this. */
  close(): void
  /**
   * Attempt to reconnect if disconnected.
   * Returns a promise that resolves when reconnected or rejects on failure.
   */
  reconnect(): Promise<void>
}

/**
 * Connects to Axon Server and returns typed gRPC clients for all services.
 *
 * Configures gRPC channel-level keepalive to maintain persistent connections,
 * aligned with Java's ManagedChannel keepalive settings.
 */
export function connectToAxonServer(config: AxonServerConnectionConfig): AxonServerConnection {
  const resolvedConfig = {
    host: config.host ?? "localhost",
    port: config.port ?? 8124,
    context: config.context ?? "default",
    componentName: config.componentName,
    clientId: config.clientId ?? crypto.randomUUID(),
    token: config.token ?? "",
    reconnectIntervalMs: config.reconnectIntervalMs ?? 2000,
    maxReconnectAttempts: config.maxReconnectAttempts ?? 0,
    keepAliveTimeMs: config.keepAliveTimeMs ?? 30000,
    keepAliveTimeoutMs: config.keepAliveTimeoutMs ?? 10000,
    keepAlivePermitWithoutCalls: config.keepAlivePermitWithoutCalls ?? true,
    readTimeoutMs: config.readTimeoutMs ?? 15000,
    servers: config.servers,
    ssl: config.ssl,
  }

  // Build gRPC channel credentials
  const sslConfig = config.ssl
  let credentials: ChannelCredentials | undefined

  if (sslConfig?.enabled) {
    const rootCerts = sslConfig.certFile ? readFileSync(sslConfig.certFile) : null
    const clientKey = sslConfig.clientKeyFile ? readFileSync(sslConfig.clientKeyFile) : null
    const clientCert = sslConfig.clientCertFile ? readFileSync(sslConfig.clientCertFile) : null
    credentials = GrpcChannelCredentials.createSsl(
      rootCerts,
      clientKey,
      clientCert,
    ) as ChannelCredentials
  }

  // gRPC channel options — keepalive to maintain persistent connections
  const channelOptions = {
    "grpc.keepalive_time_ms": config.keepAliveTimeMs ?? 30000,
    "grpc.keepalive_timeout_ms": config.keepAliveTimeoutMs ?? 10000,
    "grpc.keepalive_permit_without_calls": (config.keepAlivePermitWithoutCalls ?? true) ? 1 : 0,
    // Own subchannel pool per channel. grpc-js pools subchannels PROCESS-WIDE by
    // target address by default, so a later connection to the same host:port —
    // a fresh container on a reused mapped port, in a test process that already
    // talked to another one — can be handed the previous server's HTTP/2
    // session, and its unary calls hang until the server-side timeout.
    "grpc.use_local_subchannel_pool": 1,
  }

  // Build server address list for failover
  const serverAddresses =
    config.servers && config.servers.length > 0
      ? config.servers
      : [`${resolvedConfig.host}:${resolvedConfig.port}`]

  let currentServerIndex = 0

  function createGrpcChannel(): Channel {
    const address = serverAddresses[currentServerIndex % serverAddresses.length]!
    return credentials
      ? createChannel(address, credentials, channelOptions)
      : createChannel(address, undefined, channelOptions)
  }

  let channel = createGrpcChannel()
  let state: ConnectionState = "connected"
  let reconnectPromise: Promise<void> | undefined
  let retryTimer: ReturnType<typeof setTimeout> | undefined
  let finishRetryDelay: (() => void) | undefined

  const reconnectCallbacks: Array<() => void> = []
  const disconnectCallbacks: Array<(error?: Error) => void> = []

  function createClients() {
    return {
      platform: createClient(PlatformServiceDefinition, channel),
      commands: createClient(CommandServiceDefinition, channel),
      queries: createClient(QueryServiceDefinition, channel),
      eventStore: createClient(DcbEventStoreDefinition, channel),
      snapshotStore: createClient(DcbSnapshotStoreDefinition, channel),
    }
  }

  let clients = createClients()

  const connection: AxonServerConnection = {
    get channel() {
      return channel
    },
    get platform() {
      return clients.platform
    },
    get commands() {
      return clients.commands
    },
    get queries() {
      return clients.queries
    },
    get eventStore() {
      return clients.eventStore
    },
    get snapshotStore() {
      return clients.snapshotStore
    },
    config: resolvedConfig,

    get state() {
      return state
    },

    onReconnect(callback) {
      reconnectCallbacks.push(callback)
    },

    onDisconnect(callback) {
      disconnectCallbacks.push(callback)
    },

    close() {
      if (state === "closed") return
      state = "closed"
      for (const callback of disconnectCallbacks) {
        try { callback() } catch { /* Listener failures cannot prevent teardown. */ }
      }
      clearTimeout(retryTimer)
      finishRetryDelay?.()
      channel.close()
    },

    reconnect() {
      if (state === "closed") return Promise.reject(new UnavailableError("Connection is permanently closed"))
      if (reconnectPromise) return reconnectPromise
      reconnectPromise = (async () => {
        state = "reconnecting"
        channel.close()
        for (const callback of disconnectCallbacks) {
          try { callback() } catch { /* Listener failures cannot prevent recovery. */ }
        }
        let attempt = 0
        while (state === "reconnecting") {
          attempt++
          try {
            currentServerIndex++
            channel = createGrpcChannel()
            // Channel construction is lazy. Only report recovery once the new
            // transport is ready, otherwise a dead server falsely "succeeds".
            await waitForChannelReady(channel, new Date(Date.now() + resolvedConfig.keepAliveTimeoutMs))
            if ((state as ConnectionState) === "closed") return
            clients = createClients()
            state = "connected"
            for (const callback of reconnectCallbacks) {
              try { callback() } catch { /* Listener failures are isolated. */ }
            }
            return
          } catch (error) {
            channel.close()
            if ((state as ConnectionState) === "closed") return
            if (resolvedConfig.maxReconnectAttempts > 0 && attempt >= resolvedConfig.maxReconnectAttempts) {
              state = "disconnected"
              throw new UnavailableError(`Failed to reconnect after ${attempt} attempts: ${error}`, { cause: error })
            }
            const backoff = Math.min(resolvedConfig.reconnectIntervalMs * 2 ** (attempt - 1), 30000)
            await new Promise<void>((resolve) => {
              finishRetryDelay = resolve
              retryTimer = setTimeout(resolve, backoff * (0.75 + Math.random() * 0.5))
            })
            finishRetryDelay = undefined
          }
        }
      })().finally(() => { reconnectPromise = undefined })
      return reconnectPromise

    },
  }

  return connection
}

// ---------------------------------------------------------------------------
// The shared RESOURCE: one channel, the platform stream on it, start()/close()
// ---------------------------------------------------------------------------

/**
 * Everything the shared Axon Server resource needs: where to dial, and how to
 * put a payload on the wire.
 *
 * THE SERIALIZER IS A PROPERTY OF THIS CLIENT'S WIRE, not of a context and not
 * of a bus. Every event, snapshot, command payload and query result this
 * process exchanges with Axon Server goes through the same codec, so it is
 * named once, here — which is also what leaves
 * `axonServerEventStore(conn, context)` and `axonServerCommandBus(local, conn)`
 * their honest two-argument shapes.
 */
export type AxonServerConnectionOptions = AxonServerConnectionConfig & {
  /** Maximum graceful drain time; transport closes even when this expires. Default: 30000ms. */
  shutdownTimeoutMs?: number
  /** Payload codec for every message this client exchanges with Axon Server. */
  serializer: Serializer
  /**
   * Retry policy for the initial connect and stream re-establishment, and
   * `healthCheckThresholdMs`: how long opening the connection waits for the
   * channel to be ready before it logs a warning and returns the handle anyway.
   */
  resilience?: Partial<ResilienceConfig>
  /** Platform stream tuning — heartbeat and processor-status cadence. */
  platformService?: PlatformServiceOptions
  /**
   * How long {@link AxonServerConnectionHandle.start} waits for Axon Server to
   * acknowledge the handler subscriptions sent on the command and query
   * streams before it rejects. Default: 30000ms.
   */
  startTimeoutMs?: number
}

/**
 * What a context-scoped STORE borrows from the connection: the gRPC clients and
 * the codec. Narrower than the handle on purpose — a test can drive a store
 * with a fake `connection` and nothing else.
 */
export type AxonServerStoreSource = {
  readonly connection: AxonServerConnection
  readonly serializer: Serializer
}

/**
 * What a BUS borrows: a store's two, plus the drain latch and the retry policy.
 * A bus built by hand against a narrower source (a test, a custom composition)
 * need not provide `registerReadiness`: it then has no start barrier to join.
 */
export type AxonServerBusSource = AxonServerStoreSource & {
  /**
   * The connection-wide drain latch. In-flight dispatches register on it and
   * `close()` waits for them, so a bus never has the transport pulled out from
   * under a call it is still awaiting.
   */
  readonly shutdown: ShutdownLatch
  readonly resilience?: Partial<ResilienceConfig>
  /**
   * Buses register what {@link AxonServerConnectionHandle.start} must wait for
   * here: a function that resolves once Axon Server has acknowledged every
   * handler subscription the bus has sent.
   *
   * @internal
   */
  readonly registerReadiness?: (acknowledged: () => Promise<void>) => void
}

/** What the CONTROL PLANE borrows: the platform stream, and only that. */
export type AxonServerPlatformSource = {
  readonly platform: PlatformConnection
}

/**
 * The RESOURCE an Axon Server deployment shares: one gRPC channel, the platform
 * stream riding on it, and the lifecycle pair.
 *
 * The stores and buses are NOT on it — `axonServerEventStore(conn, context)`,
 * `axonServerCommandBus(local, conn)` and friends are plain functions over
 * this, and a caller who wants only commands builds only that one. Multiple
 * contexts share this ONE channel: the per-call `AxonIQ-Context` header is what
 * separates them.
 */
export type AxonServerConnectionHandle = AxonServerBusSource & AxonServerPlatformSource & {
  /** Config after defaults — the resolved host, context, client id. */
  readonly config: AxonServerConnection["config"]
  /**
   * DATA-PATH START. Two things, both data path:
   *
   *   1. arm heartbeat-driven reconnect detection on the platform stream, and
   *   2. wait until Axon Server has acknowledged every handler subscription the
   *      buses have sent.
   *
   * Call AFTER `kronos`: a handler subscribed later is not waited for. Rejects
   * with an `UnavailableError` when the acknowledgements have not arrived
   * within `startTimeoutMs`, and with the server's error when it refuses a
   * subscription. Takes no arguments and arms no control-plane state.
   */
  start(): Promise<void>
  /** Drain in-flight bus work, stop the platform stream, close the channel. */
  close(): Promise<void>
}

/**
 * Open the shared Axon Server connection.
 *
 * ```ts
 * const axon = await axonServerConnection({
 *   componentName: "university-service",
 *   host, port,
 *   serializer: jsonSerializer(),
 * })
 * const eventStore    = axonServerEventStore(axon, "default")
 * const eventStore = axonServerSnapshottingEventStore(
 *   axonServerEventStore(axon, "default"), axon, "default")
 * const commandBus = interceptingCommandBus(
 *   axonServerCommandBus(localCommandBus(unitOfWork), axon), correlation)
 * const queryBus = interceptingQueryBus(
 *   axonServerQueryBus(localQueryBus(unitOfWork), axon), correlation)
 *
 * const app = kronos({ commandHandlers, queryHandlers })
 * await axon.start()                  // the server has acknowledged our handler subscriptions
 * // opt in to remote administration
 * const control = await axonServerControlPlane(axon, app.processors)
 * // …
 * await app.stop(); await control.close(); await axon.close()
 * ```
 *
 * ASYNC ON PURPOSE. Connecting before anything is built is what removes the
 * lazy proxies and subscribe-buffering the container version needed: by the
 * time `kronos` subscribes a handler, the gRPC streams are already live.
 *
 * REMOTE ADMINISTRATION IS NOT IN HERE. Processor instructions and processor
 * status reporting are the platform CONTROL PLANE — they are neither
 * persistence nor transport, and lived on this object only because they share
 * the gRPC connection. See `control-plane.ts`.
 *
 * Axon-specific protocol invariants are preserved byte-for-byte:
 *
 *   - CLIENT_SUPPORTS_STREAMING capability advertised on every dispatched query;
 *   - AxonIQ-Context + AxonIQ-Access-Token gRPC metadata headers on every
 *     outbound stream/RPC (see `contextView`);
 *   - permits-AFTER-subscriptions stream ordering on the initial handshake AND
 *     on reconnect;
 *   - shutdown ordering: drain latch → platform.stop → connection.close.
 */
export async function axonServerConnection(
  options: AxonServerConnectionOptions,
): Promise<AxonServerConnectionHandle> {
  const {
    serializer,
    resilience,
    platformService,
    startTimeoutMs,
    ...connectionConfig
  } = options

  const connection = await withRetry(async () => connectToAxonServer(connectionConfig), {
    event: "initial-connect",
    ...resilience,
  })

  // Creating the channel does not connect it. Wait until it is ready, for at
  // most `healthCheckThresholdMs`. An unreachable server is logged once and the
  // handle is returned anyway; the channel keeps trying to connect.
  await healthCheck(
    (deadline) =>
      waitForChannelReady(connection.channel, deadline).catch(() => {
        throw new Error("the channel to Axon Server is not ready")
      }),
    { thresholdMs: resilience?.healthCheckThresholdMs, log: resilience?.log },
  )

  // ONE latch for the connection. Both buses ride the same channel, so "drain
  // in-flight work before the transport goes away" is one question, not two.
  const shutdown = shutdownLatch()

  // Built here, started by the control plane (or by `start()` below for the data
  // path's half). Constructing it eagerly is what lets the control plane be a
  // separate object at all — and it keeps `platformService` tuning and `stop()`
  // ownership in one place, so the documented shutdown order holds whether or
  // not anyone opted in.
  const platform = platformConnection(connection, platformService)

  const readiness: Array<() => Promise<void>> = []

  return {
    connection,
    config: connection.config,
    serializer,
    resilience,
    shutdown,
    platform,
    registerReadiness(acknowledged) {
      readiness.push(acknowledged)
    },

    async start() {
      // RECONNECT DETECTION IS DATA PATH. The heartbeat on the platform stream
      // is what notices a dead channel and calls `connection.reconnect()`; both
      // buses hook `connection.onReconnect(...)` to rebuild their own streams.
      // Arming it used to be a side effect of `platform.start()`, which only
      // `axonServerControlPlane(...)` calls — so a service that never opted into
      // remote administration had NO reconnect detection at all and would sit on
      // a dead channel forever. It is armed here, unconditionally.
      //
      // `armConnectionMonitoring()` opens the stream and starts heartbeats but
      // arms NO processor status reporting; that stays the control plane's, and
      // a later `platform.start()` adds it to this same live stream. Both calls
      // are idempotent, so either order works.
      await platform.armConnectionMonitoring()

      // Axon Server answers each subscribe instruction with an InstructionAck
      // on the bus stream that sent it, once the node this client is connected
      // to has registered the handler. That says nothing about the other nodes
      // of a cluster: a command or query that enters through another node can
      // still find no handler until the registration has reached it, and the
      // protocol reports nothing for that.
      await withMessagingTimeout(
        Promise.all(readiness.map((acknowledged) => acknowledged())),
        startTimeoutMs ?? 30000,
        "Handler registration",
      )
    },

    async close() {
      try {
        await withMessagingTimeout(shutdown.initiateShutdown(), options.shutdownTimeoutMs ?? 30000, "Messaging shutdown")
      } finally {
        platform.stop()
        connection.close()
      }
    },
  }
}
