import { createChannel, createClient, type Channel, type Client, type ChannelCredentials } from "nice-grpc"
import { ChannelCredentials as GrpcChannelCredentials } from "@grpc/grpc-js"
import { Metadata } from "nice-grpc"
import { readFileSync } from "node:fs"
import { PlatformServiceDefinition } from "./generated/control.js"
import { CommandServiceDefinition } from "./generated/command.js"
import { QueryServiceDefinition } from "./generated/query.js"
import { DcbEventStoreDefinition, DcbSnapshotStoreDefinition } from "./generated/dcb.js"

/**
 * Configuration for connecting to Axon Server.
 */
export interface AxonServerConnectionConfig {
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

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting" | "closed"

/**
 * An active connection to Axon Server, providing typed gRPC clients
 * for all services. Supports reconnection on failure.
 */
export interface AxonServerConnection {
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
  /** Snapshot store — entity state snapshots. */
  readonly snapshotStore: Client<typeof DcbSnapshotStoreDefinition>
  /** The resolved configuration. */
  readonly config: Required<Omit<AxonServerConnectionConfig, "reconnectIntervalMs" | "maxReconnectAttempts">> & {
    reconnectIntervalMs: number
    maxReconnectAttempts: number
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
  }

  // Build gRPC channel credentials
  const sslConfig = config.ssl
  let credentials: ChannelCredentials | undefined

  if (sslConfig?.enabled) {
    const rootCerts = sslConfig.certFile ? readFileSync(sslConfig.certFile) : null
    const clientKey = sslConfig.clientKeyFile ? readFileSync(sslConfig.clientKeyFile) : null
    const clientCert = sslConfig.clientCertFile ? readFileSync(sslConfig.clientCertFile) : null
    credentials = GrpcChannelCredentials.createSsl(rootCerts, clientKey, clientCert) as ChannelCredentials
  }

  // gRPC channel options — keepalive to maintain persistent connections
  const channelOptions = {
    "grpc.keepalive_time_ms": config.keepAliveTimeMs ?? 30000,
    "grpc.keepalive_timeout_ms": config.keepAliveTimeoutMs ?? 10000,
    "grpc.keepalive_permit_without_calls": (config.keepAlivePermitWithoutCalls ?? true) ? 1 : 0,
  }

  // Build server address list for failover
  const serverAddresses = config.servers && config.servers.length > 0
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
    get channel() { return channel },
    get platform() { return clients.platform },
    get commands() { return clients.commands },
    get queries() { return clients.queries },
    get eventStore() { return clients.eventStore },
    get snapshotStore() { return clients.snapshotStore },
    config: resolvedConfig,

    get state() { return state },

    onReconnect(callback) {
      reconnectCallbacks.push(callback)
    },

    onDisconnect(callback) {
      disconnectCallbacks.push(callback)
    },

    close() {
      state = "closed"
      channel.close()
    },

    async reconnect() {
      if (state === "closed") {
        throw new Error("Connection is permanently closed")
      }
      if (state === "connected" || state === "connecting") return

      state = "reconnecting"
      const maxAttempts = resolvedConfig.maxReconnectAttempts
      let attempt = 0

      while (state === "reconnecting") {
        attempt++
        try {
          // Try the next server in the list on each reconnect attempt
          currentServerIndex++
          channel = createGrpcChannel()
          clients = createClients()
          state = "connected"

          // Notify listeners that we're back
          for (const cb of reconnectCallbacks) {
            try { cb() } catch { /* ignore listener errors */ }
          }
          return
        } catch (err) {
          if (maxAttempts > 0 && attempt >= maxAttempts) {
            state = "disconnected"
            throw new Error(
              `Failed to reconnect after ${attempt} attempts: ${err}`,
            )
          }

          // Exponential backoff: base interval * 2^attempt, capped at 30s
          const delay = Math.min(
            resolvedConfig.reconnectIntervalMs * Math.pow(2, attempt - 1),
            30000,
          )
          await new Promise((r) => setTimeout(r, delay))
        }
      }
    },
  }

  return connection
}
