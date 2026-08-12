import { createChannel, createClient, type Channel, type Client, type ChannelCredentials } from "nice-grpc"
import { ChannelCredentials as GrpcChannelCredentials } from "@grpc/grpc-js"
import { Metadata } from "nice-grpc"
import { readFileSync } from "node:fs"
import {
  kronosDbServiceDefinitions,
  PlatformServiceDefinition,
  CommandServiceDefinition,
  QueryServiceDefinition,
  EventStoreDefinition,
  SnapshotStoreDefinition,
} from "./service-definitions.js"

/**
 * Configuration for connecting to KronosDB.
 */
export interface KronosDbConnectionConfig {
  /** Host of the KronosDB server. Defaults to "localhost". */
  host?: string
  /** gRPC port of the KronosDB server. Defaults to 50051. */
  port?: number
  /**
   * Multiple server addresses for cluster deployments.
   * Each entry is `"host:port"`. When provided, overrides `host` and `port`.
   * The connector tries servers in order and fails over to the next on failure.
   */
  servers?: string[]
  /** The context to connect to. Defaults to "default". */
  context?: string
  /** Name identifying this component to KronosDB. */
  componentName: string
  /** Unique client identifier. Defaults to a generated UUID. */
  clientId?: string
  /** Access token for authentication. Optional. */
  token?: string
  /** Reconnection interval in ms. Defaults to 2000. */
  reconnectIntervalMs?: number
  /** Maximum reconnection attempts before giving up. 0 = unlimited. Defaults to 0. */
  maxReconnectAttempts?: number

  /** gRPC keepalive ping interval in ms. Default: 30000. */
  keepAliveTimeMs?: number
  /** gRPC keepalive timeout in ms. Default: 10000. */
  keepAliveTimeoutMs?: number
  /** Allow keepalive pings without active RPCs. Default: true. */
  keepAlivePermitWithoutCalls?: boolean

  /** TLS/SSL configuration. */
  ssl?: {
    enabled: boolean
    certFile?: string
    clientCertFile?: string
    clientKeyFile?: string
  }
}

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting" | "closed"

/**
 * An active connection to KronosDB, providing typed gRPC clients
 * for all services. Supports reconnection on failure.
 */
export interface KronosDbConnection {
  readonly channel: Channel
  /** Platform service client. */
  readonly platform: Client<typeof PlatformServiceDefinition>
  /** Command service client. */
  readonly commands: Client<typeof CommandServiceDefinition>
  /** Query service client. */
  readonly queries: Client<typeof QueryServiceDefinition>
  /** Event store client. */
  readonly eventStore: Client<typeof EventStoreDefinition>
  /** Snapshot store client. */
  readonly snapshotStore: Client<typeof SnapshotStoreDefinition>
  /** The resolved configuration. `servers` and `ssl` stay optional — they have no defaults. */
  readonly config: Omit<Required<KronosDbConnectionConfig>, "servers" | "ssl"> & {
    servers?: KronosDbConnectionConfig["servers"]
    ssl?: KronosDbConnectionConfig["ssl"]
  }
  readonly state: ConnectionState
  onReconnect(callback: () => void): void
  onDisconnect(callback: (error?: Error) => void): void
  close(): void
  reconnect(): Promise<void>
}

/**
 * Creates gRPC metadata for KronosDB requests.
 * Injects context and optional auth token as headers.
 */
export function createKronosMetadata(config: { context: string; token: string }): Metadata {
  const metadata = new Metadata()
  metadata.set("kronosdb-context", config.context)
  if (config.token) {
    metadata.set("kronosdb-token", config.token)
  }
  return metadata
}

/**
 * Connects to KronosDB and returns typed gRPC clients for all services.
 *
 * Configures gRPC channel-level keepalive to maintain persistent connections.
 */
export function connectToKronosDb(config: KronosDbConnectionConfig): KronosDbConnection {
  const serviceDefinitions = kronosDbServiceDefinitions
  const resolvedConfig = {
    host: config.host ?? "localhost",
    port: config.port ?? 50051,
    context: config.context ?? "default",
    componentName: config.componentName,
    clientId: config.clientId ?? crypto.randomUUID(),
    token: config.token ?? "",
    reconnectIntervalMs: config.reconnectIntervalMs ?? 2000,
    maxReconnectAttempts: config.maxReconnectAttempts ?? 0,
    keepAliveTimeMs: config.keepAliveTimeMs ?? 30000,
    keepAliveTimeoutMs: config.keepAliveTimeoutMs ?? 10000,
    keepAlivePermitWithoutCalls: config.keepAlivePermitWithoutCalls ?? true,
    servers: config.servers,
    ssl: config.ssl,
  }

  const sslConfig = config.ssl
  let credentials: ChannelCredentials | undefined

  if (sslConfig?.enabled) {
    const rootCerts = sslConfig.certFile ? readFileSync(sslConfig.certFile) : null
    const clientKey = sslConfig.clientKeyFile ? readFileSync(sslConfig.clientKeyFile) : null
    const clientCert = sslConfig.clientCertFile ? readFileSync(sslConfig.clientCertFile) : null
    credentials = GrpcChannelCredentials.createSsl(rootCerts, clientKey, clientCert) as ChannelCredentials
  }

  const channelOptions = {
    "grpc.keepalive_time_ms": config.keepAliveTimeMs ?? 30000,
    "grpc.keepalive_timeout_ms": config.keepAliveTimeoutMs ?? 10000,
    "grpc.keepalive_permit_without_calls": (config.keepAlivePermitWithoutCalls ?? true) ? 1 : 0,
  }

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
      platform: createClient(serviceDefinitions.platform, channel),
      commands: createClient(serviceDefinitions.commands, channel),
      queries: createClient(serviceDefinitions.queries, channel),
      eventStore: createClient(serviceDefinitions.eventStore, channel),
      snapshotStore: createClient(serviceDefinitions.snapshotStore, channel),
    }
  }

  let clients = createClients()

  const connection: KronosDbConnection = {
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
          currentServerIndex++
          channel = createGrpcChannel()
          clients = createClients()
          state = "connected"

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

          const backoff = Math.min(
            resolvedConfig.reconnectIntervalMs * Math.pow(2, attempt - 1),
            30000,
          )
          // ±25% jitter: after a server restart every instance of a scaled-out
          // service loses its connection at the same instant, and identical
          // backoff schedules would reconnect them as one synchronized wave.
          const delay = backoff * (0.75 + Math.random() * 0.5)
          await new Promise((r) => setTimeout(r, delay))
        }
      }
    },
  }

  return connection
}
