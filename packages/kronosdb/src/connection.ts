import { readFileSync } from "node:fs"
import { nativeLink, type NativeLink } from "./native-link.js"

/**
 * Configuration for connecting to KronosDB.
 */
export type KronosDbConnectionConfig = {
  /** Host of the KronosDB server. Defaults to "localhost". */
  host?: string
  /** Port of the KronosDB server: its `listen` address. Defaults to 50051. */
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
  /** Access token or JWT for authentication. Optional. */
  token?: string
  /** Reconnection interval in ms. Defaults to 2000. */
  reconnectIntervalMs?: number
  /** Maximum reconnection attempts before giving up. 0 = unlimited. Defaults to 0. */
  maxReconnectAttempts?: number
  /** Time allowed to connect and complete the handshake, per server, in ms. Default: 10000. */
  connectTimeoutMs?: number

  /**
   * TLS configuration. The server expects the TLS handshake first on its
   * `listen` port and has no plaintext fallback.
   */
  ssl?: {
    enabled: boolean
    /** CA certificate(s) to trust, as a PEM file. */
    certFile?: string
    clientCertFile?: string
    clientKeyFile?: string
  }
}

export type ConnectionState = "disconnected" | "connecting" | "connected" | "reconnecting" | "closed"

/**
 * An active connection to KronosDB: one native-protocol socket, carrying the
 * event store, the buses and the platform stream alike, opened on first use.
 * Supports reconnection on failure.
 */
export type KronosDbConnection = {
  /** The native protocol: the socket every request and stream rides. */
  readonly native: NativeLink
  /** The resolved configuration. `servers` and `ssl` stay optional — they have no defaults. */
  readonly config: Omit<Required<KronosDbConnectionConfig>, "servers" | "ssl"> & {
    servers?: KronosDbConnectionConfig["servers"]
    ssl?: KronosDbConnectionConfig["ssl"]
  }
  readonly state: ConnectionState
  /** Called once the connection is usable again after {@link reconnect}. */
  onReconnect(callback: () => void): void
  /** Called when {@link reconnect} begins, or the connection closes: long-lived streams end here. */
  onDisconnect(callback: (error?: Error) => void): void
  close(): void
  /**
   * Make sure there is a live, non-draining socket — connecting to the next
   * server if the last one is gone — and tell the long-lived streams to reopen
   * on it. A healthy socket is kept: it has a liveness check of its own.
   */
  reconnect(): Promise<void>
}

/**
 * Connects to KronosDB.
 *
 * Nothing is opened here: the socket connects when a store, bus or platform
 * stream first needs it, and again after it dies. TLS material is read once.
 */
export function connectToKronosDb(config: KronosDbConnectionConfig): KronosDbConnection {
  const resolvedConfig = {
    host: config.host ?? "localhost",
    port: config.port ?? 50051,
    context: config.context ?? "default",
    componentName: config.componentName,
    clientId: config.clientId ?? crypto.randomUUID(),
    token: config.token ?? "",
    reconnectIntervalMs: config.reconnectIntervalMs ?? 2000,
    maxReconnectAttempts: config.maxReconnectAttempts ?? 0,
    connectTimeoutMs: config.connectTimeoutMs ?? 10_000,
    servers: config.servers,
    ssl: config.ssl,
  }

  const serverAddresses = config.servers && config.servers.length > 0
    ? config.servers
    : [`${resolvedConfig.host}:${resolvedConfig.port}`]

  const link = nativeLink({
    addresses: serverAddresses,
    tls: config.ssl?.enabled
      ? {
          ca: config.ssl.certFile ? readFileSync(config.ssl.certFile) : undefined,
          cert: config.ssl.clientCertFile ? readFileSync(config.ssl.clientCertFile) : undefined,
          key: config.ssl.clientKeyFile ? readFileSync(config.ssl.clientKeyFile) : undefined,
        }
      : undefined,
    clientName: resolvedConfig.componentName,
    clientId: resolvedConfig.clientId,
    token: resolvedConfig.token,
    connectTimeoutMs: resolvedConfig.connectTimeoutMs,
  })

  let state: ConnectionState = "connected"
  let reconnectPromise: Promise<void> | undefined
  let retryTimer: ReturnType<typeof setTimeout> | undefined
  let finishRetryDelay: (() => void) | undefined

  const reconnectCallbacks: Array<() => void> = []
  const disconnectCallbacks: Array<(error?: Error) => void> = []

  const connection: KronosDbConnection = {
    native: link,
    config: resolvedConfig,

    get state() { return state },

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
      link.close()
    },

    reconnect() {
      if (state === "closed") return Promise.reject(new Error("Connection is permanently closed"))
      if (reconnectPromise) return reconnectPromise
      reconnectPromise = (async () => {
        state = "reconnecting"
        for (const callback of disconnectCallbacks) {
          try { callback() } catch { /* Listener failures cannot prevent recovery. */ }
        }
        let attempt = 0
        while (state === "reconnecting") {
          attempt++
          try {
            // Only report recovery once a socket has completed its handshake,
            // otherwise a dead server falsely "succeeds".
            await link.ready()
            if ((state as ConnectionState) === "closed") return
            state = "connected"
            for (const callback of reconnectCallbacks) {
              try { callback() } catch { /* Listener failures are isolated. */ }
            }
            return
          } catch (error) {
            if ((state as ConnectionState) === "closed") return
            if (resolvedConfig.maxReconnectAttempts > 0 && attempt >= resolvedConfig.maxReconnectAttempts) {
              state = "disconnected"
              throw new Error(`Failed to reconnect after ${attempt} attempts: ${error}`)
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

/**
 * A context-scoped VIEW of the shared connection: the same socket, a different
 * `config.context` — which is what the event store reads to bind its context on
 * it. One connection serves N contexts with no per-context socket.
 */
export function contextView(connection: KronosDbConnection, context: string): KronosDbConnection {
  if (context === connection.config.context) return connection
  return {
    get native() { return connection.native },
    config: { ...connection.config, context },
    get state() { return connection.state },
    onReconnect: (cb) => connection.onReconnect(cb),
    onDisconnect: (cb) => connection.onDisconnect(cb),
    // The socket belongs to the connection handle, not to a context view.
    // Closing one context must not take the other eighteen down with it.
    close: () => {},
    reconnect: () => connection.reconnect(),
  }
}
