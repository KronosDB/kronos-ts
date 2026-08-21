import type { AxonServerConnectionConfig, AxonServerConnection } from "./connection.js"
import { connectToAxonServer } from "./connection.js"

/**
 * Manages Axon Server connections across multiple contexts.
 *
 * Creates and caches one connection per context. Used by the multi-tenancy
 * extension to maintain separate channels for each tenant's context.
 *
 * Aligned with Java's `AxonServerConnectionManager`.
 *
 * ```typescript
 * const manager = connectionManager({
 *   componentName: "my-app",
 *   host: "axon-server",
 *   port: 8124,
 * })
 *
 * const defaultConn = manager.getConnection("default")
 * const tenantConn = manager.getConnection("tenant-a")
 *
 * await manager.disconnectAll()
 * ```
 */
export type AxonServerConnectionManager = {
  /**
   * Get or create a connection for the given context.
   * Connections are created lazily and cached.
   */
  getConnection(context: string): AxonServerConnection

  /**
   * Disconnect a specific context's connection.
   */
  disconnect(context: string): void

  /**
   * Disconnect all cached connections.
   */
  disconnectAll(): void

  /**
   * List all active context names.
   */
  activeContexts(): string[]
}

/**
 * Creates a connection manager that lazily creates per-context connections.
 *
 * The base config (host, port, SSL, etc.) is shared across all contexts.
 * Only the `context` field varies per connection.
 */
export function connectionManager(
  baseConfig: Omit<AxonServerConnectionConfig, "context">,
): AxonServerConnectionManager {
  const connections = new Map<string, AxonServerConnection>()

  return {
    getConnection(context: string): AxonServerConnection {
      let connection = connections.get(context)
      if (!connection) {
        connection = connectToAxonServer({ ...baseConfig, context })
        connections.set(context, connection)
      }
      return connection
    },

    disconnect(context: string): void {
      const connection = connections.get(context)
      if (connection) {
        connection.close()
        connections.delete(context)
      }
    },

    disconnectAll(): void {
      for (const connection of connections.values()) {
        connection.close()
      }
      connections.clear()
    },

    activeContexts(): string[] {
      return [...connections.keys()]
    },
  }
}
