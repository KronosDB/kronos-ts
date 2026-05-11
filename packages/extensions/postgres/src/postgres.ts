/**
 * postgres(config) — Extension factory for @kronos-ts/postgres.
 *
 * Populates two slots (D-12.01):
 *   - eventStore     : EventStorageEngine via createPostgresEventStore
 *   - snapshotStore  : SnapshotStore via createPostgresSnapshotStore
 *
 * Lifecycle (mirrors @kronos-ts/kronosdb extension shape):
 *   - app.onStart("connect", ...) — adapter.connect() with withRetry; then
 *     bootstrapSchema (skip if config.bootstrap === false so users running
 *     their own migration tooling are not surprised)
 *   - app.onStop("connect", ...)  — adapter.disconnect()
 *
 * Does NOT populate eventBus, commandBus, queryBus, tokenStore, or
 * transactionManager (D-12.01 — out of scope for this extension).
 */

import type { App } from "@kronos-ts/core"
import type { ResilienceConfig } from "@kronos-ts/common"
import { withRetry } from "@kronos-ts/common"
import type { PostgresAdapter } from "./adapter.js"
import { createPostgresEventStore } from "./postgres-event-store.js"
import { createPostgresSnapshotStore } from "./postgres-snapshot-store.js"
import { bootstrapSchema, DEFAULT_TABLE_NAMES, type TableNames } from "./schema.js"

export interface PostgresConfig {
  /** Driver adapter — pg / postgres / bun-sql / custom. Created by the user. */
  readonly adapter: PostgresAdapter
  /** Auto-create the schema on connect. Defaults to true. Set false when
   *  running your own migrations. */
  readonly bootstrap?: boolean
  /** Override default table names (kronos_events / kronos_snapshots). */
  readonly tableNames?: TableNames
  /** Retry policy for the initial connect + bootstrap. Defaults to
   *  framework defaults via withRetry. */
  readonly resilience?: Partial<ResilienceConfig>
}

export function postgres(config: PostgresConfig): (app: App) => void {
  const { adapter, resilience } = config
  const bootstrap = config.bootstrap ?? true
  const tables = config.tableNames ?? DEFAULT_TABLE_NAMES

  return (app: App) => {
    app.set("eventStore", ({ serializer, tagResolver }) =>
      createPostgresEventStore({ adapter, serializer, tagResolver, tableNames: tables }),
    )
    app.set("snapshotStore", ({ serializer }) =>
      createPostgresSnapshotStore({ adapter, serializer, tableNames: tables }),
    )

    app.onStart("connect", async () => {
      await withRetry(() => adapter.connect(), { event: "initial-connect", ...resilience })
      if (bootstrap) {
        await withRetry(() => bootstrapSchema(adapter, { tableNames: tables }), {
          event: "initial-connect",
          ...resilience,
        })
      }
    })

    app.onStop("connect", async () => {
      await adapter.disconnect()
    })
  }
}
