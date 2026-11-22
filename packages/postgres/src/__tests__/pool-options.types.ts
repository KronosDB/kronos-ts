/**
 * The TYPE test for which options `postgresPool` accepts beside each source.
 * Listed in the root `tsconfig.json` `files` array, so `bun run typecheck`
 * judges it.
 *
 * What it pins: the session timeouts are accepted beside a connection string,
 * and refused beside an adapter, where they would configure nothing.
 */
import type { PostgresAdapter } from "../adapter.js"
import { postgresPool, type PostgresPoolOptions } from "../postgres-pool.js"

declare const adapter: PostgresAdapter
declare const source: string | PostgresAdapter
declare const withTimeouts: PostgresPoolOptions & { readonly statementTimeoutMs: number }

export const fromConnectionString = postgresPool("postgresql://localhost/db", {
  idleInTransactionTimeoutMs: 15_000,
  statementTimeoutMs: 60_000,
})

export const fromAdapter = postgresPool(adapter, { bootstrap: false, resilience: { maxAttempts: 3 } })

// @ts-expect-error — a session timeout is the adapter's option, not the pool's
export const literal = postgresPool(adapter, { idleInTransactionTimeoutMs: 15_000 })

// @ts-expect-error — and an options value that carries one is refused too
export const carried = postgresPool(adapter, withTimeouts)

// @ts-expect-error — a source that may be an adapter takes no session timeout
export const either = postgresPool(source, { statementTimeoutMs: 60_000 })

export const eitherPlain = postgresPool(source, { bootstrap: false })
