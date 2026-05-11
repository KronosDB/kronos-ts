/**
 * Advisory-lock taxonomy for the DCB conflict check (D-12.09/10/11).
 *
 * Three keyspaces, S/X-asymmetric:
 *
 *   Leaf          K(T, t)    writer = X-lock,        reader = S-lock
 *   Type-intent   K(T, ε)    writer = S-lock,        reader = X-lock
 *   Global-intent K(ε, ε)    writer = S-lock,        reader = X-lock
 *
 * The asymmetry lets writers on DISJOINT (T,t) tuples run in parallel
 * (their leaf X-locks don't conflict, and they only take S on the intent
 * keys — multiple S-holders coexist). A Query.all() reader (rare) takes
 * X on the intent keys, briefly blocking ALL writers — but that's the
 * point: Query.all() needs to see a consistent snapshot.
 *
 * Locks are pg_advisory_xact_lock variants: held until tx commit/rollback,
 * automatically released. NEVER use session-scoped locks here — PgBouncer
 * in transaction-pooling mode would leak them.
 *
 * Reimplemented from principles per D-12.11. The kraken-tech version is
 * a reference for the taxonomy and FNV-1a hash; the SQL and serialisation
 * format below are original.
 */

import type { PostgresAdapterTransaction } from "./adapter.js"

const UNIT_SEPARATOR = "" // ASCII Unit Separator (U+001F) — prevents tuple-collision hashing
const KEYSPACE_LEAF = "L"
const KEYSPACE_TYPE_INTENT = "T"
const KEYSPACE_GLOBAL_INTENT = "G"

// FNV-1a 64-bit constants
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const MASK_64 = (1n << 64n) - 1n
const SIGN_THRESHOLD = 1n << 63n

/**
 * FNV-1a 64-bit hash. Returns a BIGINT in the signed 64-bit range so it can
 * be passed directly to `pg_advisory_xact_lock(BIGINT)`. Values with the
 * high bit set are returned as negative numbers (two's complement).
 *
 * Per FNV-1a: h = offset_basis; for each byte b: h = (h XOR b) * prime mod 2^64.
 * UTF-8 byte iteration via TextEncoder for predictable cross-runtime behaviour.
 */
export function hashLockKey(input: string): bigint {
  const bytes = new TextEncoder().encode(input)
  let h = FNV_OFFSET_BASIS
  for (let i = 0; i < bytes.length; i++) {
    h = (h ^ BigInt(bytes[i]!)) & MASK_64
    h = (h * FNV_PRIME) & MASK_64
  }
  // Reinterpret as signed 64-bit
  return h >= SIGN_THRESHOLD ? h - (1n << 64n) : h
}

export type LockKeyspace = "leaf" | "type-intent" | "global-intent"

export function leafKey(type: string, tag: string): bigint {
  return hashLockKey(`${KEYSPACE_LEAF}${UNIT_SEPARATOR}${type}${UNIT_SEPARATOR}${tag}`)
}

export function typeIntentKey(type: string): bigint {
  return hashLockKey(`${KEYSPACE_TYPE_INTENT}${UNIT_SEPARATOR}${type}`)
}

export function globalIntentKey(): bigint {
  return hashLockKey(KEYSPACE_GLOBAL_INTENT)
}

export interface LockTarget {
  readonly type: string
  readonly tag: string
}

/**
 * Writer pattern: X-lock on each unique leaf, S-lock on each unique
 * type-intent + the global-intent. Writers on disjoint leaves run in
 * parallel; writers sharing a leaf serialise.
 *
 * Locks are issued in a stable order (sorted by key value) to avoid
 * deadlocks between concurrent writers that share multiple leaves.
 */
export async function acquireWriteLocks(
  tx: PostgresAdapterTransaction,
  targets: ReadonlyArray<LockTarget>,
): Promise<void> {
  if (targets.length === 0) {
    // Even with no leaf targets, acquire the global-intent S-lock so that
    // a Query.all() X on the global-intent can block us if needed.
    await tx.query(`SELECT pg_advisory_xact_lock_shared($1)`, [globalIntentKey()])
    return
  }

  const leafKeys = uniqueSorted(targets.map((t) => leafKey(t.type, t.tag)))
  const typeIntentKeys = uniqueSorted([...new Set(targets.map((t) => t.type))].map(typeIntentKey))

  // X on leaves (sorted for deadlock-free acquisition order)
  for (const k of leafKeys) {
    await tx.query(`SELECT pg_advisory_xact_lock($1)`, [k])
  }
  // S on type-intent
  for (const k of typeIntentKeys) {
    await tx.query(`SELECT pg_advisory_xact_lock_shared($1)`, [k])
  }
  // S on global-intent
  await tx.query(`SELECT pg_advisory_xact_lock_shared($1)`, [globalIntentKey()])
}

/**
 * Reader pattern (Query.all): S on leaves, X on type-intent + global-intent.
 * Inverse of writer; briefly excludes all writers while held.
 */
export async function acquireReadLocks(
  tx: PostgresAdapterTransaction,
  targets: ReadonlyArray<LockTarget>,
): Promise<void> {
  if (targets.length === 0) {
    // A truly empty Query.all() still needs the global-intent X to see a
    // consistent snapshot across all types.
    await tx.query(`SELECT pg_advisory_xact_lock($1)`, [globalIntentKey()])
    return
  }

  const leafKeys = uniqueSorted(targets.map((t) => leafKey(t.type, t.tag)))
  const typeIntentKeys = uniqueSorted([...new Set(targets.map((t) => t.type))].map(typeIntentKey))

  for (const k of leafKeys) {
    await tx.query(`SELECT pg_advisory_xact_lock_shared($1)`, [k])
  }
  for (const k of typeIntentKeys) {
    await tx.query(`SELECT pg_advisory_xact_lock($1)`, [k])
  }
  await tx.query(`SELECT pg_advisory_xact_lock($1)`, [globalIntentKey()])
}

function uniqueSorted(xs: ReadonlyArray<bigint>): bigint[] {
  return [...new Set(xs)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
}
