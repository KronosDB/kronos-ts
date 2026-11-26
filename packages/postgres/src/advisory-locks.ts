/**
 * Advisory locks for an append.
 *
 * An append takes an exclusive lock on every tag and every event type it
 * writes, and on every tag or type its condition reads by. Two appends that
 * share a tag or a type wait for each other; appends that share neither run
 * side by side.
 *
 * The locks are `pg_advisory_xact_lock`: held until the transaction commits or
 * rolls back, and released by the server. Session-scoped locks are never used,
 * because a pooler in transaction mode would leak them.
 */

import type { PostgresAdapterTransaction } from "./adapter.js"

const UNIT_SEPARATOR = "\u001f"

// FNV-1a 64-bit constants
const FNV_OFFSET_BASIS = 0xcbf29ce484222325n
const FNV_PRIME = 0x100000001b3n
const MASK_64 = (1n << 64n) - 1n
const SIGN_THRESHOLD = 1n << 63n

/**
 * FNV-1a 64-bit hash of the UTF-8 bytes of `input`, as a signed 64-bit value,
 * which is what `pg_advisory_xact_lock(BIGINT)` takes.
 */
export function hashLockKey(input: string): bigint {
  const bytes = new TextEncoder().encode(input)
  let h = FNV_OFFSET_BASIS
  for (let i = 0; i < bytes.length; i++) {
    h = (h ^ BigInt(bytes[i]!)) & MASK_64
    h = (h * FNV_PRIME) & MASK_64
  }
  return h >= SIGN_THRESHOLD ? h - (1n << 64n) : h
}

/** The lock of one encoded tag. */
export function tagKey(tag: string): bigint {
  return hashLockKey(`L${UNIT_SEPARATOR}${UNIT_SEPARATOR}${tag}`)
}

/** The lock of one event type. A type and a tag with the same text have different locks. */
export function typeKey(type: string): bigint {
  return hashLockKey(`T${UNIT_SEPARATOR}${type}`)
}

/**
 * Take each lock once, in ascending order of key. Every append takes its locks
 * in that order, so two appends that share several cannot deadlock.
 */
export async function acquireLocks(tx: PostgresAdapterTransaction, keys: ReadonlyArray<bigint>): Promise<void> {
  const sorted = [...new Set(keys)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))
  for (const key of sorted) await tx.query(`SELECT pg_advisory_xact_lock($1)`, [key])
}
