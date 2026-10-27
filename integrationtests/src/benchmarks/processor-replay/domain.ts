/**
 * The log the replay benchmark reads, and the one type of it a projection wants.
 *
 * Ten event types in rotation, one handled: a processor that handles 10% of the
 * log, which is what a read model over a busy log usually is. Positions are
 * 0-based, so event `i` has type `i % 10` and the processor's token is `i + 1`
 * once it has handled it.
 */
import { z } from "zod"
import { emptyMetadata, event, generateIdentifier, qn, type EventMessage, type EventStore } from "@kronos-ts/core"

export const TYPES = 10
export const HANDLED_TYPE = 3
export const ACCOUNTS = 1000

const payload = z.object({ account: z.string(), amount: z.number() })

export const descriptors = Array.from({ length: TYPES }, (_, i) =>
  event({ name: qn("replay", `Type${i}`), payload, tags: { account: (p) => p.account } }),
)

/** The one descriptor the benchmark's handler is written for. */
export const Handled = descriptors[HANDLED_TYPE]!

export function fact(i: number): EventMessage {
  const account = `acc-${i % ACCOUNTS}`
  return {
    kind: "event",
    identifier: generateIdentifier(),
    name: descriptors[i % TYPES]!.name,
    version: "1.0",
    payload: { account, amount: i % 100 },
    metadata: emptyMetadata(),
    timestamp: 1_700_000_000_000 + i,
    tags: [{ key: "account", value: account }],
  }
}

/** Fill a log with `count` events, in order, `batch` per append. */
export async function seed(store: EventStore, count: number, batch = 5000): Promise<void> {
  for (let from = 0; from < count; from += batch) {
    const events: EventMessage[] = []
    for (let i = from; i < Math.min(from + batch, count); i++) events.push(fact(i))
    await store.append(events)
  }
}

/**
 * How many of the first `limit` events the handlers see, and the position after
 * the last of them. `all` is a processor that handles every type.
 */
export function expected(limit: number, all = false): { handled: number; lastToken: bigint } {
  if (all) return { handled: limit, lastToken: BigInt(limit) }
  let handled = 0
  let last = -1
  for (let i = HANDLED_TYPE; i < limit; i += TYPES) {
    handled++
    last = i
  }
  return { handled, lastToken: BigInt(last + 1) }
}
