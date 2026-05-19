import type { KronosComponents, SlotName } from "./components.js"
import { CircularSlotDependencyError, SlotNotRegisteredError } from "./errors.js"
import type { SlotRegistry } from "./slot-registry.js"

/**
 * Resolved is structurally identical to KronosComponents — the destructured arg
 * passed to factories. We re-export the alias so user code reads `Resolved` not `KronosComponents`.
 */
export type Resolved = KronosComponents

/**
 * Build a lazy + memoized Resolved Proxy backed by the registry. (D-52.)
 *
 * - Each property access invokes the slot's factory once and memoizes
 * - Factories receive the same proxy as their argument so dependency chains compose
 * - A `resolving` Set detects cycles: if a getter is hit while its slot is mid-resolution,
 *   throw CircularSlotDependencyError with the offending chain
 * - try/finally ensures `resolving` is cleaned even after errors so subsequent accesses still work
 */
export function buildResolved(registry: SlotRegistry): Resolved {
  const cache = new Map<SlotName, unknown>()
  const resolving = new Set<SlotName>()
  const resolvingOrder: SlotName[] = []

  const handler: ProxyHandler<object> = {
    get(_target, prop) {
      const slot = prop as SlotName
      if (cache.has(slot)) return cache.get(slot)

      if (resolving.has(slot)) {
        throw new CircularSlotDependencyError(slot, [...resolvingOrder, slot])
      }

      const entry = registry.getEntry(slot)
      if (!entry) {
        throw new SlotNotRegisteredError(slot)
      }

      resolving.add(slot)
      resolvingOrder.push(slot)
      try {
        const value = entry.factory(proxy as Resolved)
        cache.set(slot, value)
        return value
      } finally {
        resolving.delete(slot)
        resolvingOrder.pop()
      }
    },
  }

  const proxy = new Proxy({}, handler) as Resolved
  return proxy
}
