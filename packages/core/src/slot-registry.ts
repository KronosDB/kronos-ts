import type { KronosComponents, SlotName } from "./components.js"

/**
 * Optional metadata attached to a slot entry. Today the only field is
 * `inMemory` + `warning` (D-51) — used by Plan 02 to emit startup warnings
 * for any slot still using a flagged in-memory default at .start() time.
 */
export interface SlotMeta {
  inMemory?: true
  warning?: string
}

/** Each slot's resolution callable: a factory that takes the destructured Resolved and returns the component. */
export type SlotFactory<K extends SlotName> = (resolved: KronosComponents) => KronosComponents[K]

/** A registered slot entry: normalized factory + optional meta. */
export interface SlotEntry<K extends SlotName = SlotName> {
  factory: SlotFactory<K>
  meta?: SlotMeta
}

/**
 * Normalize a factory-or-instance argument into a SlotFactory.
 * SLT-03: plain instances (non-function values) become `() => instance` internally.
 */
function normalizeFactory<K extends SlotName>(
  factoryOrInstance: SlotFactory<K> | KronosComponents[K],
): SlotFactory<K> {
  if (typeof factoryOrInstance === "function") {
    return factoryOrInstance as SlotFactory<K>
  }
  const instance = factoryOrInstance
  return () => instance
}

/**
 * SlotRegistry — the storage backing the App's three verbs.
 * setDefault: ifAbsent (no-op if occupied)
 * set:        warn on double-set
 * forceSet:   silent overwrite
 * (DESIGN.md §6, REQUIREMENTS.md SLT-02.)
 */
export class SlotRegistry {
  private readonly slots = new Map<SlotName, SlotEntry>()

  setDefault<K extends SlotName>(
    slot: K,
    factoryOrInstance: SlotFactory<K> | KronosComponents[K],
    meta?: SlotMeta,
  ): void {
    if (this.slots.has(slot)) return
    this.slots.set(slot, { factory: normalizeFactory(factoryOrInstance) as SlotFactory<SlotName>, meta })
  }

  set<K extends SlotName>(
    slot: K,
    factoryOrInstance: SlotFactory<K> | KronosComponents[K],
  ): void {
    const existing = this.slots.get(slot)
    // Provenance check: setDefault always writes a `meta` key (even when its value
    // is undefined for stateless defaults like serializer / unitOfWorkFactory / tagResolver);
    // set / forceSet never write the key at all. So `"meta" in existing` identifies a prior
    // setDefault — those overrides are the expected "extension overrides in-memory default" path
    // and should NOT warn. Genuine collisions (two set/forceSet calls on the same slot) DO warn.
    if (existing && !("meta" in existing)) {
      console.warn(
        `[kronos] slot "${slot}" override: already set. Use forceSet() to suppress this warning.`,
      )
    }
    this.slots.set(slot, { factory: normalizeFactory(factoryOrInstance) as SlotFactory<SlotName> })
  }

  forceSet<K extends SlotName>(
    slot: K,
    factoryOrInstance: SlotFactory<K> | KronosComponents[K],
  ): void {
    this.slots.set(slot, { factory: normalizeFactory(factoryOrInstance) as SlotFactory<SlotName> })
  }

  /** @internal — used by buildResolved and by Plan 02's warning emitter. */
  getEntry<K extends SlotName>(slot: K): SlotEntry<K> | undefined {
    return this.slots.get(slot) as SlotEntry<K> | undefined
  }

  /** @internal — used by Plan 02 to iterate registered slots for startup warnings. */
  has(slot: SlotName): boolean {
    return this.slots.has(slot)
  }
}
