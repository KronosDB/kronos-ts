import type { SlotName } from "./components.js"

/**
 * Thrown when buildResolved detects a cycle: a factory's getter on Resolved re-enters
 * a slot that is currently being resolved. (D-52 cycle-detection contract.)
 */
export class CircularSlotDependencyError extends Error {
  readonly slot: SlotName
  readonly chain: readonly SlotName[]
  constructor(slot: SlotName, chain: readonly SlotName[]) {
    super(`[kronos] Circular slot dependency: "${slot}" via [${chain.join(" → ")}]`)
    this.name = "CircularSlotDependencyError"
    this.slot = slot
    this.chain = chain
  }
}

/** Thrown when a Resolved getter touches a slot that has no registered factory. */
export class SlotNotRegisteredError extends Error {
  readonly slot: SlotName
  constructor(slot: SlotName) {
    super(`[kronos] Slot "${slot}" has no registered factory.`)
    this.name = "SlotNotRegisteredError"
    this.slot = slot
  }
}
