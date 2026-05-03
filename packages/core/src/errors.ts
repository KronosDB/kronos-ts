import type { SlotName } from "./components.js"
import type { DecoratorHandle } from "./decorator.js"

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

/**
 * Thrown when `app.commandGateway` or `app.queryGateway` is accessed before
 * the `register` lifecycle stage completes during `.start()`. Available inside
 * `onStart('warmup'|'register'|'processors'|'serve', fn)` hooks AFTER register
 * completes, and after `.start()` resolves. (Plan 08-01.)
 */
export class AppNotStartedError extends Error {
  constructor(accessor: string) {
    super(
      `[kronos] App not started: ${accessor} is only accessible after register-stage completes during .start().`,
    )
    this.name = "AppNotStartedError"
  }
}

/**
 * Thrown by `app.removeDecorator(handle)` when the handle is not found in the
 * app's registration list (D-59). Catches typos like
 * `Defaults.commandBus.interceptingg` and removal of a handle that was never
 * registered (e.g., framework default whose factory hasn't been wired yet).
 */
export class UnknownDecoratorHandleError extends Error {
  readonly slot: SlotName
  readonly handleName: string
  constructor(handle: DecoratorHandle<SlotName>) {
    super(
      `[kronos] Unknown decorator handle "${handle.__name}" for slot "${handle.__slot}". ` +
        `Either it was never registered or it was already removed.`,
    )
    this.name = "UnknownDecoratorHandleError"
    this.slot = handle.__slot
    this.handleName = handle.__name
  }
}
