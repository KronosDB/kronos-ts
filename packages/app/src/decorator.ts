import type { Resolved } from "./resolved.js"
import type { KronosComponents, SlotName } from "./components.js"

/**
 * Slot-typed handle returned by `app.decorate()`. The `__slot` brand prevents
 * cross-slot removal at compile time (DEC-03). `__id` is the runtime identity
 * key used by `removeDecorator()` to locate the registration.
 *
 * Framework defaults expose pre-allocated handle constants on `Defaults` so
 * users can write `app.removeDecorator(Defaults.commandBus.intercepting)`.
 */
export interface DecoratorHandle<K extends SlotName> {
  readonly __slot: K
  readonly __id: symbol
  readonly __name: string
}

/**
 * A decorator factory wraps the slot's resolved value. Polymorphic over the
 * slot interface (DEC-04) — receives `KronosComponents[K]` and returns
 * `KronosComponents[K]`. The `resolved` proxy is the same lazy proxy passed
 * to slot factories (Phase 5 D-52); decorators may pull sibling slots through
 * it (cycle detection covers this).
 */
export type DecoratorFactory<K extends SlotName> = (
  inner: KronosComponents[K],
  resolved: Resolved,
) => KronosComponents[K]

/**
 * Internal accumulator entry. Stored on `AppState.decoratorRegistrations` in
 * registration order — pipeline order at `.start()` is left-to-right
 * (last registered = outermost wrap, per D-61 / DESIGN.md §8).
 *
 * `frameworkDefault` distinguishes framework-registered defaults (Plan 02)
 * from user-registered decorators (D-62 — user decorators wrap OUTSIDE
 * framework defaults). The `.start()` decoration step partitions on this
 * field and applies framework defaults first (innermost), then user
 * decorators (outermost).
 */
export interface DecoratorEntry<K extends SlotName = SlotName> {
  readonly handle: DecoratorHandle<K>
  readonly factory: DecoratorFactory<K>
  readonly frameworkDefault: boolean
}

/**
 * @internal — used by `Defaults` and by `kronos()` bootstrap (Plan 02) to
 * mint pre-allocated framework-default handles with stable identity.
 */
export function makeFrameworkHandle<K extends SlotName>(
  slot: K,
  name: string,
): DecoratorHandle<K> {
  return Object.freeze({
    __slot: slot,
    __id: Symbol(`${slot}.${name}`),
    __name: name,
  }) as DecoratorHandle<K>
}

/**
 * Apply all decorator registrations for a given slot in two passes:
 * 1. Framework defaults first (innermost, handler-adjacent)
 * 2. User decorators after (outer; last .decorate() = outermost wrap)
 *
 * Both passes iterate `registrations` in registration order so a
 * left-to-right reduce composes correctly (each factory wraps the current value).
 */
export function applyDecorators<K extends SlotName>(
  slot: K,
  base: KronosComponents[K],
  registrations: ReadonlyArray<DecoratorEntry>,
  resolved: Resolved,
): KronosComponents[K] {
  let current = base
  // Pass 1: framework defaults (innermost)
  for (const reg of registrations) {
    if (reg.handle.__slot !== slot || !reg.frameworkDefault) continue
    current = (reg.factory as unknown as DecoratorFactory<K>)(current, resolved)
  }
  // Pass 2: user decorators (outer)
  for (const reg of registrations) {
    if (reg.handle.__slot !== slot || reg.frameworkDefault) continue
    current = (reg.factory as unknown as DecoratorFactory<K>)(current, resolved)
  }
  return current
}
