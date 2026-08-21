// ---------------------------------------------------------------------------
// PERSISTENCE FAMILIES — the slot, owned by core; the occupants, owned by the
// six adapter packages.
//
// THE PRINCIPLE THIS ENFORCES WAS ALWAYS WRITTEN DOWN. "Persistence families
// are keyed by TRANSACTION IDENTITY — the token store and the dead-letter queue
// must write through the same client handle the handlers write through. Never
// mix families within one processor." It was a sentence in a document, which
// means it was a sentence somebody could not read at 2am while wiring a
// processor out of two packages that both export a `tokenStore`.
//
// The failure it prevents is the worst-shaped one there is. A drizzle token
// store handed a postgres unit of work does not throw: it looks for ITS
// transaction on the task, does not find one, and falls back to its plain
// handle — so the token update commits OUTSIDE the batch's transaction. Every
// test passes. Then a crash lands between the projection write and the token
// write, and a read model is permanently wrong in a way nobody can reconstruct.
//
// So the rule stops being prose and becomes a type.
// ---------------------------------------------------------------------------

/**
 * THE SLOT. A phantom key that every family's unit-of-work brand hangs on, so
 * two brands are mutually exclusive rather than merely different.
 *
 * `declare const` — it is AMBIENT. No JavaScript declares it, no JavaScript
 * reads it, and nothing is ever constructed with it: the family decorators
 * assert their return type rather than writing a property. It is not exported
 * either, so no module can import a value that does not exist at runtime; what
 * IS exported is {@link PersistenceFamily}, which is a type and nothing else.
 */
declare const persistenceFamily: unique symbol

/**
 * A unit of work's PERSISTENCE FAMILY — the mark a `<pkg>UnitOfWork` decorator
 * leaves on what it mints, and the thing that package's stores demand back.
 *
 * CORE OWNS THE SLOT AND KNOWS NO OCCUPANTS, which is the same division that
 * governs every other capability here: core knows what a family MEANS (a
 * transaction identity two components must agree on) and each package knows
 * which one it IS. Nothing in core mentions drizzle, and nothing in drizzle
 * needs core's permission to exist.
 *
 * ```ts
 * // in @kronos-ts/drizzle, once:
 * export type DrizzleFamily = PersistenceFamily<
 *   "drizzle",
 *   "build this processor's unitOfWork with drizzleUnitOfWork(next, db)"
 * >
 * ```
 *
 * IT IS ERASED, AND IT IS NEVER CONSTRUCTED. The brand is a phantom on a
 * unique-symbol key; the decorator returns its inner value and asserts the
 * branded type, so the emitted JavaScript is byte for byte what it was before
 * any of this existed. Nothing at runtime can observe a family.
 *
 * `Fix` IS THE DIAGNOSTIC, and it lives here because of where the certainty is.
 * A mismatch between two families is reported as two incompatible string
 * literals, and TypeScript prints them — so the sentence the author of the
 * DEMANDING package wrote is the sentence that arrives at the wiring site.
 * Core could only have said something general; the package that owns the store
 * knows exactly which factory the host should have called, so it says so.
 */
export type PersistenceFamily<Name extends string, Fix extends string> = {
  readonly [persistenceFamily]: {
    /**
     * FIRST, so it is the member TypeScript reports on. The checker drills to
     * the first incompatible property of a mismatched object type, and this is
     * the one worth reading — the family name below is a label, this is an
     * instruction.
     */
    readonly FIX: Fix
    readonly family: Name
  }
}
