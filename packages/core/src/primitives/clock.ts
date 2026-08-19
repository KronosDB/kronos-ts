/**
 * A source of the current instant, in epoch milliseconds.
 *
 * One function, because one operation is one function type. It reads an
 * INSTANT, not a duration and not a monotonic tick: it is the same number a
 * message's `timestamp` carries, which is what makes a clock substitutable at
 * every message-birth site without converting anything.
 *
 * ```ts
 * const uow = unitOfWork()                       // system time
 * const frozen = unitOfWork(() => 1_700_000_000_000)
 * ```
 *
 * It enters through {@link import("../unit-of-work/unit-of-work.js").unitOfWork}
 * — the task — because a timestamp is a fact about WHEN a task ran, and every
 * message a task gives birth to should agree about that. `Date.now` is itself a
 * `Clock`, and is what a unit of work uses when none is handed in.
 */
export type Clock = () => number
