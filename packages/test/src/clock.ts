// ---------------------------------------------------------------------------
// Time a test can move.
// ---------------------------------------------------------------------------

/** Milliseconds. The unit a clock reads and `timestamp` carries. */
export type Duration = number

/**
 * The instant an {@link advanceableClock} starts at when nobody says otherwise.
 *
 * A FIXED instant rather than the wall clock, because a timestamp a test can
 * predict is worth more than one that is technically current: an assertion can
 * name `FIXTURE_EPOCH + 30_000` and mean it.
 */
export const FIXTURE_EPOCH = Date.UTC(2024, 0, 1)

/** A CLOCK YOU CAN MOVE — a clock, plus the one verb that moves it. */
export type AdvanceableClock = (() => number) & {
  /** Move time forward. What was due before is due now. */
  advance(duration: Duration): void
}

/**
 * A clock that starts at `from` and moves only when you say so. Build the app's
 * tasks on it — `() => unitOfWork(clock)` — and give it to the event store's
 * recorder — `recordingEventStore(store, { clock })` — whose deadlines then wait
 * for it.
 *
 * It is a CLOSURE over its own offset: everything that captured the arrow sees
 * a move, and whatever captured a NUMBER is frozen where it was built.
 */
export function advanceableClock(from: number = FIXTURE_EPOCH): AdvanceableClock {
  let offset = 0
  const read = () => from + offset
  return Object.assign(read, {
    advance(duration: Duration): void {
      offset += duration
    },
  })
}
