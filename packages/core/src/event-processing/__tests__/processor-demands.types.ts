/**
 * THE TYPE TEST FOR THE PROCESSOR'S OWN DEMAND: A QUEUE IMPLIES A LANE.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not subject
 * to `exclude`, so it is judged by `tsc --noEmit`. A `@ts-expect-error` that
 * stops erroring turns that gate red.
 *
 * What it pins: `eventProcessor({ deadLetterQueue })` WITHOUT a `sequence` does
 * not compile. Parking is a lane operation — the queue holds a failed event and
 * everything behind it in the same lane — so "which lane" stops being optional
 * the moment there is a queue.
 *
 * This used to be a `throw` in the constructor. It was honest, and it was late:
 * a composition root runs at boot, so the mistake was found by starting the
 * process rather than by building it. The throw is still there for JavaScript
 * callers, one line, exactly as `repository.ts` keeps its one line.
 */
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemoryTokenStore } from "../token-store.js"
import { inMemoryDeadLetterQueue } from "../dead-letter-queue.js"
import { eventProcessor } from "../processor.js"
import { sequentialPerTag } from "../sequence.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"

const eventStore = inMemoryEventStore()
const tokenStore = inMemoryTokenStore()
const deadLetterQueue = inMemoryDeadLetterQueue()
const lane = sequentialPerTag("courseId")

// ---------------------------------------------------------------------------
// ALL FOUR QUADRANTS of (queue?) × (lane?).
// ---------------------------------------------------------------------------

/** NEITHER ✓ — a projection in global stream order, which is most of them. */
export const plainProjection = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore,
  unitOfWork,
})

/** LANE ONLY ✓ — ordered per entity, failures propagate and retry. */
export const lanedNoQueue = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore,
  unitOfWork,
  sequence: lane,
})

/** BOTH ✓ — the arrangement the demand exists to require. */
export const lanedWithQueue = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore,
  unitOfWork,
  sequence: lane,
  deadLetterQueue,
})

/**
 * QUEUE ONLY ✗ — THE HEADLINE.
 *
 * The demand adds a REQUIRED `sequence` whose type is an anonymous ERROR/FIX
 * record, so the compiler reports a missing property and prints its structure —
 * which means the keys ARE the message and they arrive at the wiring site. The
 * wording is GENERAL, because core is certain about the rule and about nothing
 * else; `sequentialPerTag` is core's own export, so naming it is not core
 * guessing at anybody's stack.
 */
export const queueWithoutLane = eventProcessor(
  // @ts-expect-error — a deadLetterQueue without a sequence: parking is a lane operation
  {
    name: "courses",
    eventStore,
    tokenStore,
    unitOfWork,
    deadLetterQueue,
  },
)

/**
 * AND THE DEMAND READS THE CONFIG, NOT THE TYPE. A queue arriving through a
 * variable is still a queue, so the refusal does not depend on the property
 * being written inline.
 */
const queue = deadLetterQueue
export const queueViaVariable = eventProcessor(
  // @ts-expect-error — same mistake, one indirection later
  {
    name: "courses",
    eventStore,
    tokenStore,
    unitOfWork,
    deadLetterQueue: queue,
  },
)
