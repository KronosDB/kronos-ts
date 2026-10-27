/**
 * THE TYPE TEST FOR THE PROCESSOR'S OWN DEMANDS.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not subject
 * to `exclude`, so it is judged by `tsc --noEmit`. A `@ts-expect-error` that
 * stops erroring turns that gate red.
 *
 * What it pins:
 * - `U` is read off the unit-of-work FACTORY, and the token store and the
 *   dead-letter queue are CHECKED against it: a store keyed on a task the
 *   factory does not mint does not compile.
 * - A dead-letter queue cannot be given without the buckets it parks under.
 *   That used to be a demand on a config object; `deadLettering(p, queue,
 *   bucketOf)` makes the bucket a required argument, so there is nothing left
 *   to demand.
 */
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"
import { inMemoryTokenStore, type TokenStore } from "../token-store.js"
import { inMemoryDeadLetterQueue, type SequencedDeadLetterQueue } from "../dead-letter-queue.js"
import { batched, concurrent, deadLettering, eventProcessor, type EventProcessor } from "../processor.js"
import { sequentialPerTag } from "../sequence.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"

const eventStore = inMemoryEventStore()
const tokenStore = inMemoryTokenStore()
const deadLetterQueue = inMemoryDeadLetterQueue()
const byCourse = sequentialPerTag("courseId")

/** A task that carries something the bare one does not — a family's, say. */
type FamilyUnitOfWork = UnitOfWork & { readonly family: "postgres" }
declare const familyUnitOfWork: () => FamilyUnitOfWork
declare const familyTokenStore: TokenStore<FamilyUnitOfWork>
declare const familyQueue: SequencedDeadLetterQueue<FamilyUnitOfWork>

// ---------------------------------------------------------------------------
// The four parts, and the wrappers over them.
// ---------------------------------------------------------------------------

/** ✓ A projection in global stream order, which is most of them. */
export const plainProjection: EventProcessor = eventProcessor("courses", eventStore, tokenStore, unitOfWork)

/** ✓ Every wrapper, composed. */
export const wrapped: EventProcessor = deadLettering(
  concurrent(batched(eventProcessor("courses", eventStore, tokenStore, unitOfWork), 1000), byCourse),
  deadLetterQueue,
  byCourse,
)

// @ts-expect-error — a queue without the buckets it parks under
export const queueWithoutBuckets = deadLettering(plainProjection, deadLetterQueue)

// ---------------------------------------------------------------------------
// `U` comes from the factory; the stores are checked against it.
// ---------------------------------------------------------------------------

/** ✓ The family's store with the family's factory: `U` is the family's task. */
export const family: EventProcessor<FamilyUnitOfWork> = eventProcessor("courses", eventStore, familyTokenStore, familyUnitOfWork)

/** ✓ A store that takes any task lives with a family's factory too. */
export const anyStore: EventProcessor<FamilyUnitOfWork> = eventProcessor("courses", eventStore, tokenStore, familyUnitOfWork)

// @ts-expect-error — a family's token store with a bare factory: the store cannot live with the tasks
export const mixedToken = eventProcessor("courses", eventStore, familyTokenStore, unitOfWork)

// @ts-expect-error — a family's queue on a processor whose factory mints bare tasks
export const mixedQueue = deadLettering(plainProjection, familyQueue, byCourse)
