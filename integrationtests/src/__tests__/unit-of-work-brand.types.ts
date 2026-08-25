/**
 * THE TYPE TEST FOR PERSISTENCE-FAMILY BRANDING.
 *
 * Every claim here is a compile-time one, so the test IS the typecheck: this
 * file is listed in the root `tsconfig.json` `files` array, which is not subject
 * to `exclude`, so it is judged by `tsc --noEmit`. A `@ts-expect-error` that
 * stops erroring turns that gate red.
 *
 * What it pins, in one sentence: A PROCESSOR CANNOT BE BUILT OUT OF TWO
 * PERSISTENCE FAMILIES.
 *
 * ── WHY THIS ONE MATTERS MORE THAN IT LOOKS ────────────────────────────────
 *
 * The other demands in this codebase prevent a THROW — a snapshotting state
 * over a bare log, a `ctx.schedule` with nothing to schedule into. Both would
 * have failed loudly on the first call. This one prevents SILENCE. A drizzle
 * token store handed a postgres unit of work does not throw: it asks for its own
 * transaction, is told there is none, and falls back to its plain handle. So
 * the token update commits outside the batch's transaction, every test passes,
 * and the bug is a read model that is permanently wrong after a crash that
 * happened between two writes nobody knew were separate.
 *
 * It lives HERE rather than in any one package because the claim is about two
 * packages disagreeing, and no single package can import its rival to make it.
 *
 * It also has to live somewhere that can see `eventProcessor`, because THE
 * PROCESSOR IS WHERE THE COMPILER MEETS THE FACTORY AND THE STORES. A brand on
 * a decorator that nothing ever compared against would be decoration.
 */
import {
  eventProcessor,
  correlating,
  inMemoryEventStore,
  inMemoryTokenStore,
  inMemoryDeadLetterQueue,
  sequentialPerTag,
  unitOfWork,
  type CorrelatingUnitOfWork,
  type TokenStore,
  type UnitOfWork,
} from "@kronos-ts/core"
import {
  drizzleDeadLetterQueue,
  drizzleTokenStore,
  drizzleUnitOfWork,
  type DrizzleUnitOfWork,
} from "@kronos-ts/drizzle"
import {
  postgresTokenStore,
  postgresUnitOfWork,
  type PostgresUnitOfWork,
} from "@kronos-ts/postgres"

// The clients are irrelevant to every claim below — a family is a TYPE fact,
// and none of this code runs.
declare const db: any
declare const pg: any

const eventStore = inMemoryEventStore()
const lane = sequentialPerTag("courseId")

// ---------------------------------------------------------------------------
// (a) THE DECORATOR BRANDS WHAT IT MINTS, and preserves what it was handed.
// ---------------------------------------------------------------------------

/** A bare task, decorated: `() => UnitOfWork & DrizzleUnitOfWork`. */
export const drizzleTasks: () => UnitOfWork & DrizzleUnitOfWork = drizzleUnitOfWork(unitOfWork, db)

/**
 * THE BRAND AND A COMPOSED CAPABILITY COEXIST — the claim the surface makes
 * about these decorators being capability-preserving, now with a mark on top.
 * `drizzleUnitOfWork(() => correlating(unitOfWork()), db)` is the documented
 * bootstrap idiom, and it still yields a task that CARRIES as well as one that
 * belongs to a family.
 */
export const correlatingDrizzleTasks: () => CorrelatingUnitOfWork & DrizzleUnitOfWork =
  drizzleUnitOfWork(() => correlating(unitOfWork()), db)

/** And it reads BOTH ways round: the correlation survives the brand… */
export const stillCorrelating: () => CorrelatingUnitOfWork = correlatingDrizzleTasks
/** …and the brand survives the correlation. */
export const stillBranded: () => UnitOfWork & DrizzleUnitOfWork = correlatingDrizzleTasks

/** A bare factory is NOT branded, so none of the refusals below are vacuous. */
// @ts-expect-error — nothing marked this task
export const bareIsNotBranded: () => UnitOfWork & DrizzleUnitOfWork = unitOfWork

// ---------------------------------------------------------------------------
// (b) THE PROCESSOR IS WHERE THE DEMAND IS MET. All four quadrants of
// (store's family) × (task's family).
// ---------------------------------------------------------------------------

/** SAME FAMILY ✓ — the arrangement the demand exists to require. */
export const sameFamily = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: drizzleTokenStore(db),
  deadLetterQueue: drizzleDeadLetterQueue(db),
  sequence: lane,
  unitOfWork: drizzleUnitOfWork(unitOfWork, db),
})

/**
 * A BARE STORE DEMANDS NOTHING ✓. `inMemoryTokenStore()` has no transaction to
 * share, so it cannot be in the wrong family — and contravariance says so
 * without anybody writing a special case.
 */
export const bareStoreFitsAnyFamily = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: inMemoryTokenStore(),
  deadLetterQueue: inMemoryDeadLetterQueue(),
  sequence: lane,
  unitOfWork: drizzleUnitOfWork(unitOfWork, db),
})

/** A BARE PROCESSOR IS UNTOUCHED ✓ — a project using neither never meets this. */
export const allBare = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: inMemoryTokenStore(),
  unitOfWork,
})

/**
 * MIXED FAMILIES ✗ — THE HEADLINE.
 *
 * The diagnostic is the DEMANDING package's own sentence, because that is the
 * package that knows the answer for certain: the two brands differ first on
 * their `FIX` string, so the checker prints "build this processor's unitOfWork
 * with drizzleUnitOfWork(next, db)" at the wiring site.
 */
export const mixedFamilies = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: drizzleTokenStore(db),
  // @ts-expect-error — a drizzle token store cannot write through a postgres task
  unitOfWork: postgresUnitOfWork(unitOfWork, pg),
})

/** A FAMILY STORE ON A BARE TASK ✗ — the same mistake, one step earlier. */
export const familyStoreBareTask = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: drizzleTokenStore(db),
  // @ts-expect-error — nothing put a drizzle transaction on this task
  unitOfWork,
})

/** THE QUEUE CARRIES THE SAME DEMAND — it parks in the same transaction. */
export const mixedQueue = eventProcessor({
  name: "courses",
  eventStore,
  tokenStore: postgresTokenStore(pg),
  // @ts-expect-error — a drizzle queue cannot park through a postgres task
  deadLetterQueue: drizzleDeadLetterQueue(db),
  sequence: lane,
  unitOfWork: postgresUnitOfWork(unitOfWork, pg),
})

// ---------------------------------------------------------------------------
// (c) THE VARIANCE, stated directly — because the whole mechanism rests on it.
//
// If these members were method SHORTHAND, TypeScript would check their
// parameters bivariantly and every refusal above would silently pass. They are
// function-typed FIELDS, so the check is contravariant and the demand is real.
// ---------------------------------------------------------------------------

/** A branded store does NOT fit a bare slot. */
// @ts-expect-error — this store demands a drizzle task; the slot promises none
export const brandedIntoBare: TokenStore = drizzleTokenStore(db)

/** A bare store DOES fit a branded slot — nothing to disagree about. */
export const bareIntoBranded: TokenStore<UnitOfWork & DrizzleUnitOfWork> = inMemoryTokenStore()

/** And two families do not fit each other. */
// @ts-expect-error — postgres is not drizzle
export const crossFamily: TokenStore<UnitOfWork & DrizzleUnitOfWork> = postgresTokenStore(pg)

// ---------------------------------------------------------------------------
// (d) A FAMILY BRAND IS NOT A CAPABILITY. It rides on the task, so a handler
// that demands a composed capability still demands exactly that, and a brand
// neither satisfies nor blocks one.
// ---------------------------------------------------------------------------

export const brandAloneIsNotCorrelating = (): void => {
  // @ts-expect-error — a mark is not a map; branding a task does not make it carry
  const carries: () => CorrelatingUnitOfWork = drizzleUnitOfWork(unitOfWork, db)
  void carries
}

export type FamiliesAreDistinct = PostgresUnitOfWork extends DrizzleUnitOfWork ? never : true
export const familiesAreDistinct: FamiliesAreDistinct = true
