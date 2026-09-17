/**
 * The TYPE test for the query bus's factory. Listed in the root
 * `tsconfig.json` `files` array, so `bunx tsc --noEmit` judges it.
 *
 * What it pins: `localQueryBus` refuses a factory a transaction family marked
 * — with a sentence — and accepts the plain one, keeping `U` intact.
 */
import { localQueryBus } from "../local-bus.js"
import { unitOfWork, type UnitOfWork } from "../../unit-of-work/unit-of-work.js"
import { transactional } from "../../unit-of-work/transactional.js"
import type { SubscriptionCapableQueryBus } from "../bus.js"

// A stand-in for what `drizzleUnitOfWork(unitOfWork, db)` returns.
type Family = UnitOfWork & { readonly family: "probe" }
const familyFactory = transactional((): Family => Object.assign(unitOfWork(), { family: "probe" as const }))

// Plain factory: accepted, `U` preserved.
export const plain: SubscriptionCapableQueryBus<UnitOfWork> = localQueryBus(unitOfWork)

// A composed but NON-transactional factory is fine too — the mark is what is refused, not the composition.
export const composed: SubscriptionCapableQueryBus<Family> = localQueryBus(
  (): Family => Object.assign(unitOfWork(), { family: "probe" as const }),
)

// @ts-expect-error — "a query bus must be built from the plain unitOfWork factory — a read needs no transaction…"
export const refused = localQueryBus(familyFactory)
