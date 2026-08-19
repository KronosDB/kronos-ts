// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `drizzleTransaction` is the only way to open one and `activeDrizzleTransaction`
// the only way to observe one without opening it.
export {
  drizzleUnitOfWork,
  drizzleTransaction,
  activeDrizzleTransaction,
  drizzleHandler,
  type DrizzleCapability,
  type DrizzleContext,
  type DrizzleEventContext,
  type DrizzleQueryContext,
  type DrizzleDb,
  type DrizzleTransaction,
  type DrizzleTransactionOptions,
} from "./drizzle-transaction.js"

export {
  drizzleTokenStore,
  type DrizzleTokenStoreOptions,
} from "./drizzle-token-store.js"

export {
  drizzleDeadLetterQueue,
  type DrizzleDeadLetterQueueOptions,
} from "./drizzle-dead-letter-queue.js"

// The tables this adapter owns — exported so a migration generator can see
// them, never passed back in.
export { kronosDeadLetters, kronosTokenEntries } from "./drizzle-schema.js"
