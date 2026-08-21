// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `knexTransaction` is the only way to open one and `activeKnexTransaction`
// the only way to observe one without opening it.
export {
  knexUnitOfWork,
  type KnexFamily,
  knexTransaction,
  activeKnexTransaction,
  knexHandler,
  type KnexCapability,
  type KnexContext,
  type KnexEventContext,
  type KnexQueryContext,
  type KnexClient,
  type KnexTransaction,
  type KnexTransactionOptions,
} from "./knex-transaction.js"

export {
  knexTokenStore,
  type KnexTokenStoreOptions,
  KNEX_TOKEN_TABLE,
} from "./knex-token-store.js"

export {
  knexDeadLetterQueue,
  type KnexDeadLetterQueueOptions,
  KNEX_DEAD_LETTER_TABLE,
} from "./knex-dead-letter-queue.js"
