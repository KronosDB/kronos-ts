// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `kyselyTransaction` is the only way to open one and `activeKyselyTransaction`
// the only way to observe one without opening it.
export {
  kyselyUnitOfWork,
  type KyselyFamily,
  kyselyTransaction,
  activeKyselyTransaction,
  kyselyHandler,
  type KyselyCapability,
  type KyselyContext,
  type KyselyEventContext,
  type KyselyQueryContext,
  type KyselyDb,
  type KyselyTransaction,
} from "./kysely-transaction.js"

export {
  kyselyTokenStore,
  type KyselyTokenStoreOptions,
  KYSELY_TOKEN_TABLE,
} from "./kysely-token-store.js"

export {
  kyselyDeadLetterQueue,
  type KyselyDeadLetterQueueOptions,
  KYSELY_DEAD_LETTER_TABLE,
} from "./kysely-dead-letter-queue.js"
