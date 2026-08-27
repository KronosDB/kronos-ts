// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `typeormTransaction` is the only way to open one and `activeTypeormTransaction`
// the only way to observe one without opening it.
export {
  typeormUnitOfWork,
  typeormTransaction,
  activeTypeormTransaction,
  typeormHandler,
  type TypeormCapability,
  type TypeormCommandContext,
  type TypeormEventContext,
  type TypeormQueryContext,
  type TypeormManager,
  type TypeormTransaction,
} from "./typeorm-transaction.js"

export {
  typeormTokenStore,
  type TypeormTokenStoreOptions,
  TYPEORM_TOKEN_TABLE,
} from "./typeorm-token-store.js"

export {
  typeormDeadLetterQueue,
  type TypeormDeadLetterQueueOptions,
  TYPEORM_DEAD_LETTER_TABLE,
} from "./typeorm-dead-letter-queue.js"
