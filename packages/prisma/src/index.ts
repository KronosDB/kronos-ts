// The unit-of-work factory and its TYPED accessor pair. The transaction lives
// in this package, keyed by unit of work — the base `UnitOfWork` has none — so
// `prismaTransaction` is the only way to open one and `activePrismaTransaction`
// the only way to observe one without opening it.
export {
  prismaUnitOfWork,
  prismaTransaction,
  activePrismaTransaction,
  prismaHandler,
  type PrismaCapability,
  type PrismaContext,
  type PrismaEventContext,
  type PrismaQueryContext,
  type PrismaClientLike,
  type PrismaTransactionClient,
  type PrismaTransactionOptions,
} from "./prisma-transaction.js"

export {
  prismaTokenStore,
  type PrismaTokenStoreOptions,
} from "./prisma-token-store.js"

export {
  prismaDeadLetterQueue,
  type PrismaDeadLetterQueueOptions,
} from "./prisma-dead-letter-queue.js"
