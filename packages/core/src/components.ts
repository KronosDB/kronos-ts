import type { EventStore, SnapshotStore, TagResolver } from "@kronos-ts/eventsourcing"
import type {
  CommandBus,
  QueryBus,
  EventBus,
  UoWRunner,
  TokenStore,
  TransactionManager,
} from "@kronos-ts/messaging"
import type { Serializer } from "@kronos-ts/common"

/**
 * Fixed slot interface. SLT-01: enumerates ALL 10 framework slots.
 * No declaration merging, no string tokens — closed contract.
 *
 * Plan 09-01 (D-84): tokenStore + transactionManager added so persistence
 * extensions (KronosDB, etc.) replace typed slots instead of routing through
 * the deleted configurer's componentRegistry.
 */
export interface KronosComponents {
  eventStore: EventStore
  snapshotStore: SnapshotStore
  commandBus: CommandBus
  queryBus: QueryBus
  eventBus: EventBus
  serializer: Serializer
  unitOfWorkFactory: UoWRunner
  tagResolver: TagResolver
  tokenStore: TokenStore
  transactionManager: TransactionManager
}

/** Type-level: keyof KronosComponents — for verb signatures. */
export type SlotName = keyof KronosComponents

/** Sentinel listing every slot name; used by .start() to iterate slots and emit startup warnings. Order is stable for deterministic warning emission. */
export const ALL_SLOTS: readonly SlotName[] = [
  "eventStore",
  "snapshotStore",
  "commandBus",
  "queryBus",
  "eventBus",
  "serializer",
  "unitOfWorkFactory",
  "tagResolver",
  "tokenStore",
  "transactionManager",
] as const
