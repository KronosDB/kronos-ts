import type { EventStore, SnapshotStore, TagResolver } from "@kronos-ts/eventsourcing"
import type { CommandBus, QueryBus, EventBus, UoWRunner } from "@kronos-ts/messaging"
import type { Serializer } from "@kronos-ts/common"

/**
 * Fixed slot interface. SLT-01: enumerates ALL 8 framework slots.
 * No declaration merging, no string tokens — closed contract.
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
] as const
