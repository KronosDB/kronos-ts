/**
 * Re-exports the generated gRPC service definitions for KronosDB.
 * Used internally by the connector and can be passed to connectToKronosDb().
 */
import { PlatformServiceDefinition } from "./generated/platform.js"
import { CommandServiceDefinition } from "./generated/command.js"
import { QueryServiceDefinition } from "./generated/query.js"
import { EventStoreDefinition } from "./generated/eventstore.js"
import { SchedulerServiceDefinition } from "./generated/scheduler.js"

export const kronosDbServiceDefinitions = {
  platform: PlatformServiceDefinition,
  commands: CommandServiceDefinition,
  queries: QueryServiceDefinition,
  eventStore: EventStoreDefinition,
  scheduler: SchedulerServiceDefinition,
} as const

export {
  PlatformServiceDefinition,
  CommandServiceDefinition,
  QueryServiceDefinition,
  EventStoreDefinition,
  SchedulerServiceDefinition,
}
