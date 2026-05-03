// TODO(plan-09): the original tests in this file drove
// EventSourcingConfigurer + axonServerConfigurationEnhancer + ComponentKeys
// (configurer trio deleted in Plan 08-04). They covered an end-to-end
// command/event-sourcing/snapshot flow against a Testcontainers Axon Server
// instance: dispatch through CommandGateway, source events by tag criteria,
// enforce business rules from event-sourced state, capacity / duplicate
// enrollment, and snapshot store roundtrip via createAxonServerSnapshotStore.
// Re-enable once kronos() App exposes a typed Axon Server connection slot
// (or ships an (app: App) => void extension equivalent to the deleted
// axonServerConfigurationEnhancer). Tracked in
// .planning/phases/08-configurer-deletion/deferred-items.md §"Plan 04".
import { describe, it } from "bun:test"

describe.skip("Axon Server integration — deferred to Phase 9", () => {
  it.skip("dispatches a command through Axon Server and sources state", () => {})
  it.skip("enforces business rules from event-sourced state", () => {})
  it.skip("sources events by tag criteria", () => {})
  it.skip("enforces capacity limits across multiple students", () => {})
  it.skip("prevents duplicate student enrollment", () => {})
  it.skip("stores and loads snapshots via Axon Server", () => {})
})
