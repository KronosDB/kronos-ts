// TODO(plan-09): the original tests in this file drove
// EventSourcingConfigurer + registerEventHandlers + trackingProcessor
// (configurer trio deleted in Plan 08-04). They covered the full flow:
// command → event store → tracking processor → projection → query handler.
// Re-enable once kronos() App exposes a typed event-processor / projection
// surface (or ships an (app: App) => void extension equivalent to the
// deleted EventSourcingConfigurer.messaging(...).registerEventHandlers chain).
// Tracked in .planning/phases/08-configurer-deletion/deferred-items.md §"Plan 04".
import { describe, it } from "bun:test"

describe.skip("Full flow: command → event → processor → projection → query — deferred to Phase 9", () => {
  it.skip("command produces events, processor delivers to projection, query reads it", () => {})
  it.skip("multiple commands produce events that update the projection", () => {})
  it.skip("processor handles events from multiple command handler slices", () => {})
})
