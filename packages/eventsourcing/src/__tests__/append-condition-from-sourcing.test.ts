// TODO(plan-09): the original tests in this file drove
// EventSourcingConfigurer.create({ eventStore }) + registerEntity + load/append
// (configurer trio deleted in Plan 08-04). They covered the append-condition
// flow derived from sourced state: appending with a condition that prevents
// stale-state decisions, and detecting concurrent modification via the
// append condition. Re-enable once kronos() App exposes a typed event-store
// override slot (or ships an (app: App) => void extension equivalent to
// EventSourcingConfigurer.create({ eventStore })). Tracked in
// .planning/phases/08-configurer-deletion/deferred-items.md §"Plan 04".
import { describe, it } from "bun:test"

describe.skip("Append condition derived from sourced state — deferred to Phase 9", () => {
  it.skip("appends with condition that prevents stale-state decisions", () => {})
  it.skip("detects concurrent modification via append condition", () => {})
})
