// TODO(plan-09): the original tests in this file drove
// EventSourcingConfigurer + ComponentKeys.TOKEN_STORE / TRANSACTION_MANAGER
// (both deleted in Plan 08-04). They covered: (a) token position persistence
// via TokenStore, (b) resume-from-stored-token-position, (c) wrapping event
// processing in a TransactionManager. Re-enable once kronos() App exposes
// typed `tokenStore` and `transactionManager` slots (or ships
// (app: App) => void extensions for them). Tracked in
// .planning/phases/08-configurer-deletion/deferred-items.md §"Plan 04".
import { describe, it } from "bun:test"

describe.skip("Transactional event processing — deferred to Phase 9", () => {
  it.skip("persists token position via TokenStore", () => {})
  it.skip("resumes from stored token position on restart", () => {})
  it.skip("wraps event processing in transaction when TransactionManager is configured", () => {})
})
