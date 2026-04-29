/**
 * Typed lifecycle stages for app-level startup/shutdown hooks (LIF-01).
 *
 * Forward order on `.start()`: connect → warmup → register → processors → serve.
 * Reverse order on `.stop()`:  serve → processors → register → warmup → connect.
 *
 * Within a single stage, hooks execute in registration order (no `{stage, order}`
 * tiebreaker — D-70 defers it until a real consumer asks).
 *
 * Closed string union by design (LIF-01). NOT extensible via declaration merging.
 */
export type LifecycleStage = "connect" | "register" | "warmup" | "processors" | "serve"

/**
 * Hook signature for `app.onStart(stage, fn)` / `app.onStop(stage, fn)` (LIF-02, D-69).
 *
 * Bare zero-arg shape. Extensions close over their own state from the enclosing
 * `(app: App) => void` extension scope (see DESIGN.md §12 KronosDB pattern).
 *
 * Why no `Resolved` arg: the Resolved proxy exists for slot-factory composition
 * (Phase 5 D-52); lifecycle hooks are a different concern.
 *
 * Why no `Configuration` arg: matching the legacy `LifecycleHandler(config?: any)`
 * signature would couple this public type to the configurer's `Configuration` —
 * a Phase 8 deletion target.
 */
export type LifecycleHook = () => void | Promise<void>

/**
 * Internal mapping from typed stages to legacy numeric `LifecyclePhase` values
 * (LIF-03, D-67). Used by `AppImpl.start()` to bridge typed-stage registrations
 * onto the existing `EventSourcingConfigurer.lifecycleRegistry((reg) => …)`
 * callback API.
 *
 * NOT exported from `@kronos-ts/core` — module-private. The numeric scale is a
 * Phase 8 deletion target; widening its consumer base in Phase 7 is anti-goal.
 *
 * Numeric slots (rationale per D-67):
 * - connect=-1000     aligns with LifecyclePhase.EXTERNAL_CONNECTIONS
 * - warmup=-500       new clean gap between connect and register (no collisions
 *                     with OUTBOUND_EVENT_CONNECTORS=-10)
 * - register=0        aligns with LifecyclePhase.LOCAL_MESSAGE_HANDLER_REGISTRATIONS
 * - processors=1000   aligns with LifecyclePhase.INBOUND_EVENT_CONNECTORS
 * - serve=2000        new clean gap past INSTRUCTION_COMPONENTS=1010
 */
export const STAGE_TO_PHASE: Record<LifecycleStage, number> = {
  connect: -1000,
  warmup: -500,
  register: 0,
  processors: 1000,
  serve: 2000,
}
