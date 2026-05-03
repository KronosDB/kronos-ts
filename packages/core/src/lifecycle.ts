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

// Plan 08-03a (D-77): the legacy numeric-phase mapping is deleted. Native
// AppImpl.start() executes typed-stage hooks directly off
// AppState.startHooks/stopHooks — no numeric-phase bridge to the legacy
// LifecycleRegistry. Plan 03b's enhancer-bridge keeps a private inverted copy
// locally for the D-81 fallback path.
