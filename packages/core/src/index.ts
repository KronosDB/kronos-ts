export type { KronosComponents, SlotName } from "./components.js"
export { ALL_SLOTS } from "./components.js"
export { CircularSlotDependencyError, SlotNotRegisteredError } from "./errors.js"
export { SlotRegistry, type SlotMeta, type SlotEntry, type SlotFactory } from "./slot-registry.js"
export { buildResolved, type Resolved } from "./resolved.js"
export {
  createWarningChannel,
  type WarningLogger,
  type WarningChannelOptions,
  type WarningChannel,
} from "./warnings.js"
export {
  AppImpl,
  AppAlreadyStartedError,
  type App,
  type RunningApp,
  type Extension,
  type AppState,
  type AppImplOptions,
} from "./app.js"
export { registerInMemoryDefaults } from "./defaults.js"
export { kronos, type KronosPartialConfig } from "./kronos.js"
export {
  type DecoratorHandle,
  type DecoratorFactory,
  type DecoratorEntry,
  applyDecorators,
} from "./decorator.js"
export { Defaults } from "./defaults-handles.js"
export { UnknownDecoratorHandleError, AppNotStartedError } from "./errors.js"
export type { LifecycleStage, LifecycleHook } from "./lifecycle.js"
// Plan 09-01 Task 1: re-export of typed-slot interfaces so extension packages don't
// need to deep-import from @kronos-ts/messaging just to set/replace slots.
export type { TokenStore, TransactionManager } from "@kronos-ts/messaging"
