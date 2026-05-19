import { makeFrameworkHandle, type DecoratorHandle } from "./decorator.js"

/**
 * Global static module export — frozen handle identities for framework-default
 * decorators (D-54). Per-app decorator state lives on `AppState`; `Defaults`
 * only carries handle *identities*.
 *
 * Plan 02 wires the actual factories in `kronos()` bootstrap, keyed by these
 * handles. Plan 01 ships the handle constants only — having them in place
 * lets users write `app.removeDecorator(Defaults.commandBus.intercepting)`
 * even before Plan 02 registers the factories (the call will throw
 * `UnknownDecoratorHandleError` until Plan 02 lands, which is correct
 * behavior for now and ratchets to the real removal once Plan 02 ships).
 */
export const Defaults = Object.freeze({
  commandBus: Object.freeze({
    intercepting: makeFrameworkHandle("commandBus", "intercepting"),
  }),
  queryBus: Object.freeze({
    intercepting: makeFrameworkHandle("queryBus", "intercepting"),
  }),
  eventBus: Object.freeze({
    intercepting: makeFrameworkHandle("eventBus", "intercepting"),
  }),
}) as {
  readonly commandBus: { readonly intercepting: DecoratorHandle<"commandBus"> }
  readonly queryBus: { readonly intercepting: DecoratorHandle<"queryBus"> }
  readonly eventBus: { readonly intercepting: DecoratorHandle<"eventBus"> }
}
