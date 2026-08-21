/**
 * Shared test helper: run a function inside a live UnitOfWork.
 *
 * Drives the real lifecycle via `uow.execute`, so the handle the callback
 * receives is in the INVOCATION phase exactly as a handler's would be — which
 * is what the mutating capabilities require.
 */
import { unitOfWork, type UnitOfWork } from "../../../unit-of-work/unit-of-work.js"

export function inUoW<R>(fn: (uow: UnitOfWork) => R | Promise<R>): Promise<R> {
  return unitOfWork().execute(async (uow) => fn(uow))
}
