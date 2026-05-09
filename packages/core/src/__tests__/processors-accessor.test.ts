/**
 * Plan 09-01 Task 2 — App.processors() dual-overload (D-103).
 *
 * Asserts:
 *  T1. processors() with zero args returns a frozen readonly view of the
 *      registered EventProcessorModule[] in registration order.
 *  T2. The returned array is frozen — mutations don't smuggle into _state.
 *  T3. Chained writers (.processors(modA).processors(modB)) accumulate; the
 *      subsequent read returns [modA, modB].
 *  T4. Empty case: processors() on a fresh App returns an empty readonly array.
 */
import { describe, it, expect } from "bun:test"
import {
  on,
  eventHandlers,
  event,
  type EventProcessorModule,
} from "@kronos-ts/messaging"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import { kronos } from "../kronos.js"

const Bumped = event({
  name: qn("acc", "Bumped"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})

function makeProcessorModule(name: string): EventProcessorModule {
  return {
    kind: "subscribing",
    name,
    handlerGroups: [
      eventHandlers({
        name: `${name}-group`,
        handlers: [on(Bumped, async () => {})],
      }),
    ],
  }
}

describe("App.processors() — read accessor (D-103)", () => {
  it("zero-arg call returns the registered modules in order", () => {
    const modA = makeProcessorModule("A")
    const modB = makeProcessorModule("B")
    const app = kronos({ quiet: true }).processors(modA, modB)
    const result = app.processors()
    expect(result).toEqual([modA, modB])
  })

  it("returned array is frozen — mutating it does not affect app state", () => {
    const modA = makeProcessorModule("A")
    const app = kronos({ quiet: true }).processors(modA)
    const result = app.processors() as readonly EventProcessorModule[]
    expect(Object.isFrozen(result)).toBe(true)
    // Pushing onto a frozen array throws in strict mode (ESM is strict by default).
    expect(() => (result as EventProcessorModule[]).push(makeProcessorModule("X"))).toThrow()
    // The internal state is untouched.
    expect(app.processors()).toHaveLength(1)
  })

  it("chained writes accumulate; subsequent read reflects registration order", () => {
    const modA = makeProcessorModule("A")
    const modB = makeProcessorModule("B")
    const modC = makeProcessorModule("C")
    const app = kronos({ quiet: true })
      .processors(modA)
      .processors(modB, modC)
    const result = app.processors()
    expect(result).toEqual([modA, modB, modC])
  })

  it("empty case returns an empty readonly array", () => {
    const app = kronos({ quiet: true })
    const result = app.processors()
    expect(result).toEqual([])
    expect(result.length).toBe(0)
  })
})
