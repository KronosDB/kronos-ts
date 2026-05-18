/**
 * Type-level tests for the collapsed handler signatures (Plan 04-02, Task 1).
 *
 * These tests assert the POST-COLLAPSE shape where:
 * - Event/query handlers receive (payload, metadata) — no context object
 * - Evolvers remain unchanged: (state, event, id) per D-41
 * - CommandHandlerContext / EventHandlerContext / QueryHandlerContext are DELETED
 */
import { describe, test, expect } from "bun:test"
import type { Metadata } from "@kronos-ts/common"
import { qn } from "@kronos-ts/common"
import { z } from "zod"
import { event, query } from "../descriptor.js"
import { on } from "../handler.js"
import type { EventHandlerRegistration, EvolverRegistration, QueryHandlerRegistration } from "../handler.js"

const SomethingHappened = event({
  name: qn("test", "SomethingHappened"),
  payload: z.object({ value: z.number() }),
})

const GetSomething = query({
  name: qn("test", "GetSomething"),
  payload: z.object({ id: z.string() }),
})

// Test 1: on(EventDescriptor, (event, metadata) => ...) typechecks and returns EventHandlerRegistration
test("on() with EventDescriptor and (event, metadata) returns EventHandlerRegistration", () => {
  const reg: EventHandlerRegistration<typeof SomethingHappened.payload> = on(
    SomethingHappened,
    async (event: { value: number }, metadata: Metadata) => {
      // metadata is Metadata, not EventHandlerContext
      void event
      void metadata
    },
  )
  expect(reg.kind).toBe("event-handler")
  expect(reg.descriptor).toBe(SomethingHappened)
})

// Test 2: on(QueryDescriptor, (query, metadata) => ...) typechecks and returns QueryHandlerRegistration
test("on() with QueryDescriptor and (query, metadata) returns QueryHandlerRegistration", () => {
  const reg: QueryHandlerRegistration<typeof GetSomething.payload, { result: string }> = on(
    GetSomething,
    async (query: { id: string }, metadata: Metadata): Promise<{ result: string }> => {
      void query
      void metadata
      return { result: "ok" }
    },
  )
  expect(reg.kind).toBe("query-handler")
  expect(reg.descriptor).toBe(GetSomething)
})

// Test 3: on(EventDescriptor, (state, event, id) => ...) (evolver) typechecks — D-41 evolver-unchanged
test("on() evolver overload still typechecks with (state, event, id) — D-41 preserved", () => {
  type State = { count: number }
  const reg: EvolverRegistration<State, typeof SomethingHappened.payload> = on(
    SomethingHappened,
    (state: State, event: { value: number }, _id: unknown): State => ({
      count: state.count + event.value,
    }),
  )
  expect(reg.descriptor).toBe(SomethingHappened)
  // Verify evolver actually works
  const result = reg.evolve({ count: 0 }, { value: 5 }, "some-id")
  expect(result).toEqual({ count: 5 })
})

// Test 4: EventHandlerRegistration.handler signature uses (event, metadata) — no context arg
test("EventHandlerRegistration.handler has (event, metadata) signature", () => {
  let capturedMetadata: Metadata | undefined
  const testMetadata: Metadata = { correlationId: "test-123" }

  const reg: EventHandlerRegistration<typeof SomethingHappened.payload> = on(
    SomethingHappened,
    async (_event: { value: number }, metadata: Metadata) => {
      capturedMetadata = metadata
    },
  )

  // Call the handler with (payload, metadata) — the new shape
  reg.handler({ value: 42 }, testMetadata)

  expect(capturedMetadata).toEqual(testMetadata)
})

// Test 5: no regressions — existing handler.test.ts tests still pass
// (this test file runs alongside handler.test.ts; the describe block is included for clarity)
describe("handler-signature: evolver shape preserved", () => {
  test("evolver registration evolve function receives (state, event, id)", () => {
    type S = { name: string }
    const reg = on(
      SomethingHappened,
      (state: S, event: { value: number }, id: unknown): S => ({
        ...state,
        name: `${id}-${event.value}`,
      }),
    )
    const result = reg.evolve({ name: "" }, { value: 7 }, "agg-1")
    expect(result).toEqual({ name: "agg-1-7" })
  })
})
