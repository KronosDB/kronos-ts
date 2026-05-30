/**
 * Type-level tests for the collapsed handler signatures (Plan 04-02, Task 1).
 *
 * These tests assert the POST-COLLAPSE shape where:
 * - Event handlers receive EventMessage
 * - Evolvers receive EventMessage
 * - Query handlers receive QueryMessage
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

// Test 1: on(EventDescriptor, (message) => ...) typechecks and returns EventHandlerRegistration
test("on() with EventDescriptor and message handler returns EventHandlerRegistration", () => {
  const reg: EventHandlerRegistration<typeof SomethingHappened.payload> = on(
    SomethingHappened,
    async ({ payload, metadata }) => {
      void payload.value
      void metadata
    },
  )
  expect(reg.kind).toBe("event-handler")
  expect(reg.descriptor).toBe(SomethingHappened)
})

// Test 2: on(QueryDescriptor, (message) => ...) typechecks and returns QueryHandlerRegistration
test("on() with QueryDescriptor and message handler returns QueryHandlerRegistration", () => {
  const reg: QueryHandlerRegistration<typeof GetSomething.payload, { result: string }> = on(
    GetSomething,
    async ({ payload, metadata }): Promise<{ result: string }> => {
      void payload.id
      void metadata
      return { result: "ok" }
    },
  )
  expect(reg.kind).toBe("query-handler")
  expect(reg.descriptor).toBe(GetSomething)
})

// Test 3: on(EventDescriptor, (state, message) => ...) evolver typechecks
test("on() evolver overload typechecks with state and message", () => {
  type State = { count: number }
  const reg: EvolverRegistration<State, typeof SomethingHappened.payload> = on(
    SomethingHappened,
    (state: State, { payload }): State => ({
      count: state.count + payload.value,
    }),
  )
  expect(reg.descriptor).toBe(SomethingHappened)
  // Verify evolver actually works
  const result = reg.evolve({ count: 0 }, {
    identifier: "evt-1",
    name: SomethingHappened.name,
    version: SomethingHappened.version,
    payload: { value: 5 },
    metadata: {},
    timestamp: 1234,
    tags: [],
  })
  expect(result).toEqual({ count: 5 })
})

// Test 4: EventHandlerRegistration.handler supports message details
test("EventHandlerRegistration.handler supports message details", () => {
  let capturedMetadata: Metadata | undefined
  let capturedTimestamp: number | undefined
  const testMetadata: Metadata = { correlationId: "test-123" }
  const message = {
    identifier: "evt-1",
    name: SomethingHappened.name,
    version: SomethingHappened.version,
    payload: { value: 42 },
    metadata: testMetadata,
    timestamp: 1234,
    tags: [],
  }

  const reg: EventHandlerRegistration<typeof SomethingHappened.payload> = on(
    SomethingHappened,
    async ({ metadata, timestamp }) => {
      capturedMetadata = metadata
      capturedTimestamp = timestamp
    },
  )

  reg.handler(message)

  expect(capturedMetadata).toEqual(testMetadata)
  expect(capturedTimestamp).toBe(1234)
})

// Test 5: no regressions — existing handler.test.ts tests still pass
// (this test file runs alongside handler.test.ts; the describe block is included for clarity)
describe("handler-signature: evolver shape preserved", () => {
  test("evolver registration evolve function receives event message", () => {
    type S = { name: string }
    const message = {
      identifier: "evt-1",
      name: SomethingHappened.name,
      version: SomethingHappened.version,
      payload: { value: 7 },
      metadata: {},
      timestamp: 5678,
      tags: [],
    }
    const reg = on(
      SomethingHappened,
      (state: S, { payload, timestamp }): S => ({
        ...state,
        name: `${payload.value}-${timestamp}`,
      }),
    )
    const result = reg.evolve({ name: "" }, message)
    expect(result).toEqual({ name: "7-5678" })
  })
})
