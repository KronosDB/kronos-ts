/**
 * Type-level tests for the `on()` signature.
 *
 * `on()` registers exactly two things:
 * - Evolvers        — `on(EventDescriptor, (state, message) => state)`
 * - Query handlers  — `on(QueryDescriptor, (message) => result)`
 *
 * It does NOT register event handlers. A state evolver `(state, event) => state`
 * and an event handler `(event, ctx) => void` are different things and no longer
 * share a factory: event handlers are declared with `eventHandler(...)` and yield
 * an `EventHandlerDefinition`, which is what `.eventHandlers(...)` on the
 * processor builders accepts.
 *
 * Handler context wrapper types (CommandHandlerContext / EventHandlerContext /
 * QueryHandlerContext as handler-shaping types) remain deleted (Plan 04-02).
 */
import { describe, test, expect } from "bun:test"
import type { Metadata } from "@kronos-ts/common"
import { qn } from "@kronos-ts/common"
import { z } from "zod"
import { event, query } from "../descriptor.js"
import { on } from "../handler.js"
import type { EvolverRegistration, QueryHandlerRegistration } from "../handler.js"
import { eventHandler } from "../event-handler.js"
import type { EventHandlerDefinition } from "../event-handler.js"

const SomethingHappened = event({
  name: qn("test", "SomethingHappened"),
  payload: z.object({ value: z.number() }),
})

const GetSomething = query({
  name: qn("test", "GetSomething"),
  payload: z.object({ id: z.string() }),
})

// Test 1: on(EventDescriptor, (state, message) => ...) returns EvolverRegistration
test("on() with EventDescriptor returns EvolverRegistration", () => {
  type State = { count: number }
  const reg: EvolverRegistration<State, typeof SomethingHappened.payload> = on(
    SomethingHappened,
    (state: State, { payload, metadata }): State => {
      void metadata
      return { count: state.count + payload.value }
    },
  )
  expect(reg.kind).toBe("evolver")
  expect(reg.descriptor).toBe(SomethingHappened)

  const result = reg.evolve({ count: 0 }, {
    kind: "event",
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

// Test 3: on() has no event-handler overload — the one-parameter message
// callback that used to select it no longer typechecks, and the value it
// produces is a plain evolver (no `handler`, no chimera).
test("on() no longer produces an event handler", () => {
  // @ts-expect-error — no event-handler overload: a `(message) => void` callback
  // is not a valid evolver.
  const reg = on(SomethingHappened, async ({ payload }) => {
    void payload.value
  })

  expect(reg.kind).toBe("evolver")
  expect("handler" in reg).toBe(false)
})

// Test 4: event handlers come from eventHandler() and receive (message, context)
test("eventHandler() is the event-handler factory and takes a context parameter", async () => {
  let capturedMetadata: Metadata | undefined
  let capturedTimestamp: number | undefined
  let capturedContext: unknown
  const testMetadata: Metadata = { correlationId: "test-123" }

  const def: EventHandlerDefinition<typeof SomethingHappened.payload> = eventHandler(
    SomethingHappened,
    async ({ metadata, timestamp }, context) => {
      capturedMetadata = metadata
      capturedTimestamp = timestamp
      capturedContext = context
    },
  )

  expect(def.kind).toBe("event-handler")
  expect(def.descriptor).toBe(SomethingHappened)

  const context = {} as Parameters<typeof def.handler>[1]
  await def.handler(
    {
      kind: "event",
      identifier: "evt-1",
      name: SomethingHappened.name,
      version: SomethingHappened.version,
      payload: { value: 42 },
      metadata: testMetadata,
      timestamp: 1234,
      tags: [],
      sequence: 1n,
    },
    context,
  )

  expect(capturedMetadata).toEqual(testMetadata)
  expect(capturedTimestamp).toBe(1234)
  expect(capturedContext).toBe(context)
})

describe("handler-signature: evolver shape preserved", () => {
  test("evolver registration evolve function receives event message", () => {
    type S = { name: string }
    const message = {
      kind: "event" as const,
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
