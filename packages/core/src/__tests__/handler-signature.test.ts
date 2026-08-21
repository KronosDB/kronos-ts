/**
 * Type-level tests for handler factories.
 *
 * `on()` — the evolver/query-handler callback-pairing DSL — is deleted.
 * Evolvers are now correlated-tuple DATA on `state({ evolve: [...] })`; see
 * `packages/modelling/src/__tests__/state.test.ts` for evolver coverage and
 * the deliberate-wrong-payload inference check. `QueryHandlerRegistration` is
 * a different, unrelated pattern (nothing collects it into an array via a
 * builder DSL) and is left in place as a plain struct type — see handler.ts.
 *
 * Event handlers are declared with `eventHandler(...)` and yield an
 * `EventHandler`, which is what `.eventHandlers(...)` on the
 * processor builders accepts.
 *
 * Handler context wrapper types (CommandHandlerContext / EventHandlerContext /
 * QueryHandlerContext as handler-shaping types) remain deleted (Plan 04-02).
 */
import { test, expect } from "bun:test"
import { type Metadata, qn, event } from "../messaging/messages.js"
import { z } from "zod"
import { eventHandler, type EventHandler } from "../event-processing/handler.js"
const SomethingHappened = event({
  name: qn("test", "SomethingHappened"),
  payload: z.object({ value: z.number() }),
})

// Test 4: event handlers come from eventHandler() and receive (message, context)
test("eventHandler() is the event-handler factory and takes a context parameter", async () => {
  let capturedMetadata: Metadata | undefined
  let capturedTimestamp: number | undefined
  let capturedContext: unknown
  const testMetadata: Metadata = { correlationId: "test-123" }

  const def: EventHandler<typeof SomethingHappened.payload> = eventHandler(
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
