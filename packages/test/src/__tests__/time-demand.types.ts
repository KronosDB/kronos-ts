/**
 * THE TYPE TEST FOR MOVING TIME.
 *
 * Advancing a clock is a capability the fixture has or does not, so it lives in
 * the type: `.advance` makes a `Scenario<true>`, and only a fixture built on a
 * clock it can MOVE accepts one. The refusal lands at the line that pairs the
 * scenario with the fixture — not as a throw part-way through a run, which is
 * what it used to be.
 *
 * Judged by `bunx tsc --noEmit` through the root tsconfig `files` array.
 */
import { advanceableClock, testFixture, type FixtureLists } from "../index.js"
import { scenario } from "../scenario.js"
import { command, event, noEvents } from "../values.js"
import { qn, command as commandDescriptor, event as eventDescriptor } from "@kronos-ts/core"
import { z } from "zod"

const Do = commandDescriptor({ name: qn("probe", "Do"), payload: z.object({ id: z.string() }) })
const Did = eventDescriptor({ name: qn("probe", "Did"), payload: z.object({ id: z.string() }) })

const scope = (): FixtureLists => ({})

// ---------------------------------------------------------------------------
// The two scenarios: one moves the clock, one only waits for the world.
// ---------------------------------------------------------------------------

const advancing = scenario()
  .when(command(Do, { id: "x" }))
  .advance(1_000)
  .then(noEvents())

const waiting = scenario()
  .when(command(Do, { id: "x" }))
  .await()
  .then(event(Did, { id: "x" }))

const withUntil = scenario()
  .when(command(Do, { id: "x" }))
  .await(({ events }) => events.length === 1, 500)
  .then(event(Did, { id: "x" }))

// ---------------------------------------------------------------------------
// (a) A CLOCK IT CAN MOVE — both kinds run.
// ---------------------------------------------------------------------------

const movable = testFixture(scope, { clock: advanceableClock() })
export const advancesOk = movable.run(advancing)
export const waitsOk = movable.run(waiting)
export const untilOk = movable.run(withUntil)

// A fixture given no clock at all builds an advanceable one, so the default
// stays the ergonomic one: `.advance` works out of the box.
const byDefault = testFixture(scope)
export const defaultAdvances = byDefault.run(advancing)

// ---------------------------------------------------------------------------
// (b) A CLOCK IT CANNOT MOVE — waiting is fine, advancing is refused. This is
// the arrangement a real-infrastructure test is in: nothing here can hurry a
// postgres poller or a kronosdb server, and the type says so up front.
// ---------------------------------------------------------------------------

const readOnly = testFixture(scope, { clock: () => 1_700_000_000_000 })
export const stillWaits = readOnly.run(waiting)
export const stillUntil = readOnly.run(withUntil)

// @ts-expect-error — this scenario moves the clock; this fixture cannot
export const cannotAdvance = readOnly.run(advancing)
