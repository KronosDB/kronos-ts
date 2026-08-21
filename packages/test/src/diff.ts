import { qualifiedNameToString } from "@kronos-ts/core"
import type { CommandMessage, EventMessage, Metadata } from "@kronos-ts/core"
import type { Scenario } from "./scenario.js"
import { isAny } from "./values.js"
import type { Assertion, CommandValue, EventValue } from "./values.js"
import type { ScheduleRecord } from "./recording.js"

// ---------------------------------------------------------------------------
// The failure message IS the product.
//
// A test that fails has to answer one question — what is different — and it has
// to answer it without the reader opening a debugger. So: both lists in full so
// the SHAPE of the mismatch is visible at a glance, names aligned by best match
// so one missing event in the middle reports as one missing event instead of
// shifting every later pair into a spurious mismatch, and a field-level diff on
// the pairs that did line up, because that is the line the reader actually
// needs.
// ---------------------------------------------------------------------------

/** A scenario's claims did not hold. The message is the diff. */
export class ScenarioAssertionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "ScenarioAssertionError"
  }
}

/** What one act actually did. */
export type Observed = {
  readonly result: unknown
  readonly threw: boolean
  readonly thrown: unknown
  readonly events: ReadonlyArray<EventMessage>
  readonly commands: ReadonlyArray<CommandMessage>
  readonly schedules: ReadonlyArray<ScheduleRecord>
}

/**
 * Judge a scenario's claims against what happened.
 *
 * Returns the failure text, or `undefined` when everything asserted held. It
 * does not throw, because a real-infrastructure scope is judged REPEATEDLY until
 * it settles or the patience runs out — and "not yet" and "wrong" have to be the
 * same answer for that loop to be writable.
 */
export function evaluate(
  scenario: Scenario,
  observed: Observed,
  actKind: "command" | "query" | "event",
): string | undefined {
  const sections: string[] = []

  const expectedEvents = scenario.then.filter(isEventValue)
  const expectedCommands = scenario.then.filter(isCommandValue)
  const noEvents = scenario.then.some((a) => a.kind === "no-events")
  const noCommands = scenario.then.some((a) => a.kind === "no-commands")
  const resultAssertion = scenario.then.find((a) => a.kind === "result")
  const errorAssertion = scenario.then.find((a) => a.kind === "error")

  // The throw comes first: everything downstream of a handler that blew up is
  // noise, and an events diff over an act that never ran is a lie about what
  // went wrong.
  if (errorAssertion !== undefined) {
    const failure = judgeError(errorAssertion.matcher, observed)
    if (failure !== undefined) return headed(scenario, [failure])
  }

  if (noEvents || expectedEvents.length > 0) {
    const failure = judgeMessages(
      "events",
      "appended",
      expectedEvents.map((e) => ({ value: e, name: qualifiedNameToString(e.descriptor.name) })),
      observed.events.map((e) => ({
        name: qualifiedNameToString(e.name),
        payload: e.payload,
        metadata: e.metadata,
      })),
    )
    if (failure !== undefined) sections.push(failure)
  }

  if (noCommands || expectedCommands.length > 0) {
    const failure = judgeMessages(
      "commands",
      "dispatched",
      expectedCommands.map((c) => ({ value: c, name: qualifiedNameToString(c.descriptor.name) })),
      observed.commands.map((c) => ({
        name: qualifiedNameToString(c.name),
        payload: c.payload,
        metadata: c.metadata,
      })),
    )
    if (failure !== undefined) sections.push(failure)
  }

  if (resultAssertion !== undefined) {
    const differences = compare(resultAssertion.value, observed.result, "")
    if (differences.length > 0) {
      sections.push(
        `the ${actKind === "query" ? "query's answer" : "command's result"} is not what \`result()\` claimed.\n` +
          `  expected: ${preview(resultAssertion.value)}\n` +
          `  actual:   ${preview(observed.result)}\n` +
          `  differences:\n${indent(differences, 4)}`,
      )
    }
  }

  for (const assertion of scenario.then) {
    if (assertion.kind === "scheduled") {
      const failure = judgeScheduled(assertion.event, assertion.after, observed.schedules)
      if (failure !== undefined) sections.push(failure)
    } else if (assertion.kind === "cancelled") {
      const failure = judgeCancelled(assertion.event, observed.schedules)
      if (failure !== undefined) sections.push(failure)
    }
  }

  return sections.length === 0 ? undefined : headed(scenario, sections)
}

function headed(scenario: Scenario, sections: ReadonlyArray<string>): string {
  return [`${scenario.description}\n`, ...sections].join("\n")
}

function isEventValue(assertion: Assertion): assertion is EventValue<any> {
  return assertion.kind === "event"
}

function isCommandValue(assertion: Assertion): assertion is CommandValue<any, any> {
  return assertion.kind === "command"
}

// ── the throw ──────────────────────────────────────────────────────────────

function judgeError(
  matcher: string | RegExp | ((error: unknown) => boolean),
  observed: Observed,
): string | undefined {
  if (!observed.threw) {
    return `expected the act to throw ${describeMatcher(matcher)}, but it completed successfully.`
  }
  const thrown = observed.thrown
  const message = thrown instanceof Error ? thrown.message : String(thrown)
  const name = thrown instanceof Error ? thrown.name : typeof thrown

  if (typeof matcher === "string") {
    if (message.includes(matcher)) return undefined
  } else if (matcher instanceof RegExp) {
    if (matcher.test(message)) return undefined
  } else if (matcher(thrown)) {
    return undefined
  }
  return (
    `expected the act to throw ${describeMatcher(matcher)}.\n` + `  thrown: ${name}: "${message}"`
  )
}

function describeMatcher(matcher: string | RegExp | ((error: unknown) => boolean)): string {
  if (typeof matcher === "string") return `an error whose message contains "${matcher}"`
  if (matcher instanceof RegExp) return `an error whose message matches ${String(matcher)}`
  return "an error the predicate accepts"
}

// ── the message lists ──────────────────────────────────────────────────────

type ExpectedMessage = {
  readonly value: EventValue<any> | CommandValue<any, any>
  readonly name: string
}

type ActualMessage = {
  readonly name: string
  readonly payload: unknown
  readonly metadata: Metadata | undefined
}

/**
 * The events (or commands) diff — an EXACT, ORDERED list claim.
 *
 * Nothing missing, nothing extra, in this order. Which is why `noEvents()` needs
 * no separate code path: it is the empty list, and an empty expectation against
 * something appended reports the something as unexpected.
 */
function judgeMessages(
  what: string,
  verb: string,
  expected: ReadonlyArray<ExpectedMessage>,
  actual: ReadonlyArray<ActualMessage>,
): string | undefined {
  const missing: string[] = []
  const unexpected: string[] = []
  const differing: string[] = []

  for (const slot of align(
    expected.map((e) => e.name),
    actual.map((a) => a.name),
  )) {
    if (slot.expectedIndex !== undefined && slot.actualIndex !== undefined) {
      const want = expected[slot.expectedIndex]!
      const got = actual[slot.actualIndex]!
      const differences = compare(want.value.payload, got.payload, "")
      if (want.value.metadata !== undefined) {
        differences.push(...compareMetadata(want.value.metadata, got.metadata))
      }
      if (differences.length === 0) continue
      differing.push(`~ [${slot.expectedIndex}] ${want.name}\n${indent(differences, 8)}`)
    } else if (slot.expectedIndex !== undefined) {
      const want = expected[slot.expectedIndex]!
      missing.push(`- [${slot.expectedIndex}] ${want.name}  ${preview(want.value.payload)}`)
    } else {
      const got = actual[slot.actualIndex!]!
      unexpected.push(`+ [${slot.actualIndex}] ${got.name}  ${preview(got.payload)}`)
    }
  }

  if (missing.length === 0 && unexpected.length === 0 && differing.length === 0) return undefined

  const parts = [
    `${what} did not match what the act ${verb}.`,
    `  expected (${expected.length}):\n${listing(expected.map((e) => e.name))}`,
    `  ${verb} (${actual.length}):\n${listing(actual.map((a) => a.name))}`,
  ]
  if (missing.length > 0) {
    parts.push(``, `  missing — expected, never ${verb}:\n${indent(missing, 4)}`)
  }
  if (unexpected.length > 0) {
    parts.push(``, `  unexpected — ${verb}, never expected:\n${indent(unexpected, 4)}`)
  }
  if (differing.length > 0) {
    parts.push(``, `  payload differences:\n${indent(differing, 4)}`)
  }
  return parts.join("\n")
}

function listing(names: ReadonlyArray<string>): string {
  if (names.length === 0) return "    (none)"
  return names.map((name, i) => `    [${i}] ${name}`).join("\n")
}

function indent(lines: ReadonlyArray<string>, spaces: number): string {
  const pad = " ".repeat(spaces)
  return lines.map((line) => `${pad}${line}`).join("\n")
}

/** One aligned slot: an expectation, an actual message, or both. */
type Alignment = {
  readonly expectedIndex?: number
  readonly actualIndex?: number
}

/** Longest-common-subsequence alignment of two name lists. */
function align(expected: ReadonlyArray<string>, actual: ReadonlyArray<string>): Alignment[] {
  const n = expected.length
  const m = actual.length
  const lcs: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0))
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      lcs[i]![j] =
        expected[i] === actual[j]
          ? lcs[i + 1]![j + 1]! + 1
          : Math.max(lcs[i + 1]![j]!, lcs[i]![j + 1]!)
    }
  }

  const slots: Alignment[] = []
  let i = 0
  let j = 0
  while (i < n && j < m) {
    if (expected[i] === actual[j]) {
      slots.push({ expectedIndex: i, actualIndex: j })
      i++
      j++
    } else if (lcs[i + 1]![j]! >= lcs[i]![j + 1]!) {
      slots.push({ expectedIndex: i })
      i++
    } else {
      slots.push({ actualIndex: j })
      j++
    }
  }
  while (i < n) slots.push({ expectedIndex: i++ })
  while (j < m) slots.push({ actualIndex: j++ })
  return slots
}

// ── schedules ──────────────────────────────────────────────────────────────

function judgeScheduled(
  expected: EventValue<any>,
  after: number,
  schedules: ReadonlyArray<ScheduleRecord>,
): string | undefined {
  const name = qualifiedNameToString(expected.descriptor.name)
  const sameName = schedules.filter((s) => qualifiedNameToString(s.event.name) === name)
  const matches = sameName.filter(
    (s) =>
      s.status !== "cancelled" &&
      s.fireAt - s.armedAt === after &&
      compare(expected.payload, s.event.payload, "").length === 0,
  )
  if (matches.length > 0) return undefined

  return (
    `no schedule matches \`scheduled(${name}, ${after})\`.\n` +
    `  expected: ${name} ${preview(expected.payload)} armed to fire after ${after}ms\n` +
    `  armed (${schedules.length}):\n${scheduleListing(schedules)}`
  )
}

function judgeCancelled(
  expected: EventValue<any>,
  schedules: ReadonlyArray<ScheduleRecord>,
): string | undefined {
  const name = qualifiedNameToString(expected.descriptor.name)
  const matches = schedules.filter(
    (s) =>
      s.status === "cancelled" &&
      qualifiedNameToString(s.event.name) === name &&
      compare(expected.payload, s.event.payload, "").length === 0,
  )
  if (matches.length > 0) return undefined

  return (
    `no cancelled schedule matches \`cancelled(${name})\`.\n` +
    `  expected: ${name} ${preview(expected.payload)} cancelled before firing\n` +
    `  armed (${schedules.length}):\n${scheduleListing(schedules)}`
  )
}

function scheduleListing(schedules: ReadonlyArray<ScheduleRecord>): string {
  if (schedules.length === 0) return "    (none)"
  return schedules
    .map(
      (s, i) =>
        `    [${i}] ${qualifiedNameToString(s.event.name)} ${preview(s.event.payload)}` +
        ` after ${s.fireAt - s.armedAt}ms (${s.status})`,
    )
    .join("\n")
}

// ── the value comparison ───────────────────────────────────────────────────

/** Metadata is a SUBSET claim: only the keys the scenario named are looked at. */
function compareMetadata(expected: Metadata, actual: Metadata | undefined): string[] {
  const differences: string[] = []
  const right = (actual ?? {}) as Record<string, unknown>
  for (const [key, value] of Object.entries(expected as Record<string, unknown>)) {
    differences.push(...compare(value, right[key], `metadata.${key}`))
  }
  return differences
}

/**
 * Field-by-field differences as readable lines, with {@link isAny} holes honoured
 * at any depth.
 *
 * Deep-STRICT everywhere a hole is not: a field the scenario named must match,
 * and a field the actual value carries that the scenario did not name is
 * reported. Silent tolerance is how a payload quietly grows a field nobody
 * meant to add.
 */
export function compare(expected: unknown, actual: unknown, path: string): string[] {
  const here = path || "value"

  if (isAny(expected)) {
    if (expected.schema === undefined) return []
    // Any Standard Schema. `compare` is synchronous — a diff is data, computed
    // and rendered in one breath — so a schema whose validation is asynchronous
    // is reported as the mistake it is rather than silently passing.
    const validated = expected.schema["~standard"].validate(actual)
    if (validated instanceof Promise) {
      return [`${here}: the schema \`any()\` was given validates asynchronously, which a diff cannot await`]
    }
    if (!validated.issues) return []
    return [`${here}: ${preview(actual)} does not satisfy the schema \`any()\` was given`]
  }

  if (Object.is(expected, actual)) return []
  if (expected === null || actual === null || expected === undefined || actual === undefined) {
    return [`${here}: expected ${preview(expected)}, got ${preview(actual)}`]
  }
  if (typeof expected !== typeof actual) {
    return [
      `${here}: expected ${typeof expected} ${preview(expected)}, got ${typeof actual} ${preview(actual)}`,
    ]
  }
  if (typeof expected !== "object") {
    return [`${here}: expected ${preview(expected)}, got ${preview(actual)}`]
  }
  if (Array.isArray(expected) !== Array.isArray(actual)) {
    return [`${here}: expected ${preview(expected)}, got ${preview(actual)}`]
  }
  if (Array.isArray(expected) && Array.isArray(actual)) {
    const differences: string[] = []
    for (let i = 0; i < Math.max(expected.length, actual.length); i++) {
      if (i >= expected.length) differences.push(`${path}[${i}]: unexpected ${preview(actual[i])}`)
      else if (i >= actual.length)
        differences.push(`${path}[${i}]: missing ${preview(expected[i])}`)
      else differences.push(...compare(expected[i], actual[i], `${path}[${i}]`))
    }
    return differences
  }

  const differences: string[] = []
  const left = expected as Record<string, unknown>
  const right = actual as Record<string, unknown>
  for (const key of new Set([...Object.keys(left), ...Object.keys(right)])) {
    const field = path ? `${path}.${key}` : key
    if (!(key in left)) differences.push(`${field}: unexpected field ${preview(right[key])}`)
    else if (!(key in right)) differences.push(`${field}: missing field ${preview(left[key])}`)
    else differences.push(...compare(left[key], right[key], field))
  }
  return differences
}

/**
 * A short, readable rendering — with holes as `*`, because that is what a hole
 * looks like when you are reading a diff rather than writing one.
 */
export function preview(value: unknown): string {
  if (isAny(value)) return "*"
  let rendered: string
  try {
    rendered = JSON.stringify(value, (_key, v: unknown) => (isAny(v) ? "*" : v)) ?? String(value)
  } catch {
    rendered = String(value)
  }
  return rendered.length > 200 ? `${rendered.slice(0, 197)}...` : rendered
}
