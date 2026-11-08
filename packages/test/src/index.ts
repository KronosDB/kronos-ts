// ── the vocabulary: a message is a value, an expectation is the same value ──
export {
  type Duration,
  type Any,
  type Expected,
  type EventValue,
  type CommandValue,
  type QueryValue,
  type Action,
  any,
  isAny,
  event,
  command,
  query,
} from "./values.js"

// ── the claims: functions of what happened ─────────────────────────────────
export {
  type Outcome,
  type Assertion,
  type ErrorMatcher,
  events,
  noEvents,
  commands,
  noCommands,
  result,
  error,
  scheduled,
  cancelled,
} from "./values.js"

// ── the grammar: given → when → then, and storylines, against a running app ─
export {
  type Fixture,
  type ScenarioStart,
  type ScenarioActed,
  type Judged,
  type Storyline,
  fixture,
} from "./scenario.js"

export { type FixtureParts, type AdvanceableClock, advanceableClock, FIXTURE_EPOCH } from "./fixture.js"

// ── the failure, which is the product ──────────────────────────────────────
export { ScenarioAssertionError } from "./diff.js"

// ── recorders: what the fixture observes, applied by the test ──────────────
export {
  type RecordingEventStore,
  type RecordingCommandBus,
  type RecordingQueryBus,
  type EventRecording,
  type CommandRecording,
  type ScheduleRecord,
  type ScheduleRecording,
  recordingEventStore,
  recordingCommandBus,
  recordingQueryBus,
  controllableSchedulingEventStore,
} from "./recording.js"
