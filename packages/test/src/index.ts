// ── the vocabulary: a message is a value, an expectation is the same value ──
export {
  type Duration,
  type Any,
  type Expected,
  type EventValue,
  type CommandValue,
  type QueryValue,
  type Action,
  type ErrorMatcher,
  type ResultAssertion,
  type ErrorAssertion,
  type NoEventsAssertion,
  type NoCommandsAssertion,
  type ScheduledAssertion,
  type CancelledAssertion,
  type Assertion,
  any,
  isAny,
  event,
  command,
  query,
  result,
  error,
  noEvents,
  noCommands,
  scheduled,
  cancelled,
} from "./values.js"

// ── the grammar: given → when → then, with waits at the joints ─────────────
export {
  type Step,
  type Scenario,
  type ScenarioStart,
  type ScenarioActed,
  scenario,
  given,
} from "./scenario.js"

// ── the site ───────────────────────────────────────────────────────────────
export {
  type PartialProcessor,
  type FixtureUnitOfWork,
  type FixtureEventHandler,
  type FixtureLists,
  type FixtureScope,
  type FixtureOptions,
  type RunOutcome,
  type TestFixture,
  FIXTURE_EPOCH,
  testFixture,
} from "./fixture.js"

// ── the failure, which is the product ──────────────────────────────────────
export { ScenarioAssertionError } from "./diff.js"

// ── recorders: thing-first decorators, usable outside a fixture ────────────
export {
  type RecordingEventStore,
  type RecordingCommandBus,
  type RecordingQueryBus,
  type ScheduleRecord,
  type ScheduleRecording,
  recordingEventStore,
  recordingCommandBus,
  recordingQueryBus,
  controllableSchedulingEventStore,
} from "./recording.js"
