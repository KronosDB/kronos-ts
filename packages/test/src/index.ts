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
  type Resources,
  type App,
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

// ── the grammar: given → when → then, and storylines ───────────────────────
export {
  type ScenarioStart,
  type ScenarioActed,
  type Judged,
  type Storyline,
  scenario,
  storyline,
} from "./scenario.js"

// ── where it runs: set once, override per scenario ─────────────────────────
export {
  type Subject,
  type Scope,
  type SiteResources,
  type SiteEventStore,
  type PartialProcessor,
  type Infrastructure,
  type InfrastructureFactory,
  type ScenarioOptions,
  type AdvanceableClock,
  advanceableClock,
  configure,
  FIXTURE_EPOCH,
} from "./site.js"

// ── the failure, which is the product ──────────────────────────────────────
export { ScenarioAssertionError } from "./diff.js"

// ── recorders: thing-first decorators, usable on their own ─────────────────
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
