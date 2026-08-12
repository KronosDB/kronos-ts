import {
  generateIdentifier,
  emptyMetadata,
  qualifiedNameToString,
  type Metadata,
} from "@kronos-ts/common"
import type {
  CommandDescriptor,
  EventDescriptor,
  EventMessage,
  CommandMessage,
} from "@kronos-ts/messaging"
import { runInNewUoW } from "@kronos-ts/messaging"
import {
  kronos,
  inMemoryComponents,
  module as appModule,
  type App,
  type AppModule,
  type Components,
  type Registration,
} from "@kronos-ts/app"
import type { EventStore } from "@kronos-ts/eventsourcing"
import type { z } from "zod"
import {
  recordings,
  recordingComponents,
  recordingOverrides,
  type Recordings,
} from "./recording.js"

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

type EventPair = [EventDescriptor<any>, unknown]
type CommandPair = [CommandDescriptor<any>, unknown]

// ---------------------------------------------------------------------------
// Fixture entry point
// ---------------------------------------------------------------------------

/** Anything the variadic form accepts: a bare registration, or a whole module. */
export type FixtureRegistration = Registration | AppModule

/** The explicit form, for when the defaults are not what you want. */
export interface TestFixtureOptions {
  /** Components to run on. Defaults to `inMemoryComponents()`. */
  components?: Components
  /** Modules to boot, each with its own overrides. */
  modules?: ReadonlyArray<AppModule>
  /** Loose registrations, booted as a single module named `"test"`. */
  register?: ReadonlyArray<Registration>
}

/**
 * Creates a BDD test fixture that runs your REAL application code against
 * in-memory components, with the event store and command bus wrapped so the
 * `then()` phase can assert on what was appended and dispatched.
 *
 * Pass the same registrations you would pass to `module(...)` in production —
 * state modules, command handlers, query handlers, processors, in any order:
 *
 * ```typescript
 * const fixture = testFixture(Course, createCourse, subscribeStudent, getCourseView)
 *
 * await fixture
 *   .given()
 *     .events([CourseCreated, { courseId: "cs-101", name: "Intro", capacity: 30 }])
 *   .when()
 *     .command(SubscribeStudent, { courseId: "cs-101", studentId: "stu-001" })
 *   .then()
 *     .expectSuccess()
 *     .expectEvents([StudentSubscribed, { courseId: "cs-101", studentId: "stu-001" }])
 *
 * await fixture.stop()
 * ```
 *
 * Whole modules may be passed too — `testFixture(billing, ordering)` —
 * and the explicit form takes components and modules by name:
 *
 * ```typescript
 * const fixture = testFixture({
 *   components: inMemoryComponents({ serializer: avroSerializer() }),
 *   modules: [module("billing", { eventStore: postgresEventStore(pool) }, ...slice)],
 * })
 * ```
 *
 * The result is synchronous; `await testFixture(...)` also works, so the
 * awaited call sites of the previous fixture keep compiling.
 */
export function testFixture(...registrations: FixtureRegistration[]): TestFixture
export function testFixture(options: TestFixtureOptions): TestFixture
export function testFixture(
  ...args: [TestFixtureOptions] | FixtureRegistration[]
): TestFixture {
  const options = normalizeOptions(args)
  const recorded = recordings()

  // Recording is composition, not registration: wrap the two traffic-carrying
  // components before handing them to kronos. Because the wrapper IS the
  // component every handler resolves, it sits innermost by construction — no
  // ordering rule to remember.
  const components = recordingComponents(options.components ?? inMemoryComponents(), recorded)

  const modules: AppModule[] = [
    ...(options.register && options.register.length > 0
      ? [appModule("test", ...options.register)]
      : []),
    // A module bringing its OWN store/bus gets its own wrapper, so recordings
    // stay complete across module-scoped persistence.
    ...(options.modules ?? []).map((m) => ({
      ...m,
      overrides: recordingOverrides(m.overrides, recorded),
    })),
  ]

  const app = kronos({ components, modules })
  const eventStore = components.eventStore

  return {
    app,
    recordings: recorded,
    given() {
      return new GivenPhaseImpl(app, recorded, eventStore)
    },
    async stop() {
      await app.stop()
    },
  }
}

function normalizeOptions(
  args: [TestFixtureOptions] | FixtureRegistration[],
): TestFixtureOptions {
  const first = args[0] as Record<string, unknown> | undefined
  const isOptions =
    args.length === 1 &&
    first !== undefined &&
    !("kind" in first) &&
    !("register" in first) &&
    ("components" in first || "modules" in first)
  if (isOptions) return args[0] as TestFixtureOptions

  const register: Registration[] = []
  const modules: AppModule[] = []
  for (const item of args as FixtureRegistration[]) {
    if (item && !("kind" in item) && Array.isArray((item as AppModule).register)) {
      modules.push(item as AppModule)
    } else {
      register.push(item as Registration)
    }
  }
  return { register, modules }
}

export interface TestFixture {
  /** The running app — gateways and per-module state managers. */
  readonly app: App
  /** What the wrapped store and bus have seen since the last reset. */
  readonly recordings: Recordings
  given(): GivenPhase
  stop(): Promise<void>
}

// ---------------------------------------------------------------------------
// Given phase
// ---------------------------------------------------------------------------

export interface GivenPhase {
  /**
   * Seed history by appending straight to the app-level event store.
   *
   * Note: a module that brings its OWN `eventStore` override reads from that
   * store, not this one — seed such a module through `commands(...)` (which
   * goes via the bus and lands in the right store) rather than `events(...)`.
   */
  events(...pairs: EventPair[]): GivenPhase
  commands(...pairs: CommandPair[]): GivenPhase
  execute(fn: (app: App) => void | Promise<void>): GivenPhase
  noPriorActivity(): GivenPhase
  when(): WhenPhase
}

class GivenPhaseImpl implements GivenPhase {
  private readonly givenEvents: EventPair[] = []
  private readonly givenCommands: CommandPair[] = []
  private readonly givenSetupFns: Array<(app: App) => void | Promise<void>> = []
  _prerequisite: Promise<void> | undefined

  constructor(
    private readonly app: App,
    private readonly recordings: Recordings,
    private readonly eventStore: EventStore,
  ) {}

  events(...pairs: EventPair[]): GivenPhase {
    this.givenEvents.push(...pairs)
    return this
  }

  commands(...pairs: CommandPair[]): GivenPhase {
    this.givenCommands.push(...pairs)
    return this
  }

  execute(fn: (app: App) => void | Promise<void>): GivenPhase {
    this.givenSetupFns.push(fn)
    return this
  }

  noPriorActivity(): GivenPhase {
    return this
  }

  when(): WhenPhase {
    return new WhenPhaseImpl(
      this.app, this.recordings, this.eventStore,
      this.givenEvents, this.givenCommands, this.givenSetupFns,
      this._prerequisite,
    )
  }
}

// ---------------------------------------------------------------------------
// When phase
// ---------------------------------------------------------------------------

export interface WhenPhase {
  command<P extends z.ZodType>(descriptor: CommandDescriptor<P>, payload: z.infer<P>, metadata?: Metadata): WhenResult
  event<P extends z.ZodType>(descriptor: EventDescriptor<P>, payload: z.infer<P>, metadata?: Metadata): WhenResult
  nothing(): WhenResult
}

export interface WhenResult {
  then(): ThenPhase
}

class WhenPhaseImpl implements WhenPhase {
  constructor(
    private readonly app: App,
    private readonly recordings: Recordings,
    private readonly eventStore: EventStore,
    private readonly givenEvents: EventPair[],
    private readonly givenCommands: CommandPair[],
    private readonly givenSetupFns: Array<(app: App) => void | Promise<void>>,
    private readonly prerequisite?: Promise<void>,
  ) {}

  command<P extends z.ZodType>(descriptor: CommandDescriptor<P>, payload: z.infer<P>, metadata?: Metadata): WhenResult {
    const thenPhase = new ThenPhaseImpl(
      this.app, this.recordings, this.eventStore,
      this.givenEvents, this.givenCommands, this.givenSetupFns,
      { kind: "command", descriptor, payload, metadata },
      this.prerequisite,
    )
    return { then: () => thenPhase }
  }

  event<P extends z.ZodType>(descriptor: EventDescriptor<P>, payload: z.infer<P>, metadata?: Metadata): WhenResult {
    const thenPhase = new ThenPhaseImpl(
      this.app, this.recordings, this.eventStore,
      this.givenEvents, this.givenCommands, this.givenSetupFns,
      { kind: "event", descriptor, payload, metadata },
      this.prerequisite,
    )
    return { then: () => thenPhase }
  }

  nothing(): WhenResult {
    const thenPhase = new ThenPhaseImpl(
      this.app, this.recordings, this.eventStore,
      this.givenEvents, this.givenCommands, this.givenSetupFns,
      { kind: "nothing" },
      this.prerequisite,
    )
    return { then: () => thenPhase }
  }
}

// ---------------------------------------------------------------------------
// Then phase
// ---------------------------------------------------------------------------

type WhenAction =
  | { kind: "command"; descriptor: CommandDescriptor<any>; payload: unknown; metadata?: Metadata }
  | { kind: "event"; descriptor: EventDescriptor<any>; payload: unknown; metadata?: Metadata }
  | { kind: "nothing" }

export interface ThenPhase extends PromiseLike<void> {
  expectEvents(...pairs: EventPair[]): ThenPhase
  expectNoEvents(): ThenPhase
  expectSuccess(): ThenPhase
  expectResult(expected: unknown): ThenPhase
  expectResultSatisfying(fn: (result: unknown) => void): ThenPhase
  expectResultPayloadSatisfying<T>(fn: (payload: T) => void): ThenPhase
  expectException(messageSubstring: string): ThenPhase
  expectExceptionType(errorName: string): ThenPhase
  expectExceptionSatisfying(fn: (error: unknown) => void): ThenPhase
  expectEventsSatisfying(fn: (events: ReadonlyArray<EventMessage>) => void): ThenPhase
  expectCommands(...pairs: CommandPair[]): ThenPhase
  expectNoCommands(): ThenPhase
  expectCommandsSatisfying(fn: (commands: ReadonlyArray<CommandMessage>) => void): ThenPhase
  expect(fn: (app: App) => void | Promise<void>): ThenPhase
  await(assertion: (app: App) => void | Promise<void>, timeoutMs?: number, intervalMs?: number): ThenPhase
  and(): TestFixture
}

class ThenPhaseImpl implements ThenPhase {
  private readonly assertions: Array<(result: unknown, error: unknown, events: ReadonlyArray<EventMessage>) => void | Promise<void>> = []
  private executionPromise: Promise<void> | null = null

  constructor(
    private readonly app: App,
    private readonly recordings: Recordings,
    private readonly eventStore: EventStore,
    private readonly givenEvents: EventPair[],
    private readonly givenCommands: CommandPair[],
    private readonly givenSetupFns: Array<(app: App) => void | Promise<void>>,
    private readonly whenAction: WhenAction,
    private readonly prerequisite?: Promise<void>,
  ) {}

  expectEvents(...pairs: EventPair[]): ThenPhase {
    this.assertions.push((_result, _error, events) => {
      if (events.length !== pairs.length) {
        throw new FixtureAssertionError(
          `Expected ${pairs.length} event(s) but got ${events.length}.\n` +
          `  Expected: [${pairs.map(([d]) => qualifiedNameToString(d.name)).join(", ")}]\n` +
          `  Actual:   [${events.map((e) => qualifiedNameToString(e.name)).join(", ")}]`,
        )
      }
      for (let i = 0; i < pairs.length; i++) {
        const [desc, payload] = pairs[i]!
        const actual = events[i]!
        const expectedName = qualifiedNameToString(desc.name)
        const actualName = qualifiedNameToString(actual.name)
        if (actualName !== expectedName) {
          throw new FixtureAssertionError(`Event ${i}: expected "${expectedName}" but got "${actualName}"`)
        }
        assertDeepEqual(payload, actual.payload, `Event ${i} (${expectedName}) payload`)
      }
    })
    return this
  }

  expectNoEvents(): ThenPhase {
    this.assertions.push((_result, _error, events) => {
      if (events.length !== 0) {
        throw new FixtureAssertionError(
          `Expected no events but got ${events.length}: ` + events.map((e) => qualifiedNameToString(e.name)).join(", "),
        )
      }
    })
    return this
  }

  expectSuccess(): ThenPhase {
    this.assertions.push((_result, error) => {
      if (error) throw new FixtureAssertionError(`Expected success but command failed: ${error instanceof Error ? error.message : String(error)}`)
    })
    return this
  }

  expectResult(expected: unknown): ThenPhase {
    this.assertions.push((result, error) => {
      if (error) throw new FixtureAssertionError(`Expected result but command failed: ${error}`)
      assertDeepEqual(expected, result, "Command result")
    })
    return this
  }

  expectResultSatisfying(fn: (result: unknown) => void): ThenPhase {
    this.assertions.push((result, error) => {
      if (error) throw new FixtureAssertionError(`Expected result but command failed: ${error}`)
      fn(result)
    })
    return this
  }

  expectResultPayloadSatisfying<T>(fn: (payload: T) => void): ThenPhase {
    this.assertions.push((result, error) => {
      if (error) throw new FixtureAssertionError(`Expected result but command failed: ${error}`)
      fn(result as T)
    })
    return this
  }

  expectException(messageSubstring: string): ThenPhase {
    this.assertions.push((_result, error) => {
      if (!error) throw new FixtureAssertionError(`Expected exception containing "${messageSubstring}" but command succeeded`)
      const msg = error instanceof Error ? error.message : String(error)
      if (!msg.includes(messageSubstring)) {
        throw new FixtureAssertionError(`Expected exception containing "${messageSubstring}" but got: "${msg}"`)
      }
    })
    return this
  }

  expectExceptionType(errorName: string): ThenPhase {
    this.assertions.push((_result, error) => {
      if (!error) throw new FixtureAssertionError(`Expected ${errorName} but command succeeded`)
      const actualName = error instanceof Error ? error.name : "Error"
      if (actualName !== errorName) {
        throw new FixtureAssertionError(`Expected ${errorName} but got ${actualName}: ${error instanceof Error ? error.message : error}`)
      }
    })
    return this
  }

  expectExceptionSatisfying(fn: (error: unknown) => void): ThenPhase {
    this.assertions.push((_result, error) => {
      if (!error) throw new FixtureAssertionError("Expected exception but command succeeded")
      fn(error)
    })
    return this
  }

  expectEventsSatisfying(fn: (events: ReadonlyArray<EventMessage>) => void): ThenPhase {
    this.assertions.push((_result, _error, events) => { fn(events) })
    return this
  }

  expectCommands(...pairs: CommandPair[]): ThenPhase {
    this.assertions.push(() => {
      const actual = this.recordings.commands()
      const handlerCommands = actual.slice(1)
      if (handlerCommands.length !== pairs.length) {
        throw new FixtureAssertionError(
          `Expected ${pairs.length} dispatched command(s) but got ${handlerCommands.length}.`)
      }
      for (let i = 0; i < pairs.length; i++) {
        const [desc, payload] = pairs[i]!
        const actualCmd = handlerCommands[i]!
        const expectedName = qualifiedNameToString(desc.name)
        const actualName = qualifiedNameToString(actualCmd.name)
        if (actualName !== expectedName) throw new FixtureAssertionError(`Command ${i}: expected "${expectedName}" but got "${actualName}"`)
        assertDeepEqual(payload, actualCmd.payload, `Command ${i} (${expectedName}) payload`)
      }
    })
    return this
  }

  expectNoCommands(): ThenPhase {
    this.assertions.push(() => {
      const handlerCommands = this.recordings.commands().slice(1)
      if (handlerCommands.length !== 0) {
        throw new FixtureAssertionError(`Expected no dispatched commands but got ${handlerCommands.length}`)
      }
    })
    return this
  }

  expectCommandsSatisfying(fn: (commands: ReadonlyArray<CommandMessage>) => void): ThenPhase {
    this.assertions.push(() => { fn(this.recordings.commands().slice(1)) })
    return this
  }

  expect(fn: (app: App) => void | Promise<void>): ThenPhase {
    this.assertions.push(async () => { await fn(this.app) })
    return this
  }

  await(assertion: (app: App) => void | Promise<void>, timeoutMs: number = 5000, intervalMs: number = 50): ThenPhase {
    this.assertions.push(async () => {
      const start = Date.now()
      let lastError: unknown
      while (Date.now() - start < timeoutMs) {
        try { await assertion(this.app); return } catch (err) { lastError = err; await new Promise((r) => setTimeout(r, intervalMs)) }
      }
      throw new FixtureAssertionError(`Assertion did not pass within ${timeoutMs}ms. Last error: ${lastError instanceof Error ? lastError.message : String(lastError)}`)
    })
    return this
  }

  and(): TestFixture {
    const prerequisite = this.getExecutionPromise()
    return {
      app: this.app,
      recordings: this.recordings,
      given: () => {
        const given = new GivenPhaseImpl(this.app, this.recordings, this.eventStore)
        given._prerequisite = prerequisite
        return given
      },
      stop: async () => { await this.app.stop() },
    }
  }

  then<TResult1 = void, TResult2 = never>(
    onfulfilled?: ((value: void) => TResult1 | PromiseLike<TResult1>) | null,
    onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
  ): PromiseLike<TResult1 | TResult2> {
    return this.getExecutionPromise().then(onfulfilled, onrejected)
  }

  private getExecutionPromise(): Promise<void> {
    if (!this.executionPromise) this.executionPromise = this.execute()
    return this.executionPromise
  }

  private async execute(): Promise<void> {
    if (this.prerequisite) await this.prerequisite

    const eventStore = this.eventStore

    // 1. Given: publish events within a UnitOfWork
    if (this.givenEvents.length > 0) {
      await runInNewUoW(emptyMetadata(), async () => {
        const events: EventMessage[] = this.givenEvents.map(([desc, payload]) => {
          const tags = desc.tags ? desc.tags(payload) : []
          return { kind: "event" as const, identifier: generateIdentifier(), name: desc.name, version: desc.version, payload, metadata: emptyMetadata(), timestamp: Date.now(), tags }
        })
        await eventStore.append(events)
      })
    }

    // 1b. Given: run custom setup
    for (const fn of this.givenSetupFns) { await fn(this.app) }

    // 1c. Given: dispatch commands via the gateway (matches user-facing semantics).
    //     The recording decorator on the bus captures messages whether they
    //     arrive via gateway or direct bus dispatch.
    for (const [desc, payload] of this.givenCommands) {
      await this.app.commandGateway.send(desc, payload, emptyMetadata())
    }

    // 2. Reset recordings
    this.recordings.reset()

    // 3. When
    let result: unknown
    let error: unknown

    if (this.whenAction.kind === "command") {
      try {
        result = await this.app.commandGateway.send(
          this.whenAction.descriptor,
          this.whenAction.payload,
          this.whenAction.metadata ?? emptyMetadata(),
        )
      } catch (err) { error = err }
    } else if (this.whenAction.kind === "event") {
      const desc = this.whenAction.descriptor
      const payload = this.whenAction.payload
      const tags = desc.tags ? desc.tags(payload) : []
      try {
        await eventStore.append([{ kind: "event", identifier: generateIdentifier(), name: desc.name, version: desc.version, payload, metadata: this.whenAction.metadata ?? emptyMetadata(), timestamp: Date.now(), tags }])
      } catch (err) { error = err }
    }

    // 4. Then
    const recordedEvents = this.recordings.events()
    for (const assertion of this.assertions) { await assertion(result, error, recordedEvents) }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export class FixtureAssertionError extends Error {
  constructor(message: string) { super(message); this.name = "FixtureAssertionError" }
}

export type FieldFilter = (fieldName: string, owner: unknown) => boolean
export const allFieldsFilter: FieldFilter = () => true
export function ignoreFields(...fieldNames: string[]): FieldFilter {
  const ignored = new Set(fieldNames)
  return (name) => !ignored.has(name)
}

function assertDeepEqual(expected: unknown, actual: unknown, label: string, fieldFilter: FieldFilter = allFieldsFilter): void {
  const differences = deepCompare(expected, actual, "", fieldFilter)
  if (differences.length > 0) throw new FixtureAssertionError(`${label} mismatch:\n${differences.map((d) => `  ${d}`).join("\n")}`)
}

function deepCompare(expected: unknown, actual: unknown, path: string, fieldFilter: FieldFilter): string[] {
  if (expected === actual) return []
  if (expected === null || actual === null) return [`${path || "root"}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`]
  if (typeof expected !== typeof actual) return [`${path || "root"}: expected type ${typeof expected}, got type ${typeof actual}`]
  if (typeof expected !== "object") return [`${path || "root"}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`]
  if (Array.isArray(expected) !== Array.isArray(actual)) return [`${path || "root"}: expected ${Array.isArray(expected) ? "array" : "object"}, got ${Array.isArray(actual) ? "array" : "object"}`]
  if (Array.isArray(expected) && Array.isArray(actual)) {
    const diffs: string[] = []
    for (let i = 0; i < Math.max(expected.length, actual.length); i++) {
      if (i >= expected.length) diffs.push(`${path}[${i}]: unexpected extra element`)
      else if (i >= actual.length) diffs.push(`${path}[${i}]: missing expected element`)
      else diffs.push(...deepCompare(expected[i], actual[i], `${path}[${i}]`, fieldFilter))
    }
    return diffs
  }
  const diffs: string[] = []
  const allKeys = new Set([...Object.keys(expected as any), ...Object.keys(actual as any)])
  for (const key of allKeys) {
    if (!fieldFilter(key, expected)) continue
    const fieldPath = path ? `${path}.${key}` : key
    if (!(key in (expected as any))) diffs.push(`${fieldPath}: unexpected field`)
    else if (!(key in (actual as any))) diffs.push(`${fieldPath}: missing expected value`)
    else diffs.push(...deepCompare((expected as any)[key], (actual as any)[key], fieldPath, fieldFilter))
  }
  return diffs
}
