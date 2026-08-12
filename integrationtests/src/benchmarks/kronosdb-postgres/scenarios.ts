import { z } from "zod"
import { emptyMetadata, qn, tag, type Metadata } from "@kronos-ts/common"
import {
  EventCriteria,
  trackingEventProcessor,
  event,
  eventHandler,
  type EventMessage,
} from "@kronos-ts/messaging"
import type { EventStore } from "@kronos-ts/eventsourcing"
import type { BackendHarness } from "./backends.js"
import {
  BENCH_EVENT_NAME,
  deterministicUuid,
  makeEvent,
  makePayload,
  type BenchPayload,
  type BenchmarkOptions,
  type ScenarioName,
} from "./config.js"
import { percentile, type MetricValue, type ScenarioSample } from "./report.js"

const BenchEvent = event({
  name: BENCH_EVENT_NAME,
  payload: z.object({
    aggregateId: z.string(),
    ordinal: z.number(),
    delta: z.number(),
    commandId: z.string(),
    description: z.string(),
    attributes: z.object({
      region: z.string(),
      priority: z.number(),
      labels: z.array(z.string()),
    }),
  }),
  tags: (payload) => [tag("aggregateId", payload.aggregateId)],
})

export interface ScenarioRun {
  readonly samples: readonly ScenarioSample[]
  readonly correctnessChecks: number
}

function assert(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(`correctness check failed: ${message}`)
}

function payloadOf(eventMessage: EventMessage): BenchPayload {
  return eventMessage.payload as BenchPayload
}

function metrics(values: {
  throughputName: string
  throughput: number
  latencies: readonly number[]
}): MetricValue[] {
  return [
    { name: values.throughputName, value: values.throughput, unit: "events/s", better: "higher" },
    { name: "latencyP50Ms", value: percentile(values.latencies, 50), unit: "ms", better: "lower" },
    { name: "latencyP95Ms", value: percentile(values.latencies, 95), unit: "ms", better: "lower" },
    { name: "latencyP99Ms", value: percentile(values.latencies, 99), unit: "ms", better: "lower" },
    { name: "latencyMaxMs", value: Math.max(...values.latencies), unit: "ms", better: "lower" },
  ]
}

function verifyPayloads(events: readonly EventMessage[], expected: number, label: string): number {
  assert(events.length === expected, `${label}: expected ${expected} events, got ${events.length}`)
  const identifiers = new Set<string>()
  let checksum = 0
  for (let i = 0; i < events.length; i++) {
    const current = events[i]!
    const payload = payloadOf(current)
    assert(payload.ordinal === i, `${label}: ordinal ${i} missing (got ${payload.ordinal})`)
    assert(!identifiers.has(current.identifier), `${label}: duplicate identifier ${current.identifier}`)
    identifiers.add(current.identifier)
    checksum += payload.delta
  }
  const expectedChecksum = Array.from({ length: expected }, (_, ordinal) => (ordinal % 7) + 1)
    .reduce((sum, value) => sum + value, 0)
  assert(checksum === expectedChecksum, `${label}: payload checksum mismatch`)
  return 4
}

async function sourceAggregate(store: EventStore, aggregateId: string) {
  return store.source({ criteria: EventCriteria.havingTags(tag("aggregateId", aggregateId)) })
}

async function seedEvents(
  store: EventStore,
  aggregateId: string,
  count: number,
  seed: string,
  batchSize = 500,
): Promise<void> {
  for (let from = 0; from < count; from += batchSize) {
    const to = Math.min(count, from + batchSize)
    const batch = Array.from({ length: to - from }, (_, offset) =>
      makeEvent(aggregateId, from + offset, seed),
    )
    await store.append(batch)
  }
}

async function runAppend(harness: BackendHarness, options: BenchmarkOptions): Promise<ScenarioRun> {
  const samples: ScenarioSample[] = []
  let checks = 0
  for (const shape of options.profile.append) {
    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      const id = `warmup-append-b${shape.batchSize}-${warmup}`
      await harness.store.append(Array.from({ length: Math.min(shape.batchSize, 10) }, (_, ordinal) =>
        makeEvent(id, ordinal, `${options.seed}:${harness.name}`),
      ))
    }

    for (let sample = 0; sample < options.samples; sample++) {
      const aggregateId = `append-${harness.name}-b${shape.batchSize}-s${sample}`
      const seed = `${options.seed}:${harness.name}:append:b${shape.batchSize}:s${sample}`
      const totalEvents = shape.batchSize * shape.commits
      const allEvents = Array.from({ length: totalEvents }, (_, ordinal) =>
        makeEvent(aggregateId, ordinal, seed),
      )
      const latencies: number[] = []
      const started = performance.now()
      for (let commit = 0; commit < shape.commits; commit++) {
        const from = commit * shape.batchSize
        const before = performance.now()
        await harness.store.append(allEvents.slice(from, from + shape.batchSize))
        latencies.push(performance.now() - before)
      }
      const elapsedMs = performance.now() - started

      const sourced = await sourceAggregate(harness.store, aggregateId)
      checks += verifyPayloads(sourced.events, totalEvents, `${harness.name} append b${shape.batchSize} s${sample}`)
      const roundTrip = payloadOf(sourced.events[Math.floor(totalEvents / 2)]!)
      assert(roundTrip.description.includes("state change"), "JSON payload did not round-trip")
      checks++

      samples.push({
        backend: harness.name,
        scenario: "append",
        parameters: { batchSize: shape.batchSize },
        sample,
        metrics: [
          ...metrics({ throughputName: "eventsPerSec", throughput: totalEvents / (elapsedMs / 1_000), latencies }),
          { name: "commitsPerSec", value: shape.commits / (elapsedMs / 1_000), unit: "commits/s", better: "higher" },
        ],
        diagnostics: { elapsedMs, totalEvents, commits: shape.commits },
      })
    }
  }
  return { samples, correctnessChecks: checks }
}

async function runAppendConcurrency(
  harness: BackendHarness,
  options: BenchmarkOptions,
): Promise<ScenarioRun> {
  const samples: ScenarioSample[] = []
  let checks = 0
  for (const workers of options.profile.appendConcurrency) {
    const perWorker = Math.max(1, Math.floor(options.profile.appendConcurrencyCommits / workers))
    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      await Promise.all(Array.from({ length: workers }, (_, worker) =>
        seedEvents(harness.store, `appendc-warmup-c${workers}-${warmup}-w${worker}`, 3,
          `${options.seed}:${harness.name}:appendc-warmup`, 1),
      ))
    }
    for (let sample = 0; sample < options.samples; sample++) {
      const latencies: number[] = []
      const started = performance.now()
      await Promise.all(Array.from({ length: workers }, (_, worker) => (async () => {
        const aggregateId = `appendc-${harness.name}-c${workers}-s${sample}-w${worker}`
        const seed = `${options.seed}:${harness.name}:${aggregateId}`
        for (let ordinal = 0; ordinal < perWorker; ordinal++) {
          const before = performance.now()
          await harness.store.append([makeEvent(aggregateId, ordinal, seed)])
          latencies.push(performance.now() - before)
        }
      })()))
      const elapsedMs = performance.now() - started
      const totalEvents = workers * perWorker

      for (let worker = 0; worker < workers; worker += Math.max(1, Math.floor(workers / 4))) {
        const aggregateId = `appendc-${harness.name}-c${workers}-s${sample}-w${worker}`
        const sourced = await sourceAggregate(harness.store, aggregateId)
        checks += verifyPayloads(sourced.events, perWorker, aggregateId)
      }

      samples.push({
        backend: harness.name,
        scenario: "append-c",
        parameters: { concurrency: workers, batchSize: 1 },
        sample,
        metrics: metrics({
          throughputName: "eventsPerSec",
          throughput: totalEvents / (elapsedMs / 1_000),
          latencies,
        }),
        diagnostics: { elapsedMs, totalEvents, perWorker },
      })
    }
  }
  return { samples, correctnessChecks: checks }
}

function reduceState(events: readonly EventMessage[]): { count: number; value: number } {
  let value = 0
  for (const message of events) value += payloadOf(message).delta
  return { count: events.length, value }
}

async function workflowPass(
  harness: BackendHarness,
  options: BenchmarkOptions,
  workers: number,
  perWorker: number,
  key: string,
): Promise<{ elapsedMs: number; latencies: number[]; checks: number }> {
  const seedDepth = options.profile.workflowSeedDepth
  const aggregateIds = Array.from({ length: workers }, (_, worker) => `${key}-w${worker}`)
  await Promise.all(aggregateIds.map((aggregateId) =>
    seedEvents(harness.store, aggregateId, seedDepth, `${options.seed}:${harness.name}:${aggregateId}`, 100),
  ))

  const latencies: number[] = []
  const started = performance.now()
  await Promise.all(aggregateIds.map((aggregateId) => (async () => {
    const seed = `${options.seed}:${harness.name}:${aggregateId}`
    for (let operation = 0; operation < perWorker; operation++) {
      const before = performance.now()
      const sourced = await sourceAggregate(harness.store, aggregateId)
      const state = reduceState(sourced.events)
      assert(state.count === seedDepth + operation, `${aggregateId}: unexpected sourced history length`)
      await harness.store.append(
        [makeEvent(aggregateId, state.count, seed)],
        { criteria: EventCriteria.havingTags(tag("aggregateId", aggregateId)), marker: sourced.marker },
      )
      latencies.push(performance.now() - before)
    }
  })()))
  const elapsedMs = performance.now() - started

  let checks = 0
  for (const aggregateId of aggregateIds) {
    const sourced = await sourceAggregate(harness.store, aggregateId)
    checks += verifyPayloads(sourced.events, seedDepth + perWorker, aggregateId)
    const state = reduceState(sourced.events)
    assert(state.count === seedDepth + perWorker, `${aggregateId}: final state count mismatch`)
    checks++
  }
  return { elapsedMs, latencies, checks }
}

async function runWorkflow(harness: BackendHarness, options: BenchmarkOptions): Promise<ScenarioRun> {
  const samples: ScenarioSample[] = []
  let checks = 0
  for (const workers of options.profile.workflowConcurrency) {
    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      await workflowPass(harness, options, workers, 3, `workflow-warmup-c${workers}-${warmup}`)
    }
    for (let sample = 0; sample < options.samples; sample++) {
      const result = await workflowPass(
        harness,
        options,
        workers,
        options.profile.workflowPerWorker,
        `workflow-${harness.name}-c${workers}-s${sample}`,
      )
      checks += result.checks
      const operations = workers * options.profile.workflowPerWorker
      samples.push({
        backend: harness.name,
        scenario: "workflow",
        parameters: { concurrency: workers },
        sample,
        metrics: metrics({
          throughputName: "commandsPerSec",
          throughput: operations / (result.elapsedMs / 1_000),
          latencies: result.latencies,
        }),
        diagnostics: {
          elapsedMs: result.elapsedMs,
          operations,
          eventsPerSourceStart: options.profile.workflowSeedDepth,
          eventsPerSourceEnd: options.profile.workflowSeedDepth + options.profile.workflowPerWorker - 1,
        },
      })
    }
  }
  return { samples, correctnessChecks: checks }
}

// ---------------------------------------------------------------------------
// DCB scenario — a consistency boundary spanning many event types.
//
// Each command's criteria is an OR of DCB_KINDS branches, one per event type,
// each restricted to that type's own tag key. This is the "wide" DCB shape
// (multi-entity invariant) as opposed to the workflow scenario's single
// aggregateId tag: the store has to resolve many type+tag branches per
// source AND per conditional-append conflict check, against a log that also
// contains unrelated noise events of the same types.
// ---------------------------------------------------------------------------

const DCB_KINDS = 20

function dcbEventName(kind: number) {
  return qn("benchmark", `Entity${kind}Changed`)
}

function makeDcbEvent(caseId: string, kind: number, ordinal: number, seed: string): EventMessage {
  return {
    kind: "event",
    identifier: deterministicUuid(`${seed}:dcb:${caseId}:${kind}:${ordinal}`),
    name: dcbEventName(kind),
    payload: makePayload(caseId, ordinal, seed),
    metadata: emptyMetadata() as Metadata,
    timestamp: 1_750_000_000_000 + ordinal,
    version: "1",
    tags: [tag(`entity${kind}`, caseId)],
  } as EventMessage
}

function dcbCriteria(caseId: string): EventCriteria {
  return EventCriteria.either(
    ...Array.from({ length: DCB_KINDS }, (_, kind) =>
      EventCriteria.havingTags(tag(`entity${kind}`, caseId)).ofTypes(dcbEventName(kind)),
    ),
  )
}

async function dcbPass(
  harness: BackendHarness,
  options: BenchmarkOptions,
  workers: number,
  perWorker: number,
  key: string,
): Promise<{ elapsedMs: number; latencies: number[]; checks: number }> {
  const caseIds = Array.from({ length: workers }, (_, worker) => `${key}-w${worker}`)
  // Seed one event per entity kind for every case (the boundary's history).
  await Promise.all(caseIds.map((caseId) =>
    harness.store.append(Array.from({ length: DCB_KINDS }, (_, kind) =>
      makeDcbEvent(caseId, kind, kind, `${options.seed}:${harness.name}:${caseId}`),
    )),
  ))

  const latencies: number[] = []
  const started = performance.now()
  let checks = 0
  await Promise.all(caseIds.map((caseId) => (async () => {
    const seed = `${options.seed}:${harness.name}:${caseId}`
    const criteria = dcbCriteria(caseId)
    for (let operation = 0; operation < perWorker; operation++) {
      const before = performance.now()
      const sourced = await harness.store.source({ criteria })
      let value = 0
      for (const message of sourced.events) value += payloadOf(message).delta
      assert(
        sourced.events.length === DCB_KINDS + operation,
        `${caseId}: expected ${DCB_KINDS + operation} events across the boundary, got ${sourced.events.length}`,
      )
      assert(value > 0, `${caseId}: reduced state is empty`)
      await harness.store.append(
        [makeDcbEvent(caseId, operation % DCB_KINDS, DCB_KINDS + operation, seed)],
        { criteria, marker: sourced.marker },
      )
      latencies.push(performance.now() - before)
    }
    checks += perWorker * 2
  })()))
  const elapsedMs = performance.now() - started

  for (const caseId of caseIds) {
    const sourced = await harness.store.source({ criteria: dcbCriteria(caseId) })
    assert(
      sourced.events.length === DCB_KINDS + perWorker,
      `${caseId}: final boundary has ${sourced.events.length} events, expected ${DCB_KINDS + perWorker}`,
    )
    const identifiers = new Set(sourced.events.map((message) => message.identifier))
    assert(identifiers.size === sourced.events.length, `${caseId}: duplicate identifiers in boundary`)
    checks += 2
  }
  return { elapsedMs, latencies, checks }
}

async function runDcb(harness: BackendHarness, options: BenchmarkOptions): Promise<ScenarioRun> {
  // Noise the criteria must discriminate against: same event types, foreign
  // case ids, spread across every kind.
  const noiseSeed = `${options.seed}:${harness.name}:dcb-noise`
  for (let from = 0; from < options.profile.dcbNoiseEvents; from += 500) {
    const to = Math.min(options.profile.dcbNoiseEvents, from + 500)
    await harness.store.append(Array.from({ length: to - from }, (_, offset) => {
      const ordinal = from + offset
      return makeDcbEvent(`noise-${ordinal % 97}`, ordinal % DCB_KINDS, ordinal, noiseSeed)
    }))
  }

  const samples: ScenarioSample[] = []
  let checks = 0
  for (const workers of options.profile.dcbConcurrency) {
    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      await dcbPass(harness, options, workers, 3, `dcb-warmup-c${workers}-${warmup}`)
    }
    for (let sample = 0; sample < options.samples; sample++) {
      const result = await dcbPass(
        harness,
        options,
        workers,
        options.profile.dcbPerWorker,
        `dcb-${harness.name}-c${workers}-s${sample}`,
      )
      checks += result.checks
      const operations = workers * options.profile.dcbPerWorker
      samples.push({
        backend: harness.name,
        scenario: "dcb",
        parameters: { concurrency: workers, criteriaWidth: DCB_KINDS },
        sample,
        metrics: metrics({
          throughputName: "commandsPerSec",
          throughput: operations / (result.elapsedMs / 1_000),
          latencies: result.latencies,
        }),
        diagnostics: {
          elapsedMs: result.elapsedMs,
          operations,
          noiseEvents: options.profile.dcbNoiseEvents,
          criteriaWidth: DCB_KINDS,
        },
      })
    }
  }
  return { samples, correctnessChecks: checks }
}

async function runRehydration(harness: BackendHarness, options: BenchmarkOptions): Promise<ScenarioRun> {
  const samples: ScenarioSample[] = []
  let checks = 0
  for (const depth of options.profile.rehydrationDepths) {
    const aggregateId = `rehydration-${harness.name}-${depth}`
    const seed = `${options.seed}:${harness.name}:rehydration:${depth}`
    await seedEvents(harness.store, aggregateId, depth, seed)

    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      const sourced = await sourceAggregate(harness.store, aggregateId)
      reduceState(sourced.events)
    }

    for (let sample = 0; sample < options.samples; sample++) {
      const latencies: number[] = []
      let elapsedMs = 0
      for (let read = 0; read < options.profile.rehydrationReads; read++) {
        const before = performance.now()
        const sourced = await sourceAggregate(harness.store, aggregateId)
        const state = reduceState(sourced.events)
        const duration = performance.now() - before
        latencies.push(duration)
        elapsedMs += duration
        assert(state.count === depth, `${aggregateId}: reducer count mismatch`)
        checks++
        checks += verifyPayloads(sourced.events, depth, `${aggregateId} sample ${sample} read ${read}`)
      }
      const totalEvents = depth * options.profile.rehydrationReads
      samples.push({
        backend: harness.name,
        scenario: "rehydration",
        parameters: { depth },
        sample,
        metrics: metrics({
          throughputName: "eventsPerSec",
          throughput: totalEvents / (elapsedMs / 1_000),
          latencies,
        }),
        diagnostics: { reads: options.profile.rehydrationReads, totalEvents, elapsedMs },
      })
    }
  }
  return { samples, correctnessChecks: checks }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitFor(check: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = performance.now() + timeoutMs
  while (performance.now() < deadline) {
    if (check()) return
    await sleep(5)
  }
  throw new Error(`timed out waiting for ${label}`)
}

async function catchupPass(
  store: EventStore,
  expected: number,
  name: string,
  batchSize: number | undefined,
): Promise<{ elapsedMs: number; position: bigint; checks: number }> {
  const seen = new Set<number>()
  let deliveries = 0
  const handler = eventHandler(BenchEvent, async ({ payload }) => {
    deliveries++
    seen.add(payload.ordinal)
  })
  const processor = trackingEventProcessor({
    name,
    eventSource: store,
    eventHandlers: [handler],
    ...(batchSize === undefined ? {} : { batchSize }),
  })

  const before = performance.now()
  await processor.start()
  try {
    await waitFor(
      () => seen.size === expected && processor.status().caughtUp,
      180_000,
      `${name} catch-up`,
    )
    const elapsedMs = performance.now() - before
    assert(deliveries === expected, `${name}: expected ${expected} deliveries, got ${deliveries}`)
    assert(seen.size === expected, `${name}: missing or duplicate ordinals`)
    for (let ordinal = 0; ordinal < expected; ordinal++) {
      assert(seen.has(ordinal), `${name}: missing ordinal ${ordinal}`)
    }
    return { elapsedMs, position: processor.position, checks: expected + 2 }
  } finally {
    processor.stop()
    await sleep(50)
  }
}

async function runCatchup(
  harness: BackendHarness,
  options: BenchmarkOptions,
  batchSize: number | undefined,
): Promise<ScenarioRun> {
  const scenario: ScenarioName = batchSize === undefined ? "catchup-default" : "catchup-100"
  const aggregateId = `${scenario}-${harness.name}`
  const seed = `${options.seed}:${harness.name}:${scenario}`
  await seedEvents(harness.store, aggregateId, options.profile.catchupEvents, seed)
  const seeded = await sourceAggregate(harness.store, aggregateId)
  let checks = verifyPayloads(seeded.events, options.profile.catchupEvents, aggregateId)

  for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
    await catchupPass(harness.store, options.profile.catchupEvents, `${scenario}-${harness.name}-warmup-${warmup}`, batchSize)
  }

  const samples: ScenarioSample[] = []
  for (let sample = 0; sample < options.samples; sample++) {
    const result = await catchupPass(
      harness.store,
      options.profile.catchupEvents,
      `${scenario}-${harness.name}-sample-${sample}`,
      batchSize,
    )
    checks += result.checks
    samples.push({
      backend: harness.name,
      scenario,
      parameters: { batchSize: batchSize ?? 1, events: options.profile.catchupEvents },
      sample,
      metrics: [
        { name: "catchupEventsPerSec", value: options.profile.catchupEvents / (result.elapsedMs / 1_000), unit: "events/s", better: "higher" },
        { name: "totalMs", value: result.elapsedMs, unit: "ms", better: "lower" },
      ],
      diagnostics: { position: result.position.toString(), elapsedMs: result.elapsedMs },
    })
  }
  return { samples, correctnessChecks: checks }
}

interface PendingDelivery {
  readonly startedAt: number
  readonly resolve: (latencyMs: number) => void
}

async function runLive(harness: BackendHarness, options: BenchmarkOptions): Promise<ScenarioRun> {
  const pending = new Map<string, PendingDelivery>()
  const delivered = new Set<string>()
  const duplicates = new Set<string>()
  const handler = eventHandler(BenchEvent, async ({ identifier }) => {
    const entry = pending.get(identifier)
    if (!entry) {
      if (delivered.has(identifier)) duplicates.add(identifier)
      return
    }
    delivered.add(identifier)
    pending.delete(identifier)
    entry.resolve(performance.now() - entry.startedAt)
  })
  const processor = trackingEventProcessor({
    name: `live-${harness.name}`,
    eventSource: harness.store,
    eventHandlers: [handler],
    batchSize: 100,
  })
  await processor.start()
  await waitFor(() => processor.status().caughtUp, 30_000, `${harness.name} live processor startup`)

  function track(eventMessage: EventMessage): Promise<number> {
    return new Promise<number>((resolve) => {
      pending.set(eventMessage.identifier, { startedAt: performance.now(), resolve })
    })
  }

  async function serialPass(count: number, key: string): Promise<{ elapsedMs: number; latencies: number[] }> {
    const latencies: number[] = []
    const started = performance.now()
    for (let ordinal = 0; ordinal < count; ordinal++) {
      const eventMessage = makeEvent(key, ordinal, `${options.seed}:${harness.name}:${key}`)
      const handled = track(eventMessage)
      await harness.store.append([eventMessage])
      latencies.push(await handled)
    }
    return { elapsedMs: performance.now() - started, latencies }
  }

  async function throughputPass(count: number, concurrency: number, key: string): Promise<{
    producerMs: number
    elapsedMs: number
    latencies: number[]
  }> {
    let next = 0
    const handled: Promise<number>[] = []
    const started = performance.now()
    await Promise.all(Array.from({ length: concurrency }, async () => {
      while (true) {
        const ordinal = next++
        if (ordinal >= count) return
        const eventMessage = makeEvent(key, ordinal, `${options.seed}:${harness.name}:${key}`)
        handled.push(track(eventMessage))
        await harness.store.append([eventMessage])
      }
    }))
    const producerMs = performance.now() - started
    const latencies = await Promise.all(handled)
    return { producerMs, elapsedMs: performance.now() - started, latencies }
  }

  const samples: ScenarioSample[] = []
  let checks = 0
  try {
    for (let warmup = 0; warmup < options.profile.warmups; warmup++) {
      await serialPass(3, `live-warmup-${warmup}`)
    }
    for (let sample = 0; sample < options.samples; sample++) {
      const serialKey = `live-serial-${harness.name}-s${sample}`
      const beforeSerialDelivered = delivered.size
      const serial = await serialPass(options.profile.liveSerialEvents, serialKey)
      assert(delivered.size - beforeSerialDelivered === options.profile.liveSerialEvents, `${serialKey}: delivery count mismatch`)
      checks += options.profile.liveSerialEvents + 1
      samples.push({
        backend: harness.name,
        scenario: "live",
        parameters: { mode: "serial", concurrency: 1 },
        sample,
        metrics: metrics({
          throughputName: "handledEventsPerSec",
          throughput: options.profile.liveSerialEvents / (serial.elapsedMs / 1_000),
          latencies: serial.latencies,
        }),
        diagnostics: { elapsedMs: serial.elapsedMs, events: options.profile.liveSerialEvents },
      })

      const throughputKey = `live-throughput-${harness.name}-s${sample}`
      const beforeThroughputDelivered = delivered.size
      const throughput = await throughputPass(
        options.profile.liveThroughputEvents,
        options.profile.liveConcurrency,
        throughputKey,
      )
      assert(delivered.size - beforeThroughputDelivered === options.profile.liveThroughputEvents, `${throughputKey}: delivery count mismatch`)
      await waitFor(() => processor.status().caughtUp, 30_000, `${throughputKey} caught-up status`)
      checks += options.profile.liveThroughputEvents + 1
      samples.push({
        backend: harness.name,
        scenario: "live",
        parameters: { mode: "throughput", concurrency: options.profile.liveConcurrency },
        sample,
        metrics: [
          ...metrics({
            throughputName: "handledEventsPerSec",
            throughput: options.profile.liveThroughputEvents / (throughput.elapsedMs / 1_000),
            latencies: throughput.latencies,
          }),
          { name: "appendEventsPerSec", value: options.profile.liveThroughputEvents / (throughput.producerMs / 1_000), unit: "events/s", better: "higher" },
        ],
        diagnostics: {
          elapsedMs: throughput.elapsedMs,
          producerMs: throughput.producerMs,
          events: options.profile.liveThroughputEvents,
        },
      })
    }
    assert(pending.size === 0, `${harness.name} live processor has ${pending.size} unresolved deliveries`)
    assert(duplicates.size === 0, `${harness.name} live processor delivered duplicates: ${[...duplicates].slice(0, 3).join(", ")}`)
    checks += 2
    return { samples, correctnessChecks: checks }
  } finally {
    processor.stop()
    await sleep(50)
  }
}

export async function runScenario(
  scenario: ScenarioName,
  harness: BackendHarness,
  options: BenchmarkOptions,
): Promise<ScenarioRun> {
  switch (scenario) {
    case "append": return runAppend(harness, options)
    case "append-c": return runAppendConcurrency(harness, options)
    case "workflow": return runWorkflow(harness, options)
    case "dcb": return runDcb(harness, options)
    case "rehydration": return runRehydration(harness, options)
    case "catchup-default": return runCatchup(harness, options, undefined)
    case "catchup-100": return runCatchup(harness, options, 100)
    case "live": return runLive(harness, options)
  }
}
