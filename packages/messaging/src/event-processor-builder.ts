import type { EventHandlerDefinition } from "./event-handler.js"
import type { TokenStore } from "./token-store.js"
import type { UoWRunner } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import type { SequencedDeadLetterQueue, EnqueuePolicy } from "./dead-letter-queue.js"
import type { SequencingPolicy } from "./sequencing-policy.js"
import type { DeadLetterListener } from "./dead-letter-listener.js"

/**
 * Base configuration shared by all event processor types.
 *
 * Plan 11-02: option types carry a flat `eventHandlers: EventHandlerDefinition[]`
 * array. The processor (not the handler bundle) owns reset semantics — see
 * `TrackingProcessorModule.onReset`.
 */
interface EventProcessorBase {
  readonly name: string
  readonly eventHandlers: ReadonlyArray<EventHandlerDefinition>
}

/**
 * Configuration for a tracking event processor (polling-based, with token store).
 */
export interface TrackingProcessorModule extends EventProcessorBase {
  readonly kind: "tracking"
  readonly batchSize?: number
  readonly pollingIntervalMs?: number
  readonly tokenStore?: TokenStore
  readonly unitOfWorkRunner?: UoWRunner
  readonly errorHandler?: EventProcessingErrorHandler
  readonly deadLetterQueue?: SequencedDeadLetterQueue
  /** Decides whether a failed event is enqueued in the DLQ. Default: always enqueue. */
  readonly enqueuePolicy?: EnqueuePolicy
  /** Decides each event's ordered sequence for the DLQ. Default: first tag value. */
  readonly sequencingPolicy?: SequencingPolicy
  /** Observability hook for dead-letter lifecycle events. Default: no-op. */
  readonly deadLetterListener?: DeadLetterListener
  /** When true, resetTokens() also clears this processor's DLQ. Default: false. */
  readonly resetClearsDeadLetters?: boolean
  /** When set, automatically drains the DLQ on this interval (ms). Off by default. */
  readonly dlqRetryIntervalMs?: number
  /** Number of segments created on first startup. Default 16 (Axon Framework parity). Always set by builder.build(). */
  readonly initialSegmentCount: number
  readonly claimExtensionThresholdMs?: number
  readonly tokenClaimIntervalMs?: number
  /** Reset callback invoked when the processor is reset. Reset is processor-level (clear token + replay), and the callback that wipes view state belongs alongside it. */
  readonly onReset?: () => Promise<void> | void
}

/**
 * Configuration for a subscribing event processor (push-based, no tracking).
 *
 * Subscribing processors do not support reset — see `supportsReset()` on the
 * runtime instance — so there is no `onReset` field here.
 */
export interface SubscribingProcessorModule extends EventProcessorBase {
  readonly kind: "subscribing"
  readonly unitOfWorkRunner?: UoWRunner
  readonly errorHandler?: EventProcessingErrorHandler
}

export type EventProcessorModule = TrackingProcessorModule | SubscribingProcessorModule

// ---------------------------------------------------------------------------
// Tracking processor builder
// ---------------------------------------------------------------------------

/**
 * Builder for a tracking event processor.
 *
 * Tracking processors poll the event store for new events, maintain
 * position via a token store, and support replay/reset.
 *
 * ```typescript
 * const onCreated = eventHandler(CourseCreated, async ({ payload: e }) => { ... })
 * const onCapChanged = eventHandler(CourseCapacityChanged, async ({ payload: e }) => { ... })
 *
 * trackingProcessor("course-projection")
 *   .eventHandlers(onCreated, onCapChanged)
 *   .onReset(async () => courseViews.clear())
 *   .batchSize(50)
 *   .build()
 * ```
 *
 * NOTE: Pooled streaming processor support is deferred to a follow-up
 * research phase exploring how that model should fit Node/Bun runtime
 * semantics, where worker threads are not reservable the same way as JVM
 * threads.
 */
export function trackingProcessor(name: string): TrackingProcessorBuilder {
  return new TrackingProcessorBuilder(name)
}

export class TrackingProcessorBuilder {
  private readonly _name: string
  private readonly _eventHandlers: EventHandlerDefinition[] = []
  private _onReset?: () => Promise<void> | void
  private _batchSize?: number
  private _pollingIntervalMs?: number
  private _tokenStore?: TokenStore
  private _unitOfWorkRunner?: UoWRunner
  private _errorHandler?: EventProcessingErrorHandler
  private _deadLetterQueue?: SequencedDeadLetterQueue
  private _enqueuePolicy?: EnqueuePolicy
  private _sequencingPolicy?: SequencingPolicy
  private _deadLetterListener?: DeadLetterListener
  private _resetClearsDeadLetters?: boolean
  private _dlqRetryIntervalMs?: number
  private _initialSegmentCount?: number
  private _claimExtensionThresholdMs?: number
  private _tokenClaimIntervalMs?: number

  constructor(name: string) {
    this._name = name
  }

  /** Register one or more singular event handlers on this processor. */
  eventHandlers(...handlers: EventHandlerDefinition[]): this {
    this._eventHandlers.push(...handlers)
    return this
  }

  /** Register a callback fired when the processor is reset (clears view state for replay). */
  onReset(fn: () => Promise<void> | void): this {
    this._onReset = fn
    return this
  }

  /**
   * Events per batch/transaction (one UnitOfWork). Default: 1 (Axon parity).
   *
   * A batch shares one UnitOfWork, so all events in it share the per-UoW
   * `load()` cache, DCB read-set, and a single atomic commit. Keep the default
   * of 1 for processors that make per-entity decisions via `load()`
   * (automations) so each decision stays isolated. Raise it for read-model
   * projections that only apply idempotent view updates and want throughput.
   */
  batchSize(size: number): this {
    this._batchSize = size
    return this
  }

  /** Polling interval in ms. Default: 500. */
  pollingIntervalMs(ms: number): this {
    this._pollingIntervalMs = ms
    return this
  }

  /** Override the token store for this processor. */
  tokenStore(store: TokenStore): this {
    this._tokenStore = store
    return this
  }

  /**
   * Override the UnitOfWork runner for this processor. Compose with
   * `transactionalUnitOfWorkFactory(runInUoW, txManager)` to attach
   * transactional semantics.
   */
  unitOfWorkRunner(runner: UoWRunner): this {
    this._unitOfWorkRunner = runner
    return this
  }

  /** Override the error handler for this processor. */
  errorHandler(handler: EventProcessingErrorHandler): this {
    this._errorHandler = handler
    return this
  }

  /**
   * Set a dead letter queue for this processor. When set, handler failures are
   * parked in the queue and the processor advances past them (Option A) rather
   * than redelivering the failed batch indefinitely.
   */
  deadLetterQueue(queue: SequencedDeadLetterQueue): this {
    this._deadLetterQueue = queue
    return this
  }

  /** Policy deciding whether a failed event is enqueued in the DLQ. Default: always. */
  enqueuePolicy(policy: EnqueuePolicy): this {
    this._enqueuePolicy = policy
    return this
  }

  /** Policy deciding each event's ordered sequence for the DLQ. Default: first tag value. */
  sequencingPolicy(policy: SequencingPolicy): this {
    this._sequencingPolicy = policy
    return this
  }

  /** Observability hook for dead-letter lifecycle events. */
  deadLetterListener(listener: DeadLetterListener): this {
    this._deadLetterListener = listener
    return this
  }

  /** When true, resetTokens() also clears this processor's DLQ (Axon allowReset). */
  resetClearsDeadLetters(enabled = true): this {
    this._resetClearsDeadLetters = enabled
    return this
  }

  /** Automatically drain the DLQ on this interval (ms). Omit to disable scheduled retries. */
  dlqRetryInterval(ms: number): this {
    this._dlqRetryIntervalMs = ms
    return this
  }

  /** Number of segments to create on first startup. Default: 16 (Axon Framework parity). */
  initialSegmentCount(count: number): this {
    this._initialSegmentCount = count
    return this
  }

  /** @internal Build the processor configuration. */
  build(): TrackingProcessorModule {
    return {
      kind: "tracking",
      name: this._name,
      eventHandlers: this._eventHandlers,
      batchSize: this._batchSize,
      pollingIntervalMs: this._pollingIntervalMs,
      tokenStore: this._tokenStore,
      unitOfWorkRunner: this._unitOfWorkRunner,
      errorHandler: this._errorHandler,
      deadLetterQueue: this._deadLetterQueue,
      enqueuePolicy: this._enqueuePolicy,
      sequencingPolicy: this._sequencingPolicy,
      deadLetterListener: this._deadLetterListener,
      resetClearsDeadLetters: this._resetClearsDeadLetters,
      dlqRetryIntervalMs: this._dlqRetryIntervalMs,
      initialSegmentCount: this._initialSegmentCount ?? 16,
      claimExtensionThresholdMs: this._claimExtensionThresholdMs,
      tokenClaimIntervalMs: this._tokenClaimIntervalMs,
      onReset: this._onReset,
    }
  }
}

// ---------------------------------------------------------------------------
// Subscribing processor builder
// ---------------------------------------------------------------------------

/**
 * Builder for a subscribing event processor.
 *
 * Subscribing processors receive events pushed from the event source
 * as they are appended. No token store, no position tracking, no replay.
 *
 * ```typescript
 * const onNotification = eventHandler(NotificationRaised, async ({ payload: e }) => { ... })
 *
 * subscribingProcessor("notifications")
 *   .eventHandlers(onNotification)
 *   .build()
 * ```
 *
 * Subscribing processors do NOT support reset (`supportsReset() === false`),
 * so there is no `.onReset(fn)` builder method here — that lives on
 * `TrackingProcessorBuilder` only.
 */
export function subscribingProcessor(name: string): SubscribingProcessorBuilder {
  return new SubscribingProcessorBuilder(name)
}

export class SubscribingProcessorBuilder {
  private readonly _name: string
  private readonly _eventHandlers: EventHandlerDefinition[] = []
  private _unitOfWorkRunner?: UoWRunner
  private _errorHandler?: EventProcessingErrorHandler

  constructor(name: string) {
    this._name = name
  }

  /** Register one or more singular event handlers on this processor. */
  eventHandlers(...handlers: EventHandlerDefinition[]): this {
    this._eventHandlers.push(...handlers)
    return this
  }

  /**
   * Override the UnitOfWork runner for this processor. Compose with
   * `transactionalUnitOfWorkFactory(runInUoW, txManager)` to attach
   * transactional semantics.
   */
  unitOfWorkRunner(runner: UoWRunner): this {
    this._unitOfWorkRunner = runner
    return this
  }

  /** Override the error handler for this processor. */
  errorHandler(handler: EventProcessingErrorHandler): this {
    this._errorHandler = handler
    return this
  }

  /** @internal Build the processor configuration. */
  build(): SubscribingProcessorModule {
    return {
      kind: "subscribing",
      name: this._name,
      eventHandlers: this._eventHandlers,
      unitOfWorkRunner: this._unitOfWorkRunner,
      errorHandler: this._errorHandler,
    }
  }
}
