import type { EventHandlerDefinition } from "./event-handler.js"
import type { TokenStore } from "./token-store.js"
import type { UoWRunner } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import type { SequencedDeadLetterQueue } from "./dead-letter-queue.js"

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
 * const onCreated = eventHandler(CourseCreated, async (e) => { ... })
 * const onCapChanged = eventHandler(CourseCapacityChanged, async (e) => { ... })
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

  /** Events per batch/transaction. Default: 100. */
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

  /** Set a dead letter queue for this processor. */
  deadLetterQueue(queue: SequencedDeadLetterQueue): this {
    this._deadLetterQueue = queue
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
 * const onNotification = eventHandler(NotificationRaised, async (e) => { ... })
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
