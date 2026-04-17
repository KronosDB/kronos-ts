import type { EventHandlersDefinition } from "./event-handler.js"
import type { TokenStore } from "./token-store.js"
import type { UnitOfWorkFactory } from "./unit-of-work.js"
import type { EventProcessingErrorHandler } from "./tracking-event-processor.js"
import type { SequencedDeadLetterQueue } from "./dead-letter-queue.js"

/**
 * Base configuration shared by all event processor types.
 */
interface EventProcessorBase {
  readonly name: string
  readonly handlerGroups: ReadonlyArray<EventHandlersDefinition>
}

/**
 * Configuration for a tracking event processor (polling-based, with token store).
 */
export interface TrackingProcessorModule extends EventProcessorBase {
  readonly kind: "tracking"
  readonly batchSize?: number
  readonly pollingIntervalMs?: number
  readonly tokenStore?: TokenStore
  readonly unitOfWorkFactory?: UnitOfWorkFactory
  readonly errorHandler?: EventProcessingErrorHandler
  readonly deadLetterQueue?: SequencedDeadLetterQueue
  readonly initialSegmentCount?: number
  readonly claimExtensionThresholdMs?: number
  readonly tokenClaimIntervalMs?: number
}

/**
 * Configuration for a subscribing event processor (push-based, no tracking).
 */
export interface SubscribingProcessorModule extends EventProcessorBase {
  readonly kind: "subscribing"
  readonly unitOfWorkFactory?: UnitOfWorkFactory
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
 * trackingProcessor("course-projection")
 *   .registerEventHandler(courseProjection)
 *   .batchSize(50)
 * ```
 *
 * Aligned with AF5's pooled streaming processor configuration.
 */
export function trackingProcessor(name: string): TrackingProcessorBuilder {
  return new TrackingProcessorBuilder(name)
}

export class TrackingProcessorBuilder {
  private readonly _name: string
  private readonly _handlers: EventHandlersDefinition[] = []
  private _batchSize?: number
  private _pollingIntervalMs?: number
  private _tokenStore?: TokenStore
  private _unitOfWorkFactory?: UnitOfWorkFactory
  private _errorHandler?: EventProcessingErrorHandler
  private _deadLetterQueue?: SequencedDeadLetterQueue
  private _initialSegmentCount?: number
  private _claimExtensionThresholdMs?: number
  private _tokenClaimIntervalMs?: number

  constructor(name: string) {
    this._name = name
  }

  /** Add an event handler group to this processor. */
  registerEventHandler(handlers: EventHandlersDefinition): this {
    this._handlers.push(handlers)
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

  /** Override the UnitOfWork factory for this processor. */
  unitOfWorkFactory(factory: UnitOfWorkFactory): this {
    this._unitOfWorkFactory = factory
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

  /** Number of segments to create on first startup. Default: 1. */
  initialSegmentCount(count: number): this {
    this._initialSegmentCount = count
    return this
  }

  /** @internal Build the processor configuration. */
  build(): TrackingProcessorModule {
    return {
      kind: "tracking",
      name: this._name,
      handlerGroups: this._handlers,
      batchSize: this._batchSize,
      pollingIntervalMs: this._pollingIntervalMs,
      tokenStore: this._tokenStore,
      unitOfWorkFactory: this._unitOfWorkFactory,
      errorHandler: this._errorHandler,
      deadLetterQueue: this._deadLetterQueue,
      initialSegmentCount: this._initialSegmentCount,
      claimExtensionThresholdMs: this._claimExtensionThresholdMs,
      tokenClaimIntervalMs: this._tokenClaimIntervalMs,
    }
  }
}

// ---------------------------------------------------------------------------
// Subscribing processor builder
// ---------------------------------------------------------------------------

/**
 * Builder for a subscribing event processor.
 *
 * Subscribing processors receive events pushed from the event store
 * as they are appended. No token store, no position tracking, no replay.
 *
 * ```typescript
 * subscribingProcessor("notifications")
 *   .registerEventHandler(notificationHandlers)
 * ```
 *
 * Aligned with AF5's subscribing processor configuration.
 */
export function subscribingProcessor(name: string): SubscribingProcessorBuilder {
  return new SubscribingProcessorBuilder(name)
}

export class SubscribingProcessorBuilder {
  private readonly _name: string
  private readonly _handlers: EventHandlersDefinition[] = []
  private _unitOfWorkFactory?: UnitOfWorkFactory
  private _errorHandler?: EventProcessingErrorHandler

  constructor(name: string) {
    this._name = name
  }

  /** Add an event handler group to this processor. */
  registerEventHandler(handlers: EventHandlersDefinition): this {
    this._handlers.push(handlers)
    return this
  }

  /** Override the UnitOfWork factory for this processor. */
  unitOfWorkFactory(factory: UnitOfWorkFactory): this {
    this._unitOfWorkFactory = factory
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
      handlerGroups: this._handlers,
      unitOfWorkFactory: this._unitOfWorkFactory,
      errorHandler: this._errorHandler,
    }
  }
}
