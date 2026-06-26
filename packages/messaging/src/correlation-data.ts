import { type Metadata, type ResourceKey, resourceKey, mergeMetadata } from "@kronos-ts/common"
import type { Message } from "./message.js"
import type { DispatchInterceptor, HandlerInterceptor } from "./interceptor.js"
import { processingStateStorage, setResource } from "./processing-state.js"

/**
 * Resource key for storing correlation data in a ProcessingContext.
 */
export const CORRELATION_DATA_KEY: ResourceKey<Record<string, string>> = resourceKey("correlationData")

/**
 * Provides correlation data to attach to outgoing messages based on the
 * incoming message being processed.
 *
 * @see messageOriginProvider
 * @see simpleCorrelationDataProvider
 */
export interface CorrelationDataProvider {
  /**
   * Extract correlation data from the given message.
   * Returns a map of key-value pairs to attach to outgoing messages.
   * Should not throw — exceptions are caught and logged by the framework.
   */
  correlationDataFor(message: Message): Record<string, string>
}

/**
 * Get the active correlation data from the active UnitOfWork (D-29 permissive read).
 *
 * Reads directly from the ALS state. Returns `undefined` when called outside
 * an active UnitOfWork (e.g. primary dispatch path, no handler running) —
 * callers MUST tolerate this no-UoW case.
 *
 * Plan 03-04 (CTX-04 / D-29): no-arg permissive ALS read; the explicit
 * ProcessingContext parameter is gone.
 */
export function getActiveCorrelationData(): Record<string, string> | undefined {
  const state = processingStateStorage.getStore()
  if (!state) return undefined
  return state.resources.get(CORRELATION_DATA_KEY.symbol) as
    | Record<string, string>
    | undefined
}

// ---------------------------------------------------------------------------
// Built-in providers
// ---------------------------------------------------------------------------

/**
 * Default correlation data provider that tracks message lineage.
 *
 * - `correlationId`: preserved from the incoming message's metadata, or falls
 *   back to the message's own identifier (starts a new correlation chain).
 * - `causationId`: always set to the incoming message's identifier (direct cause).
 *
 * @param correlationKey metadata key for correlation ID (default: "correlationId")
 * @param causationKey metadata key for causation ID (default: "causationId")
 */
export function messageOriginProvider(
  correlationKey = "correlationId",
  causationKey = "causationId",
): CorrelationDataProvider {
  return {
    correlationDataFor(message: Message): Record<string, string> {
      const existingCorrelation = message.metadata[correlationKey]
      return {
        [correlationKey]: existingCorrelation != null
          ? String(existingCorrelation)
          : message.identifier,
        [causationKey]: message.identifier,
      }
    },
  }
}

/**
 * Copies specific metadata keys from the incoming message to outgoing messages.
 *
 * Silently ignores missing keys.
 */
export function simpleCorrelationDataProvider(...metadataKeys: string[]): CorrelationDataProvider {
  return {
    correlationDataFor(message: Message): Record<string, string> {
      const result: Record<string, string> = {}
      for (const key of metadataKeys) {
        if (key in message.metadata && message.metadata[key] != null) {
          result[key] = String(message.metadata[key])
        }
      }
      return result
    },
  }
}

// ---------------------------------------------------------------------------
// Extract phase (shared by the handler interceptor and the event processors)
// ---------------------------------------------------------------------------

/**
 * Compute correlation data from the given providers for `message` and merge it
 * into the active UnitOfWork's correlation-data resource (`CORRELATION_DATA_KEY`).
 *
 * This is the reusable "extract" step. It is run:
 * - by {@link correlationDataHandlerInterceptor} for command/query handlers, and
 * - by the event processors per-event before invoking event handlers, so an
 *   automation's outgoing commands/events inherit the triggering event's
 *   lineage.
 *
 * Each provider is called with the message. Exceptions are caught and logged
 * (they don't break message processing). Results merge over any existing
 * correlation data, so values contributed earlier (e.g. via
 * {@link contributeCorrelationData}) are preserved unless a provider overrides
 * the same key.
 *
 * Must be called inside an active UnitOfWork.
 */
export function applyCorrelationData(
  message: Message,
  providers: ReadonlyArray<CorrelationDataProvider>,
): void {
  const correlationData: Record<string, string> = {}

  for (const provider of providers) {
    try {
      Object.assign(correlationData, provider.correlationDataFor(message))
    } catch (err) {
      console.warn(
        "Encountered exception creating correlation data from provider:",
        err,
      )
    }
  }

  const existing = getActiveCorrelationData() ?? {}
  setResource(CORRELATION_DATA_KEY, { ...existing, ...correlationData })
}

/**
 * Contribute additional correlation data to the active UnitOfWork, merged over
 * whatever is already present under `CORRELATION_DATA_KEY`. The merged set is
 * applied to every message dispatched/appended from this UnitOfWork by the
 * correlation-data dispatch interceptor and the event appender.
 *
 * Use this from a handler enhancer or handler to seed extra lineage keys that
 * the built-in providers don't cover — for example an OpenTelemetry
 * `traceparent` so the trace context rides along on outgoing messages. This is
 * the supported alternative to mutating the object returned by
 * {@link getActiveCorrelationData}.
 *
 * Throws `NoActiveUnitOfWork` when called outside an active UnitOfWork.
 */
export function contributeCorrelationData(partial: Record<string, string>): void {
  const existing = getActiveCorrelationData() ?? {}
  setResource(CORRELATION_DATA_KEY, { ...existing, ...partial })
}

// ---------------------------------------------------------------------------
// Interceptor factory
// ---------------------------------------------------------------------------

/**
 * Creates a handler interceptor that extracts correlation data from the
 * incoming message and stores it in the ProcessingContext.
 *
 * This is the "extract" phase of the dual-interceptor pattern, delegating to
 * {@link applyCorrelationData}. The correlation data is stored as a
 * ProcessingContext resource under `CORRELATION_DATA_KEY`, where the dispatch
 * interceptor reads it.
 */
export function correlationDataHandlerInterceptor(
  providers: ReadonlyArray<CorrelationDataProvider>,
): HandlerInterceptor {
  return (message, next) => {
    // CTX-01 / Plan 03-03: HandlerInterceptor no longer threads ProcessingContext;
    // resource writes go directly through the module-level ALS accessor.
    applyCorrelationData(message, providers)
    return next()
  }
}

/**
 * Creates a dispatch interceptor that reads correlation data from the
 * active ProcessingContext and merges it into the outgoing message's
 * metadata.
 *
 * This is the "apply" phase of the dual-interceptor pattern.
 *
 * When a ProcessingContext is available (nested dispatch from a handler),
 * the interceptor reads correlation data stored by the handler interceptor
 * and merges it into the outgoing message's metadata. For the primary
 * dispatch path (no context), this is a no-op — correlation data flows
 * through message metadata inheritance.
 */
export function correlationDataDispatchInterceptor<M extends Message>(): DispatchInterceptor<M> {
  return (message: M): M => {
    // D-24: single code path. Read directly from the ALS state — `getResource`
    // throws on no-UoW, but the dispatch interceptor MUST tolerate the no-UoW
    // primary-dispatch path and return the message unchanged.
    // CTX-01 / Plan 03-03: vestigial _context parameter removed.
    const state = processingStateStorage.getStore()
    if (!state) return message
    const correlationData = state.resources.get(CORRELATION_DATA_KEY.symbol) as
      | Record<string, string>
      | undefined
    if (!correlationData || Object.keys(correlationData).length === 0) return message
    return {
      ...message,
      metadata: mergeMetadata(message.metadata, correlationData),
    }
  }
}
