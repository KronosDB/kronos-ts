import type { Message } from "./message.js"

/**
 * Callback returned by a MessageMonitor when a message is ingested.
 * Call `reportSuccess()` or `reportFailure()` when processing completes.
 */
export interface MonitorCallback {
  /** Report that the message was processed successfully. */
  reportSuccess(): void
  /** Report that processing failed. */
  reportFailure(error?: Error): void
}

/**
 * Monitors message processing — observes ingestion, success, and failure
 * of messages flowing through the framework.
 *
 * Used by tracing, metrics, and custom observability extensions.
 * Monitors are registered via the kronos() App messageMonitorRegistry slot
 * (Phase 8 reshape — was previously the MessagingConfigurer surface).
 *
 * Aligned with AF5's `MessageMonitor`.
 */
export interface MessageMonitor<M extends Message = Message> {
  /**
   * Called when a message enters processing.
   * Returns a callback to report the outcome.
   */
  onMessageIngested(message: M): MonitorCallback
}

/**
 * A no-op monitor that does nothing. Default when no monitors are registered.
 */
export function noOpMessageMonitor<M extends Message = Message>(): MessageMonitor<M> {
  const noOpCallback: MonitorCallback = {
    reportSuccess() {},
    reportFailure() {},
  }
  return {
    onMessageIngested(): MonitorCallback {
      return noOpCallback
    },
  }
}

/**
 * Combines multiple monitors into one. Each monitor is called for every message.
 */
export function multiMessageMonitor<M extends Message = Message>(
  monitors: ReadonlyArray<MessageMonitor<M>>,
): MessageMonitor<M> {
  if (monitors.length === 0) return noOpMessageMonitor()
  if (monitors.length === 1) return monitors[0]!

  return {
    onMessageIngested(message: M): MonitorCallback {
      const callbacks = monitors.map(m => m.onMessageIngested(message))
      return {
        reportSuccess() {
          for (const cb of callbacks) cb.reportSuccess()
        },
        reportFailure(error?: Error) {
          for (const cb of callbacks) cb.reportFailure(error)
        },
      }
    },
  }
}
