/**
 * Message size validation for Axon Server gRPC communication.
 *
 * Axon Server has a maximum inbound message size (default: 4MB).
 * This module pre-checks message sizes before sending and warns
 * when approaching the limit.
 */

/** Default max message size in bytes (4MB — Axon Server default). */
const DEFAULT_MAX_MESSAGE_SIZE = 4 * 1024 * 1024

/** Warning threshold as a fraction of max size. */
const WARNING_THRESHOLD = 0.75

export interface MessageSizeConfig {
  /** Maximum message size in bytes. Default: 4MB */
  maxMessageSize?: number
  /** Warning threshold as a fraction (0-1). Default: 0.75 */
  warningThreshold?: number
}

export class MessageSizeExceededError extends Error {
  readonly actualSize: number
  readonly maxSize: number

  constructor(actualSize: number, maxSize: number) {
    super(
      `Message size ${actualSize} bytes exceeds maximum ${maxSize} bytes`,
    )
    this.name = "MessageSizeExceededError"
    this.actualSize = actualSize
    this.maxSize = maxSize
  }
}

/**
 * Creates a message size validator.
 *
 * - `validate(data)` — throws if over limit, warns if over 75%
 * - `estimateSize(payload)` — quick byte size estimate
 */
export function createMessageSizeValidator(config?: MessageSizeConfig) {
  const maxSize = config?.maxMessageSize ?? DEFAULT_MAX_MESSAGE_SIZE
  const threshold = config?.warningThreshold ?? WARNING_THRESHOLD
  const warningSize = Math.floor(maxSize * threshold)

  return {
    /**
     * Validate a serialized message size.
     * Throws if over the limit, logs a warning if over the threshold.
     */
    validate(data: Uint8Array, context?: string): void {
      const size = data.byteLength
      if (size > maxSize) {
        throw new MessageSizeExceededError(size, maxSize)
      }
      if (size > warningSize) {
        console.warn(
          `Message size warning${context ? ` (${context})` : ""}: ` +
          `${size} bytes is ${Math.round((size / maxSize) * 100)}% of max ${maxSize} bytes`,
        )
      }
    },

    /**
     * Estimate the serialized size of a payload (quick JSON length check).
     */
    estimateSize(payload: unknown): number {
      return new TextEncoder().encode(JSON.stringify(payload)).byteLength
    },

    /** The configured maximum size in bytes. */
    maxSize,
  }
}
