import type { CommandMessage } from "./message.js"

/**
 * Determines the routing key for a command message.
 *
 * Used by the distributed command bus to route commands to the correct
 * handler instance via consistent hashing. Commands with the same routing
 * key are routed to the same handler.
 *
 * Aligned with Kronos Framework's `RoutingStrategy`.
 */
export interface RoutingStrategy {
  /**
   * Get the routing key for the given command message.
   * Returns a string that identifies the target for this command
   * (typically an aggregate identifier).
   */
  getRoutingKey(message: CommandMessage): string
}

/**
 * Extracts the routing key from a command message's metadata.
 *
 * Aligned with Kronos Framework's `MetadataRoutingStrategy`.
 *
 * @param metadataKey The metadata key to extract the routing key from.
 */
export function metadataRoutingStrategy(metadataKey: string): RoutingStrategy {
  return {
    getRoutingKey(message: CommandMessage): string {
      const value = message.metadata[metadataKey]
      if (value == null) {
        throw new Error(
          `No routing key found in metadata key "${metadataKey}" ` +
          `for command "${String(message.name)}"`,
        )
      }
      return String(value)
    },
  }
}

/**
 * Extracts the routing key from a field of the command payload.
 *
 * This is the TypeScript equivalent of Java's `AnnotationRoutingStrategy`,
 * adapted for function-based descriptors. Instead of annotations on the
 * payload class, the field name is specified explicitly.
 *
 * @param field The payload field to extract the routing key from.
 */
export function payloadFieldRoutingStrategy(field: string): RoutingStrategy {
  return {
    getRoutingKey(message: CommandMessage): string {
      const payload = message.payload as Record<string, unknown>
      const value = payload?.[field]
      if (value == null) {
        throw new Error(
          `No routing key found in payload field "${field}" ` +
          `for command "${String(message.name)}"`,
        )
      }
      return String(value)
    },
  }
}
