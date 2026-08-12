import type { EventSink } from "./event-sink.js"
import type { EventMessage } from "./message.js"
import type { EventDescriptor } from "./descriptor.js"
import { generateIdentifier } from "@kronos-ts/common"
import type { z } from "zod"

/**
 * User-facing gateway for publishing events directly (without going through
 * command handlers).
 *
 */
export interface EventGateway {
  /**
   * Publish a single event described by its descriptor.
   */
  publish<P extends z.ZodType>(
    descriptor: EventDescriptor<P>,
    payload: z.infer<P>,
    metadata?: Record<string, unknown>,
  ): Promise<void>
}

/**
 * Creates an event gateway backed by an event sink.
 */
export function eventGateway(eventSink: EventSink): EventGateway {
  return {
    async publish(descriptor, payload, metadata = {}) {
      const tags = descriptor.tags ? descriptor.tags(payload) : []
      const event: EventMessage = {
        kind: "event",
        identifier: generateIdentifier(),
        name: descriptor.name,
        version: descriptor.version,
        payload,
        metadata,
        timestamp: Date.now(),
        tags,
      }
      await eventSink.publish([event])
    },
  }
}
