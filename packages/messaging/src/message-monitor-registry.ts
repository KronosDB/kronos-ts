import type { Message, CommandMessage, EventMessage, QueryMessage } from "./message.js"
import type { MessageMonitor } from "./message-monitor.js"
import { multiMessageMonitor, noOpMessageMonitor } from "./message-monitor.js"

/**
 * Registry for message monitors. Collects monitors per message type
 * and provides combined monitors for each type.
 *
 * Aligned with AF5's `MessageMonitorRegistry`.
 */
export interface MessageMonitorRegistry {
  /** Register a monitor for all message types. */
  registerMonitor(monitor: MessageMonitor<Message>): void
  /** Register a monitor for command messages. */
  registerCommandMonitor(monitor: MessageMonitor<CommandMessage>): void
  /** Register a monitor for event messages. */
  registerEventMonitor(monitor: MessageMonitor<EventMessage>): void
  /** Register a monitor for query messages. */
  registerQueryMonitor(monitor: MessageMonitor<QueryMessage>): void

  /** Get the combined monitor for command messages. */
  commandMonitor(): MessageMonitor<CommandMessage>
  /** Get the combined monitor for event messages. */
  eventMonitor(): MessageMonitor<EventMessage>
  /** Get the combined monitor for query messages. */
  queryMonitor(): MessageMonitor<QueryMessage>
}

/**
 * Creates a default message monitor registry.
 */
export function createMessageMonitorRegistry(): MessageMonitorRegistry {
  const genericMonitors: MessageMonitor<Message>[] = []
  const commandMonitors: MessageMonitor<CommandMessage>[] = []
  const eventMonitors: MessageMonitor<EventMessage>[] = []
  const queryMonitors: MessageMonitor<QueryMessage>[] = []

  return {
    registerMonitor(monitor) {
      genericMonitors.push(monitor)
    },
    registerCommandMonitor(monitor) {
      commandMonitors.push(monitor)
    },
    registerEventMonitor(monitor) {
      eventMonitors.push(monitor)
    },
    registerQueryMonitor(monitor) {
      queryMonitors.push(monitor)
    },

    commandMonitor() {
      const all = [...genericMonitors, ...commandMonitors] as MessageMonitor<CommandMessage>[]
      return all.length > 0 ? multiMessageMonitor(all) : noOpMessageMonitor()
    },
    eventMonitor() {
      const all = [...genericMonitors, ...eventMonitors] as MessageMonitor<EventMessage>[]
      return all.length > 0 ? multiMessageMonitor(all) : noOpMessageMonitor()
    },
    queryMonitor() {
      const all = [...genericMonitors, ...queryMonitors] as MessageMonitor<QueryMessage>[]
      return all.length > 0 ? multiMessageMonitor(all) : noOpMessageMonitor()
    },
  }
}
