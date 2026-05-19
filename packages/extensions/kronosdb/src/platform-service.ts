import type { KronosDbConnection } from "./connection.js"
import { createKronosMetadata } from "./connection.js"
import { createOutboundStream } from "./outbound-stream.js"
import type { PlatformInbound } from "./generated/platform.js"
import type { ProcessorStatusSupplier } from "./event-processor-info.js"
import { toEventProcessorInfo } from "./event-processor-info.js"

/**
 * Server-initiated instructions received via the platform stream.
 *
 * KronosDB uses direct fields on PlatformOutbound (not nested like Axon Server's
 * eventProcessorControl/topologyChange wrappers).
 */
export type PlatformInstruction =
  | { kind: "pause-processor"; processorName: string }
  | { kind: "start-processor"; processorName: string }
  | { kind: "release-segment"; processorName: string; segmentId: number }
  | { kind: "split-segment"; processorName: string; segmentId: number }
  | { kind: "merge-segment"; processorName: string; segmentId: number }
  | { kind: "reconnect-request" }

export type InstructionHandler = (instruction: PlatformInstruction) => void | Promise<void>

export interface PlatformConnection {
  start(): Promise<void>
  stop(): void
  onInstruction(handler: InstructionHandler): void
  registerProcessorStatusSupplier(supplier: ProcessorStatusSupplier): void
  readonly connected: boolean
  /**
   * Resolves with `true` once KronosDB has acknowledged this client's
   * registration on the platform stream (we use the first server-originated
   * inbound message — typically a heartbeat reply — as the ack signal).
   * Resolves with `false` if the platform is not yet started.
   *
   * This replaces the legacy 1-second sleep at
   * kronosdb-configuration-enhancer.ts:216 (D-102): the kronosDb extension's
   * `onStart('processors', ...)` hook polls this method via `withRetry` so the
   * application waits exactly long enough for handler subscriptions to be
   * routable, no longer or shorter.
   *
   * Implementation note: the platform stream sends a `register` frame on
   * `start()` and KronosDB responds with a heartbeat tick at the configured
   * heartbeat interval. We treat the first inbound frame as the ack signal,
   * which is the earliest observable point at which the server has accepted
   * the registration. (No explicit ack frame exists in the KronosDB protobuf
   * surface today — Pitfall 5 / RESEARCH.md.)
   */
  subscriptionsAcked(): Promise<boolean>
}

export interface PlatformServiceOptions {
  /** Heartbeat interval in ms. Default: 10000 */
  heartbeatIntervalMs?: number
  /** Heartbeat timeout in ms. Default: 7500 */
  heartbeatTimeoutMs?: number
  /** Processor status reporting interval in ms. Default: 500 */
  processorsNotificationRateMs?: number
  /** Delay before first processor status report in ms. Default: 5000 */
  processorsNotificationInitialDelayMs?: number
}

/**
 * Parses a raw PlatformOutbound message into a typed PlatformInstruction.
 *
 * Returns `null` for any arm that does not correspond to a known instruction
 * (e.g. heartbeat, nodeNotification, topologyNotification). This is the
 * correct catch-all for forward-compatibility: new proto fields added to
 * PlatformOutbound will silently be ignored rather than causing errors.
 */
export function parseInstruction(message: any): PlatformInstruction | null {
  if (message.requestReconnect) {
    return { kind: "reconnect-request" }
  }

  // KronosDB sends processor instructions directly on PlatformOutbound
  if (message.pauseEventProcessor) {
    return {
      kind: "pause-processor",
      processorName: message.pauseEventProcessor.processorName ?? "",
    }
  }
  if (message.startEventProcessor) {
    return {
      kind: "start-processor",
      processorName: message.startEventProcessor.processorName ?? "",
    }
  }
  if (message.releaseSegment) {
    return {
      kind: "release-segment",
      processorName: message.releaseSegment.processorName ?? "",
      segmentId: message.releaseSegment.segmentIdentifier ?? 0,
    }
  }
  if (message.splitEventProcessorSegment) {
    return {
      kind: "split-segment",
      processorName: message.splitEventProcessorSegment.processorName ?? "",
      segmentId: message.splitEventProcessorSegment.segmentIdentifier ?? 0,
    }
  }
  if (message.mergeEventProcessorSegment) {
    return {
      kind: "merge-segment",
      processorName: message.mergeEventProcessorSegment.processorName ?? "",
      segmentId: message.mergeEventProcessorSegment.segmentIdentifier ?? 0,
    }
  }

  return null
}

/**
 * Creates a PlatformService connection to KronosDB.
 *
 * The platform stream is the control plane:
 * 1. Registers this client with KronosDB
 * 2. Sends periodic heartbeats to verify connectivity
 * 3. Receives instructions from KronosDB (split, merge, pause, resume)
 * 4. Reports event processor status periodically
 */
export function createPlatformConnection(
  connection: KronosDbConnection,
  options?: PlatformServiceOptions,
): PlatformConnection {
  const heartbeatIntervalMs = options?.heartbeatIntervalMs ?? 10000
  const heartbeatTimeoutMs = options?.heartbeatTimeoutMs ?? 7500
  const processorsNotificationRateMs = options?.processorsNotificationRateMs ?? 500
  const processorsNotificationInitialDelayMs = options?.processorsNotificationInitialDelayMs ?? 5000

  const instructionHandlers: InstructionHandler[] = []
  const processorStatusSuppliers: ProcessorStatusSupplier[] = []
  let isConnected = false
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null
  let processorStatusTimer: ReturnType<typeof setInterval> | null = null
  let lastHeartbeatResponse = Date.now()
  let outbound: ReturnType<typeof createOutboundStream<PlatformInbound>> | null = null
  /**
   * Latches once KronosDB sends its first inbound message after registration
   * — the earliest observable signal that the platform stream is fully wired
   * (D-102, replaces the legacy 1s sleep). Reset on every `start()` so a
   * stop/start cycle re-arms the latch correctly.
   */
  let acked = false

  const grpcMetadata = createKronosMetadata(connection.config)

  async function processInboundInstructions(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        // First inbound message after start() = the platform has accepted our
        // registration and is talking back. Latch the ack flag (D-102).
        acked = true
        const instruction = parseInstruction(message)
        if (instruction) {
          for (const handler of instructionHandlers) {
            try {
              await handler(instruction)
            } catch (err) {
              console.error("Platform instruction handler error:", err)
            }
          }
        }

        if (message.heartbeat) {
          lastHeartbeatResponse = Date.now()
        }
      }
    } catch (err) {
      if (isConnected) {
        console.error("Platform stream error:", err)
        isConnected = false
      }
    }
  }

  function startHeartbeat() {
    if (heartbeatTimer) clearInterval(heartbeatTimer)
    lastHeartbeatResponse = Date.now()

    heartbeatTimer = setInterval(() => {
      if (!isConnected || !outbound) return

      const timeSinceLastResponse = Date.now() - lastHeartbeatResponse
      if (timeSinceLastResponse > heartbeatTimeoutMs) {
        console.warn(
          `Platform heartbeat timeout: no response in ${timeSinceLastResponse}ms ` +
          `(threshold: ${heartbeatTimeoutMs}ms). Marking connection as lost.`,
        )
        isConnected = false
        connection.reconnect().catch((err) => {
          console.error("Failed to reconnect after heartbeat timeout:", err)
        })
        return
      }

      // Send heartbeat — KronosDB PlatformInbound uses oneof, heartbeat field
      outbound.send({
        heartbeat: {},
      })
    }, heartbeatIntervalMs)
  }

  function startProcessorStatusReporting() {
    if (processorStatusTimer) clearInterval(processorStatusTimer)

    setTimeout(() => {
      if (!isConnected) return
      reportProcessorStatus()

      processorStatusTimer = setInterval(() => {
        if (!isConnected || !outbound) return
        reportProcessorStatus()
      }, processorsNotificationRateMs)
    }, processorsNotificationInitialDelayMs)
  }

  function reportProcessorStatus() {
    if (!outbound || processorStatusSuppliers.length === 0) return

    for (const supplier of processorStatusSuppliers) {
      try {
        const statuses = supplier()
        for (const status of statuses) {
          outbound.send({
            eventProcessorInfo: toEventProcessorInfo(status),
          })
        }
      } catch (err) {
        console.warn("Failed to report processor status:", err)
      }
    }
  }

  return {
    async start() {
      if (isConnected) return

      // Re-arm the ack latch so a stop/start cycle correctly re-waits.
      acked = false
      outbound = createOutboundStream<PlatformInbound>()

      // Register with KronosDB — first message must be ClientIdentification
      outbound.send({
        register: {
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
          version: "1.0.0",
          tags: {},
        },
      })

      // Open bidirectional platform stream
      const inbound = connection.platform.openStream(outbound.iterable, {
        metadata: grpcMetadata,
      })

      isConnected = true
      startHeartbeat()
      startProcessorStatusReporting()
      processInboundInstructions(inbound)
    },

    stop() {
      isConnected = false
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer)
        heartbeatTimer = null
      }
      if (processorStatusTimer) {
        clearInterval(processorStatusTimer)
        processorStatusTimer = null
      }
      if (outbound) {
        outbound.close()
        outbound = null
      }
    },

    onInstruction(handler) {
      instructionHandlers.push(handler)
    },

    registerProcessorStatusSupplier(supplier) {
      processorStatusSuppliers.push(supplier)
    },

    get connected() {
      return isConnected
    },

    async subscriptionsAcked(): Promise<boolean> {
      return isConnected && acked
    },
  }
}
