import type { AxonServerConnection } from "./connection.js"
import { createOutboundStream } from "./outbound-stream.js"
import { Metadata } from "nice-grpc"
import type { ProcessorStatusSupplier } from "./event-processor-info.js"
import { toEventProcessorInfo } from "./event-processor-info.js"

/**
 * Server-initiated instructions received via the platform stream.
 * Axon Server sends these to control processor behavior.
 */
export type PlatformInstruction =
  | { kind: "pause-processor"; processorName: string }
  | { kind: "start-processor"; processorName: string }
  | { kind: "release-segment"; processorName: string; segmentId: number }
  | { kind: "split-segment"; processorName: string; segmentId: number }
  | { kind: "merge-segment"; processorName: string; segmentId: number }
  | { kind: "command-handler-added"; componentName: string; commandName: string }
  | { kind: "command-handler-removed"; componentName: string; commandName: string }
  | { kind: "query-handler-added"; componentName: string; queryName: string }
  | { kind: "query-handler-removed"; componentName: string; queryName: string }
  | { kind: "reconnect-request" }

/**
 * Callback for handling platform instructions from Axon Server.
 */
export type InstructionHandler = (instruction: PlatformInstruction) => void | Promise<void>

/**
 * Manages the PlatformService connection to Axon Server.
 *
 * Responsibilities:
 * - Node discovery via `getPlatformServer()`
 * - Bidirectional stream for topology management (`openStream()`)
 * - Heartbeat protocol to detect dead connections
 * - Receives server-initiated instructions (pause, resume, split, merge segments)
 */
export interface PlatformConnection {
  /** Start the platform stream (register with Axon Server, begin heartbeats). */
  start(): Promise<void>
  /** Stop the platform stream and heartbeats. */
  stop(): void
  /** Register a handler for server-initiated instructions. */
  onInstruction(handler: InstructionHandler): void
  /**
   * Register a supplier that provides event processor status.
   * Status is reported to Axon Server periodically.
   */
  registerProcessorStatusSupplier(supplier: ProcessorStatusSupplier): void
  /** Whether the platform stream is active. */
  readonly connected: boolean
}

export interface PlatformServiceOptions {
  /** Heartbeat interval in ms. Default: 10000 */
  heartbeatIntervalMs?: number
  /** Heartbeat timeout in ms. If no response within this window, reconnect. Default: 7500 */
  heartbeatTimeoutMs?: number
  /**
   * Interval for reporting event processor status to Axon Server in ms.
   * Aligned with Java's `processorsNotificationRate`. Default: 500.
   */
  processorsNotificationRateMs?: number
  /**
   * Delay before first processor status report in ms.
   * Aligned with Java's `processorsNotificationInitialDelay`. Default: 5000.
   */
  processorsNotificationInitialDelayMs?: number
}

/**
 * Creates a PlatformService connection to Axon Server.
 *
 * The platform stream is the control plane — it:
 * 1. Registers this client with Axon Server
 * 2. Sends periodic heartbeats to verify connectivity
 * 3. Receives instructions from Axon Server (split, merge, pause, resume)
 */
export function createPlatformConnection(
  connection: AxonServerConnection,
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
  let heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null
  let processorStatusTimer: ReturnType<typeof setInterval> | null = null
  let lastHeartbeatResponse = Date.now()
  let outbound: ReturnType<typeof createOutboundStream> | null = null

  const grpcMetadata = new Metadata()
  grpcMetadata.set("AxonIQ-Context", connection.config.context)
  if (connection.config.token) {
    grpcMetadata.set("AxonIQ-Access-Token", connection.config.token)
  }

  async function processInboundInstructions(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        // Parse instruction type
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

        // Handle heartbeat response — track last response time for timeout detection
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

  function parseInstruction(message: any): PlatformInstruction | null {
    if (message.requestReconnect) {
      return { kind: "reconnect-request" }
    }

    // Processor control instructions
    const ctrl = message.eventProcessorControl
    if (ctrl) {
      const processorName = ctrl.processorName ?? ""
      if (ctrl.pauseEventProcessor) {
        return { kind: "pause-processor", processorName }
      }
      if (ctrl.startEventProcessor) {
        return { kind: "start-processor", processorName }
      }
      if (ctrl.releaseSegment !== undefined) {
        return { kind: "release-segment", processorName, segmentId: ctrl.releaseSegment.segmentId ?? 0 }
      }
      if (ctrl.splitEventProcessor !== undefined) {
        return { kind: "split-segment", processorName, segmentId: ctrl.splitEventProcessor.segmentId ?? 0 }
      }
      if (ctrl.mergeEventProcessor !== undefined) {
        return { kind: "merge-segment", processorName, segmentId: ctrl.mergeEventProcessor.segmentId ?? 0 }
      }
    }

    // Topology change instructions
    const topo = message.topologyChange
    if (topo) {
      const componentName = topo.componentName ?? ""
      if (topo.commandHandlerAdded) {
        return { kind: "command-handler-added", componentName, commandName: topo.commandHandlerAdded.commandName ?? "" }
      }
      if (topo.commandHandlerRemoved) {
        return { kind: "command-handler-removed", componentName, commandName: topo.commandHandlerRemoved.commandName ?? "" }
      }
      if (topo.queryHandlerAdded) {
        return { kind: "query-handler-added", componentName, queryName: topo.queryHandlerAdded.queryName ?? "" }
      }
      if (topo.queryHandlerRemoved) {
        return { kind: "query-handler-removed", componentName, queryName: topo.queryHandlerRemoved.queryName ?? "" }
      }
    }

    return null
  }

  function startHeartbeat() {
    if (heartbeatTimer) clearInterval(heartbeatTimer)
    lastHeartbeatResponse = Date.now()

    heartbeatTimer = setInterval(() => {
      if (!isConnected || !outbound) return

      // Check if last heartbeat response was too long ago
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

      outbound.send({
        heartbeat: {
          clientId: connection.config.clientId,
        },
        instructionId: "",
      })
    }, heartbeatIntervalMs)
  }

  function startProcessorStatusReporting() {
    if (processorStatusTimer) clearInterval(processorStatusTimer)

    // Initial delay before first report
    setTimeout(() => {
      if (!isConnected) return
      reportProcessorStatus()

      // Then report at the configured rate
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
            instructionId: "",
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

      outbound = createOutboundStream()

      // Register with Axon Server
      outbound.send({
        register: {
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
          version: "1.0.0",
          tags: {},
        },
        instructionId: "",
      })

      // Open bidirectional stream
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
      if (heartbeatTimeoutTimer) {
        clearTimeout(heartbeatTimeoutTimer)
        heartbeatTimeoutTimer = null
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
  }
}
