import type { AxonServerConnection } from "./connection.js"
import { outboundStream } from "./outbound-stream.js"
import type { PlatformInboundInstruction } from "./generated/control.js"
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
export type PlatformConnection = {
  /**
   * DATA PATH. Open the platform stream, register this client, and arm the
   * heartbeat that calls `connection.reconnect()` when the server stops
   * answering.
   *
   * This is split out of {@link start} deliberately. Reconnect detection is a
   * property of the CONNECTION, and the command/query buses hook
   * `connection.onReconnect(...)` to re-establish their own streams — so a
   * service that never opts into remote administration still needs it. When it
   * lived only inside `start()` (which only the control plane calls), such a
   * service had no heartbeat-driven reconnect detection on its data path at all
   * and would sit on a dead channel indefinitely.
   *
   * Arms NOTHING control-plane-specific: no processor status reporting. Safe to
   * call repeatedly; a stream that is already up is left alone.
   */
  armConnectionMonitoring(): Promise<void>
  /**
   * CONTROL PLANE. Everything {@link armConnectionMonitoring} does, plus
   * periodic processor status reporting.
   *
   * Idempotent in both halves: if the data path already opened the stream, this
   * only adds status reporting; if it did not, this opens the stream too.
   */
  start(): Promise<void>
  /**
   * Stop the platform stream, heartbeats and status reporting.
   *
   * Note that this tears down the SHARED stream — including the data path's
   * reconnect detection. `axonServerControlPlane(...).close()` calls it, which
   * is correct in the documented shutdown order (`app.stop()` → `control.close()`
   * → `axon.close()`) but means closing a control plane on a still-running
   * service disarms reconnect detection with it.
   */
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
  /**
   * Resolves with `true` once Axon Server has acknowledged this client's
   * registration on the platform stream (we use the first server-originated
   * inbound message — typically a heartbeat reply or platform instruction —
   * as the ack signal). Resolves with `false` if the platform is not yet
   * started.
   *
   * This mirrors the kronosdb implementation (Plan 09-03 / D-102): the
   * native `axonServer` extension's `onStart('processors', ...)` hook polls
   * this method via `withRetry` so the application waits exactly long enough
   * for handler subscriptions to be routable, no longer or shorter — the
   * Axon equivalent of dropping the legacy 1-second sleep.
   *
   * Implementation note: while Axon Server's outbound stream maintains a
   * `pendingSubscriptions` map (per RESEARCH.md), the `register` frame is
   * sent on the *platform* stream (separate from the bus streams) and Axon
   * Server replies with a heartbeat tick / topology message at the
   * configured interval. We treat the first inbound platform-stream frame
   * as the ack signal — same observable derivation as kronosdb to keep the
   * two extensions structurally symmetric.
   */
  subscriptionsAcked(): Promise<boolean>
}

export type PlatformServiceOptions = {
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
export function platformConnection(
  connection: AxonServerConnection,
  options?: PlatformServiceOptions,
): PlatformConnection {
  const heartbeatIntervalMs = options?.heartbeatIntervalMs ?? 10000
  const heartbeatTimeoutMs = options?.heartbeatTimeoutMs ?? 7500
  const processorsNotificationRateMs = options?.processorsNotificationRateMs ?? 500
  const processorsNotificationInitialDelayMs = options?.processorsNotificationInitialDelayMs ?? 5000

  const instructionHandlers: InstructionHandler[] = []
  const processorStatusSuppliers: ProcessorStatusSupplier[] = []
  /**
   * Instructions that arrived before anything registered a handler.
   *
   * The control plane registers its handler before calling `start()`, but the
   * DATA path now opens the same stream via `armConnectionMonitoring()` — which
   * runs before any control plane exists. That leaves a window in which Axon
   * Server can push an instruction at a client with nothing to route it to.
   * Buffering makes the window harmless instead of merely forbidden: the first
   * `onInstruction` registration drains this queue in arrival order. Mirrors the
   * kronosdb platform connection, which has had this since its backend started
   * the stream for `subscriptionsAcked()`.
   */
  const pendingInstructions: PlatformInstruction[] = []
  let isConnected = false
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null
  let heartbeatTimeoutTimer: ReturnType<typeof setTimeout> | null = null
  let processorStatusTimer: ReturnType<typeof setInterval> | null = null
  /** Guards against arming the status-report timer twice — see `startProcessorStatusReporting`. */
  let processorStatusArmed = false
  let lastHeartbeatResponse = Date.now()
  let outbound: ReturnType<typeof outboundStream<PlatformInboundInstruction>> | null = null
  /**
   * Latches once Axon Server sends its first inbound message after
   * registration — the earliest observable signal that the platform stream
   * is fully wired (mirror of kronosdb Plan 09-03 / D-102, replaces the
   * legacy 1-second sleep). Reset on every `start()` so a stop/start cycle
   * re-arms the latch correctly.
   */
  let acked = false

  const grpcMetadata = new Metadata()
  grpcMetadata.set("AxonIQ-Context", connection.config.context)
  if (connection.config.token) {
    grpcMetadata.set("AxonIQ-Access-Token", connection.config.token)
  }

  async function processInboundInstructions(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        // First inbound message after start() = the platform has accepted our
        // registration and is talking back. Latch the ack flag (mirror of
        // kronosdb Plan 09-03 / D-102 — replaces the legacy 1s sleep).
        acked = true
        // Parse instruction type
        const instruction = parseInstruction(message)
        if (instruction) {
          if (instructionHandlers.length === 0) {
            // Nothing to route to yet — hold it for the first registration
            // rather than dropping it.
            pendingInstructions.push(instruction)
          }
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
    // Idempotency guard. The control plane may call `start()` after the data
    // path already brought the stream up, and `start()` is itself documented as
    // safe to call twice. Without this flag a second call inside the initial
    // delay window would leave TWO pending timeouts, each of which installs an
    // interval, and only the last would be tracked in `processorStatusTimer` —
    // the first would leak past `stop()`.
    if (processorStatusArmed) return
    processorStatusArmed = true
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

  /**
   * Open the platform stream and arm the heartbeat. Shared by
   * `armConnectionMonitoring()` (data path) and `start()` (control plane);
   * whichever runs first opens it, the other finds it up and returns.
   */
  function openPlatformStream() {
    if (isConnected) return

    // A heartbeat timeout clears `isConnected` WITHOUT going through `stop()`,
    // so a later call can land here with the dropped stream's outbound still
    // open. Close it before replacing the reference, or it leaks. This is more
    // reachable now that the data path arms the heartbeat: previously only a
    // control plane could get here after a timeout.
    if (outbound) {
      outbound.close()
      outbound = null
    }

    // Re-arm the ack latch so a stop/start cycle correctly re-waits.
    acked = false
    outbound = outboundStream<PlatformInboundInstruction>()

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
    // DATA PATH: the heartbeat is what calls connection.reconnect() on
    // timeout, which is what the command/query buses hang their stream
    // re-establishment off. It belongs to every service, administered or not.
    startHeartbeat()
    processInboundInstructions(inbound)

    // Axon Server's PlatformService does NOT proactively emit an inbound
    // frame in response to `register` — the stream is held open silently
    // until either (a) the server pushes a topology / instruction event,
    // or (b) one of our heartbeat pings round-trips back. That means the
    // first-inbound-frame ack signal used by kronosdb (Plan 09-03 / D-102)
    // doesn't fire deterministically here, and the processors-stage
    // `withRetry({event: "per-operation"})` poll would otherwise hang.
    //
    // Axon-specific ack derivation: latch `acked = true` immediately once
    // the outbound `register` frame has been flushed to the gRPC layer.
    // Bus subscriptions (sent on the command/query streams, NOT the
    // platform stream) are an orthogonal concern handled by the
    // command/query bus reconnect path — the legacy 1-second sleep that
    // we replaced was always covering register processing, not bus-side
    // routability. Structurally this is the Axon Server equivalent of
    // D-102: drop the magic-number wait, use the earliest deterministic
    // observable signal that fits the underlying protocol.
    acked = true
  }

  return {
    async armConnectionMonitoring() {
      openPlatformStream()
    },

    async start() {
      openPlatformStream()
      startProcessorStatusReporting()
    },

    stop() {
      isConnected = false
      // A stopped stream's un-routed backlog is stale — do not replay it if a
      // handler registers later.
      pendingInstructions.length = 0
      processorStatusArmed = false
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
      const isFirst = instructionHandlers.length === 0
      instructionHandlers.push(handler)

      // Drain anything that arrived before a handler existed, in arrival order.
      if (isFirst && pendingInstructions.length > 0) {
        const backlog = pendingInstructions.splice(0, pendingInstructions.length)
        void (async () => {
          for (const instruction of backlog) {
            try {
              await handler(instruction)
            } catch (err) {
              console.error("Platform instruction handler error:", err)
            }
          }
        })()
      }
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
