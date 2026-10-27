import type { KronosDbConnection } from "./connection.js"
import type { ProcessorStatusSupplier } from "./event-processor-info.js"
import { toEventProcessorInfo } from "./event-processor-info.js"
import type { NativeStream } from "./native-transport.js"
import {
  FrameKind,
  ProcessorAction,
  decodeProcessorInstruction,
  encodeInstructionResult,
  encodeOpenPlatform,
  encodePlatformHeartbeat,
  encodeProcessorStatus,
  reader,
  type ClientIdentity,
} from "./protocol/kronosdb-protocol.js"

/**
 * Server-initiated instructions received via the platform stream.
 *
 * KronosDB sends one ProcessorInstruction frame per action, naming the
 * processor and, for segment actions, the segment.
 */
export type PlatformInstruction =
  | { kind: "pause-processor"; processorName: string }
  | { kind: "start-processor"; processorName: string }
  | { kind: "release-segment"; processorName: string; segmentId: number }
  | { kind: "split-segment"; processorName: string; segmentId: number }
  | { kind: "merge-segment"; processorName: string; segmentId: number }
  | { kind: "reconnect-request" }

export type InstructionHandler = (instruction: PlatformInstruction) => void | Promise<void>

export type PlatformConnection = {
  start(): Promise<void>
  stop(): void
  onInstruction(handler: InstructionHandler): void
  registerProcessorStatusSupplier(supplier: ProcessorStatusSupplier): void
  readonly connected: boolean
  /**
   * Resolves with `true` once KronosDB has acknowledged this client's
   * registration on the platform stream: the server opens every platform
   * stream with a NodeInfo frame, so the first frame from the server is the
   * earliest observable point at which it has accepted the registration.
   * Resolves with `false` if the platform is not yet started.
   *
   * This replaces the legacy 1-second sleep at
   * kronosdb-configuration-enhancer.ts:216 (D-102): the kronosDb extension's
   * `start()` polls this method via `withRetry` so the
   * application waits exactly long enough for handler subscriptions to be
   * routable, no longer or shorter.
   */
  subscriptionsAcked(): Promise<boolean>
}

export type PlatformServiceOptions = {
  /** How often to tell the server this client is alive, in ms. Default: 5000 */
  heartbeatIntervalMs?: number
  /**
   * How long the server may say nothing on the platform stream before the
   * connection is considered lost, in ms. The server sends a heartbeat every
   * `heartbeat-interval` (10 s by default), so this must be well above that.
   * Default: 30000
   */
  heartbeatTimeoutMs?: number
  /** Processor status reporting interval in ms. Default: 500 */
  processorsNotificationRateMs?: number
  /** Delay before first processor status report in ms. Default: 5000 */
  processorsNotificationInitialDelayMs?: number
}

/**
 * What a ProcessorInstruction asks of this client, or `null` for an action a
 * processor here has nothing to do for (`report_status` is answered by the
 * platform connection itself; anything unknown is refused).
 */
export function parseInstruction(message: {
  action: number
  processorName: string
  segment: number
}): PlatformInstruction | null {
  switch (message.action) {
    case ProcessorAction.pause:
      return { kind: "pause-processor", processorName: message.processorName }
    case ProcessorAction.start:
      return { kind: "start-processor", processorName: message.processorName }
    case ProcessorAction.release_segment:
      return { kind: "release-segment", processorName: message.processorName, segmentId: message.segment }
    case ProcessorAction.split_segment:
      return { kind: "split-segment", processorName: message.processorName, segmentId: message.segment }
    case ProcessorAction.merge_segment:
      return { kind: "merge-segment", processorName: message.processorName, segmentId: message.segment }
    default:
      return null
  }
}

/**
 * What the platform stream remembers of an instruction until a handler has run
 * it: the instruction, and the stream it came on — the result goes back there,
 * not on whatever stream is open by the time the handler is done.
 */
type Pending = { instruction: PlatformInstruction; instructionId: string; stream: NativeStream | undefined }

/**
 * Creates a PlatformService connection to KronosDB.
 *
 * The platform stream is the control plane:
 * 1. Registers this client with KronosDB
 * 2. Answers the server's heartbeats and sends its own, so each side knows the other is there
 * 3. Receives instructions from KronosDB (split, merge, pause, resume) and reports how each went
 * 4. Reports event processor status periodically
 */
export function platformConnection(
  connection: KronosDbConnection,
  options?: PlatformServiceOptions,
): PlatformConnection {
  const heartbeatIntervalMs = options?.heartbeatIntervalMs ?? 5000
  const heartbeatTimeoutMs = options?.heartbeatTimeoutMs ?? 30000
  const processorsNotificationRateMs = options?.processorsNotificationRateMs ?? 500
  const processorsNotificationInitialDelayMs = options?.processorsNotificationInitialDelayMs ?? 5000
  const link = connection.native

  const instructionHandlers: InstructionHandler[] = []
  const processorStatusSuppliers: ProcessorStatusSupplier[] = []
  /**
   * Instructions that arrived before anything registered a handler.
   *
   * The stream is meant to go live only after the control plane has registered
   * (see `kronosDbControlPlane`), but the backend also starts the stream on its
   * own for the subscription-ack barrier — `subscriptionsAcked()` has no signal
   * without a live stream. That leaves a window in which KronosDB can push an
   * instruction at a client that has nothing to route it to. Buffering makes the
   * window harmless instead of merely forbidden: the first `onInstruction`
   * registration drains this queue in arrival order.
   */
  const pendingInstructions: Pending[] = []
  let isConnected = false
  let monitoringArmed = false
  let recoveryTimer: ReturnType<typeof setTimeout> | undefined
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null
  let processorStatusInitialTimer: ReturnType<typeof setTimeout> | undefined
  let processorStatusTimer: ReturnType<typeof setInterval> | null = null
  let processorStatusArmed = false
  let lastFromServer = Date.now()
  /** The open platform stream, and the incarnation counter that tells a late callback it is not this one's. */
  let current: NativeStream | undefined
  let incarnation = 0
  /** Instructions are run one after the other, in the order the server sent them. */
  let instructionQueue: Promise<void> = Promise.resolve()
  /**
   * Latches once KronosDB sends its first frame after registration — the
   * earliest observable signal that the platform stream is fully wired (D-102,
   * replaces the legacy 1s sleep). Reset on every open so a stop/start cycle
   * re-arms the latch correctly.
   */
  let acked = false

  const identity: ClientIdentity = {
    clientId: connection.config.clientId,
    componentName: connection.config.componentName,
    version: "1.0.0",
    tags: [],
  }

  function scheduleRecovery() {
    if (!monitoringArmed || recoveryTimer || connection.state === "closed" || connection.state === "reconnecting") return
    recoveryTimer = setTimeout(() => {
      recoveryTimer = undefined
      if (!monitoringArmed || connection.state === "closed") return
      void connection.reconnect().catch((error) => console.error("Platform recovery failed", error))
    }, connection.config.reconnectIntervalMs ?? 2000)
  }

  connection.onDisconnect?.(() => {
    isConnected = false
    incarnation++
    current?.cancel()
    current = undefined
  })

  connection.onReconnect(() => {
    if (!monitoringArmed) return
    clearTimeout(recoveryTimer)
    recoveryTimer = undefined
    isConnected = false
    openPlatformStream()
  })

  function answer(stream: NativeStream | undefined, instructionId: string, success: boolean, message?: string): void {
    if (instructionId === "") return
    try {
      stream?.send(FrameKind.InstructionResult, (w) =>
        encodeInstructionResult(w, {
          instructionId,
          success,
          error: success ? undefined : { message: message ?? "", location: identity.componentName, details: [], errorCode: "" },
        }),
      )
    } catch {
      // The stream is going away; the server has stopped waiting.
    }
  }

  async function run({ instruction, instructionId, stream }: Pending): Promise<void> {
    let failure: string | undefined
    for (const handler of instructionHandlers) {
      try {
        await handler(instruction)
      } catch (err) {
        console.error("Platform instruction handler error:", err)
        failure ??= err instanceof Error ? err.message : String(err)
      }
    }
    answer(stream, instructionId, failure === undefined, failure)
  }

  function receive(pending: Pending): void {
    if (instructionHandlers.length === 0) {
      // Nothing to route to yet — hold it for the first registration
      // rather than dropping it.
      pendingInstructions.push(pending)
      return
    }
    instructionQueue = instructionQueue.then(() => run(pending))
  }

  function startHeartbeat() {
    if (heartbeatTimer) clearInterval(heartbeatTimer)
    lastFromServer = Date.now()

    heartbeatTimer = setInterval(() => {
      if (!isConnected || !current) return

      const silence = Date.now() - lastFromServer
      if (silence > heartbeatTimeoutMs) {
        console.warn(
          `Platform heartbeat timeout: nothing from the server in ${silence}ms ` +
          `(threshold: ${heartbeatTimeoutMs}ms). Marking connection as lost.`,
        )
        isConnected = false
        // The socket looks alive but is not answering: end it, so every stream on it hears.
        link.current()?.close()
        connection.reconnect().catch((err) => {
          console.error("Failed to reconnect after heartbeat timeout:", err)
        })
        return
      }

      try {
        current.send(FrameKind.PlatformHeartbeat, (w) => encodePlatformHeartbeat(w, {}))
      } catch {
        // The stream just ended; its failure callback takes it from here.
      }
    }, heartbeatIntervalMs)
    heartbeatTimer.unref?.()
  }

  function startProcessorStatusReporting() {
    if (processorStatusArmed) return
    processorStatusArmed = true
    if (processorStatusTimer) clearInterval(processorStatusTimer)

    processorStatusInitialTimer = setTimeout(() => {
      if (!monitoringArmed) return
      if (isConnected) reportProcessorStatus()

      processorStatusTimer = setInterval(() => {
        if (!isConnected || !current) return
        reportProcessorStatus()
      }, processorsNotificationRateMs)
      processorStatusTimer.unref?.()
    }, processorsNotificationInitialDelayMs)
    processorStatusInitialTimer.unref?.()
  }

  function reportProcessorStatus() {
    const stream = current
    if (!stream || processorStatusSuppliers.length === 0) return

    for (const supplier of processorStatusSuppliers) {
      try {
        for (const status of supplier()) {
          const info = toEventProcessorInfo(status)
          stream.send(FrameKind.ProcessorStatus, (w) => encodeProcessorStatus(w, info))
        }
      } catch (err) {
        console.warn("Failed to report processor status:", err)
      }
    }
  }

  function openPlatformStream() {
    if (isConnected) return
    current?.cancel()
    current = undefined

    // Re-arm the ack latch so a stop/start cycle correctly re-waits.
    acked = false
    const mine = ++incarnation

    const sink = (kind: number, _flags: number, buf: Uint8Array, start: number, end: number): void => {
      if (mine !== incarnation || !monitoringArmed) return
      // Whatever the server says, it is there.
      lastFromServer = Date.now()
      switch (kind) {
        case FrameKind.NodeInfo:
          // The first frame of every platform stream: the registration took.
          acked = true
          return
        case FrameKind.ServerHeartbeat:
          // The server's heartbeat is a request ("the client answers it"):
          // answer at once, and track it as liveness.
          try {
            current?.send(FrameKind.PlatformHeartbeat, (w) => encodePlatformHeartbeat(w, {}))
          } catch {
            // The stream just ended.
          }
          return
        case FrameKind.RequestReconnect:
          // The cluster changed, or this node is going: reconnect to rebalance.
          scheduleRecovery()
          return
        case FrameKind.ProcessorInstruction: {
          const message = decodeProcessorInstruction(reader(buf, start, end))
          if (message.action === ProcessorAction.report_status) {
            reportProcessorStatus()
            answer(current, message.instructionId, true)
            return
          }
          const instruction = parseInstruction(message)
          if (instruction === null) {
            answer(current, message.instructionId, false, `Unsupported processor action ${message.action}`)
            return
          }
          receive({ instruction, instructionId: message.instructionId, stream: current })
          return
        }
        default:
          // TopologyChange and whatever a later version adds: nothing to act on here.
          return
      }
    }

    const fail = (error: Error): void => {
      if (mine !== incarnation || !monitoringArmed) return
      if (isConnected) console.error("Platform stream error:", error)
      isConnected = false
      current = undefined
      scheduleRecovery()
    }

    const attach = (stream: NativeStream): void => {
      if (mine !== incarnation) {
        stream.cancel()
        return
      }
      current = stream
      isConnected = true
      startHeartbeat()
    }

    ;(async () => {
      const transport = link.current() ?? (await link.ready())
      return transport.open(FrameKind.OpenPlatform, (w) => encodeOpenPlatform(w, { client: identity }), false, sink, fail)
    })().then(attach, fail)
    // Connected as far as callers can tell from here; the stream's own failure undoes it.
    isConnected = true
  }

  return {
    async start() {
      monitoringArmed = true
      openPlatformStream()
      startProcessorStatusReporting()
    },

    stop() {
      monitoringArmed = false
      incarnation++
      clearTimeout(processorStatusInitialTimer)
      clearTimeout(recoveryTimer)
      recoveryTimer = undefined
      isConnected = false
      // A stopped stream's un-routed backlog is stale — do not replay it if a
      // handler registers later.
      pendingInstructions.length = 0
      processorStatusArmed = false
      if (heartbeatTimer) {
        clearInterval(heartbeatTimer)
        heartbeatTimer = null
      }
      if (processorStatusTimer) {
        clearInterval(processorStatusTimer)
        processorStatusTimer = null
      }
      current?.cancel()
      current = undefined
    },

    onInstruction(handler) {
      const isFirst = instructionHandlers.length === 0
      instructionHandlers.push(handler)

      // Drain anything that arrived before a handler existed, in arrival order.
      if (isFirst && pendingInstructions.length > 0) {
        for (const pending of pendingInstructions.splice(0, pendingInstructions.length)) {
          instructionQueue = instructionQueue.then(() => run(pending))
        }
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
