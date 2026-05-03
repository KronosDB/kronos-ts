import {
  qualifiedNameToString,
  qualifiedNameFromString,
  generateIdentifier,
  type Serializer,
  type SerializedObject,
} from "@kronos-ts/common"
// transitional: Phase 9 deletes — pulls legacy ConfigurationEnhancer surface from
// the bridge until this extension is migrated to (app: App) => void.
import {
  ComponentKeys,
  type ComponentRegistry,
  type ConfigurationEnhancer,
} from "@kronos-ts/core/legacy-enhancer-bridge"
import type { CommandBus, QueryBus, CommandMessage, QueryMessage, SubscriptionQueryResult, UpdateHandler } from "@kronos-ts/messaging"
import { type UoWRunner, createUpdateHandler, runAfterCommitOrImmediately } from "@kronos-ts/messaging"
import type { AxonServerConnectionConfig } from "./connection.js"
import { connectToAxonServer, type AxonServerConnection } from "./connection.js"
import { createAxonServerEventStore } from "./axon-server-event-store.js"
import { createAxonServerSnapshotStore } from "./axon-server-snapshot-store.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { createOutboundStream } from "./outbound-stream.js"
import { Metadata } from "nice-grpc"
import { mapErrorCode, AxonServerErrorCode } from "./errors.js"
import { createShutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"
import { createPlatformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"

/** Default flow control settings — aligned with Java's 5000 permits. */
const DEFAULT_PERMITS = 5000n
const DEFAULT_THRESHOLD = 2500n

/**
 * Flow control configuration for a bus channel.
 */
export interface FlowControlConfig {
  /** Initial permits granted to Axon Server. Default: 5000 (aligned with Java). */
  permits?: number
  /** Threshold at which to request more permits. Default: 2500 (aligned with Java). */
  refillThreshold?: number
}

/**
 * Processing instructions attached to outbound messages.
 * Controls routing, priority, and timeout behavior on Axon Server.
 */
export interface ProcessingInstructions {
  /** Routing key for consistent hashing (e.g., aggregate ID). */
  routingKey?: string
  /** Priority (higher = processed first). Default: 0 */
  priority?: number
  /** Timeout in ms. Axon Server cancels the command/query if not handled in time. */
  timeoutMs?: number
}

// Processing instruction keys (aligned with Java's ProcessingKey enum)
// Processing instruction keys — aligned with proto ProcessingKey enum
const INSTRUCTION_KEY = {
  ROUTING_KEY: 0,
  PRIORITY: 1,
  TIMEOUT: 2,
  NR_OF_RESULTS: 3,
  CLIENT_SUPPORTS_STREAMING: 8,
} as const

function toProtoProcessingInstructions(instructions?: ProcessingInstructions): any[] {
  if (!instructions) return []
  const result: any[] = []
  if (instructions.routingKey !== undefined) {
    result.push({ key: INSTRUCTION_KEY.ROUTING_KEY, value: { textValue: instructions.routingKey } })
  }
  if (instructions.priority !== undefined) {
    result.push({ key: INSTRUCTION_KEY.PRIORITY, value: { numberValue: BigInt(instructions.priority) } })
  }
  if (instructions.timeoutMs !== undefined) {
    result.push({ key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(instructions.timeoutMs) } })
  }
  return result
}

/** Build default processing instructions for query dispatch */
function defaultQueryInstructions(timeoutMs: number): any[] {
  return [
    { key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(timeoutMs) } },
    { key: INSTRUCTION_KEY.NR_OF_RESULTS, value: { numberValue: 1n } },
    { key: INSTRUCTION_KEY.CLIENT_SUPPORTS_STREAMING, value: { booleanValue: true } },
  ]
}

/**
 * Configuration enhancer that replaces local infrastructure with
 * Axon Server-backed implementations.
 *
 * - Replaces the event store with DCB event store over gRPC
 * - Replaces the command bus with a distributed command bus
 * - Replaces the query bus with a distributed query bus
 *
 * This is the TypeScript equivalent of AF5's `AxonServerConfigurationEnhancer`.
 *
 * ```
 * EventSourcingConfigurer.create()
 *   .configure(configureCourses)
 *   .registerEnhancer(axonServerConfigurationEnhancer({
 *     componentName: "university-service",
 *   }))
 *   .start()
 * ```
 */
export function axonServerConfigurationEnhancer(
  serverConfig: AxonServerConnectionConfig & {
    /** Flow control for the command bus channel. */
    commandFlowControl?: FlowControlConfig
    /** Flow control for the query bus channel. */
    queryFlowControl?: FlowControlConfig
    /** Platform service configuration (heartbeat, etc.). */
    platformService?: PlatformServiceOptions
    /**
     * When true, queries are first checked against locally registered handlers
     * before being dispatched through Axon Server. Avoids a network round-trip
     * when the handler is co-located.
     *
     * Aligned with Java's `shortcutQueriesToLocalHandlers`.
     * Default: false.
     */
    shortcutQueriesToLocalHandlers?: boolean
    /**
     * Load factor for command handler registration.
     * Signals to Axon Server how much capacity this handler has.
     * Higher value = handler can take more commands.
     *
     * Aligned with Java's `commandLoadFactor`. Default: 100.
     */
    commandLoadFactor?: number
    /**
     * Default timeout for command dispatch in ms. Default: 300000 (5 min).
     * Aligned with Java's processing instruction timeout.
     */
    commandTimeoutMs?: number
    /**
     * Default timeout for query dispatch in ms. Default: 3600000 (1 hour).
     * Aligned with Java's processing instruction timeout.
     */
    queryTimeoutMs?: number
  },
): ConfigurationEnhancer {
  let connection: AxonServerConnection | undefined
  let platform: PlatformConnection | undefined
  const busLatches: Array<{ initiateShutdown(): Promise<void> }> = []

  function getConnection(): AxonServerConnection {
    connection = connection ?? connectToAxonServer(serverConfig)
    return connection
  }

  return {
    order: -100,

    enhance(registry: ComponentRegistry) {
      registry.register(ComponentKeys.EVENT_STORE, (config) => {
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        return createAxonServerEventStore(getConnection(), serializer)
      })

      registry.register(ComponentKeys.SNAPSHOT_STORE, (config) => {
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        return createAxonServerSnapshotStore(getConnection(), serializer)
      })

      registry.register(ComponentKeys.COMMAND_BUS, (config) => {
        const uowRunner = config.getComponent<UoWRunner>(ComponentKeys.UNIT_OF_WORK_FACTORY)
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        const latch = createShutdownLatch()
        busLatches.push(latch)
        return createDistributedCommandBus(getConnection(), uowRunner, latch, serializer, serverConfig.commandFlowControl, serverConfig.commandLoadFactor)
      })

      registry.register(ComponentKeys.QUERY_BUS, (config) => {
        const uowRunner = config.getComponent<UoWRunner>(ComponentKeys.UNIT_OF_WORK_FACTORY)
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        const latch = createShutdownLatch()
        busLatches.push(latch)
        return createDistributedQueryBus(getConnection(), uowRunner, latch, serializer, serverConfig.queryFlowControl, serverConfig.shortcutQueriesToLocalHandlers, serverConfig.queryTimeoutMs)
      })
    },

    async onStart(config) {
      // Start platform service for topology management and heartbeats
      platform = createPlatformConnection(getConnection(), serverConfig.platformService)

      // Register processor control handlers — routes instructions to actual processors
      const processors = config.getOptionalComponent<any[]>(ComponentKeys.EVENT_PROCESSORS) ?? []
      const processorMap = new Map<string, any>()
      for (const proc of processors) {
        processorMap.set(proc.name, proc)
      }

      platform.onInstruction(async (instruction) => {
        switch (instruction.kind) {
          case "pause-processor": {
            const proc = processorMap.get(instruction.processorName)
            if (proc?.stop) proc.stop()
            break
          }
          case "start-processor": {
            const proc = processorMap.get(instruction.processorName)
            if (proc?.start) await proc.start()
            break
          }
          case "release-segment": {
            const proc = processorMap.get(instruction.processorName)
            if (proc?.releaseSegment) await proc.releaseSegment(instruction.segmentId)
            break
          }
          case "split-segment": {
            const proc = processorMap.get(instruction.processorName)
            if (proc?.splitSegment) await proc.splitSegment(instruction.segmentId)
            break
          }
          case "merge-segment": {
            const proc = processorMap.get(instruction.processorName)
            if (proc?.mergeSegment) await proc.mergeSegment(instruction.segmentId)
            break
          }
        }
      })

      // Register processor status supplier for periodic reporting to Axon Server
      platform.registerProcessorStatusSupplier(() => {
        return processors.map((proc: any) => ({
          name: proc.name,
          running: proc.running ?? false,
          mode: proc.supportsReset?.() === false ? "Subscribing" : "Tracking",
          isStreamingProcessor: proc.supportsReset?.() !== false,
          activeThreads: proc.running ? 1 : 0,
          availableThreads: 0,
          error: false,
          tokenStoreIdentifier: "",
          segments: proc.processingStatus
            ? Array.from(proc.processingStatus().entries()).map(([segId, status]: [number, any]) => ({
                segmentId: segId,
                caughtUp: status.caughtUp ?? false,
                replaying: status.replaying ?? false,
                onePartOf: 1,
                tokenPosition: status.position ?? 0n,
                errorState: status.error?.message ?? "",
              }))
            : [{
                segmentId: 0,
                caughtUp: true,
                replaying: proc.replaying ?? false,
                onePartOf: 1,
                tokenPosition: proc.position ?? 0n,
                errorState: "",
              }],
        }))
      })

      await platform.start()

      // Eagerly build buses so handler subscriptions are sent to Axon Server
      // before any commands/queries are dispatched
      config.getComponent(ComponentKeys.COMMAND_BUS)
      config.getComponent(ComponentKeys.QUERY_BUS)

      // Give Axon Server time to process the handler subscriptions
      await new Promise((r) => setTimeout(r, 1000))
    },

    async onStop() {
      // Drain in-flight operations before closing the connection
      await Promise.all(busLatches.map((l) => l.initiateShutdown()))
      platform?.stop()
      connection?.close()
    },
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

function createAxonMetadata(config: { context: string; token: string }): Metadata {
  const metadata = new Metadata()
  metadata.set("AxonIQ-Context", config.context)
  if (config.token) {
    metadata.set("AxonIQ-Access-Token", config.token)
  }
  return metadata
}

/**
 * Creates serializer-aware payload helpers.
 * Uses the configured Serializer from the component registry,
 * falling back to jsonSerializer() if none is configured.
 */
function createPayloadHelpers(serializer: Serializer) {
  return {
    serializePayload(name: string, payload: unknown, revision: string = "") {
      return serializer.serialize(payload, name, revision)
    },
    deserializePayload(data: Uint8Array | undefined, type: string = "", revision: string = ""): unknown {
      if (!data || data.length === 0) return undefined
      return serializer.deserialize({ data, type, revision })
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Command Bus
// ---------------------------------------------------------------------------

/**
 * A command bus backed by Axon Server.
 *
 * - **Outbound dispatch**: Always goes through Axon Server via the unary Dispatch RPC.
 *   Axon Server routes the command to the appropriate node (which may be this one).
 * - **Local segment**: Handlers subscribed via `subscribe()` are registered with
 *   Axon Server (so other nodes can route to us) and stored locally. When Axon Server
 *   routes an inbound command to this node, it's executed on the local segment
 *   within a UnitOfWork.
 */
function createDistributedCommandBus(
  connection: AxonServerConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  commandLoadFactor?: number,
): CommandBus {
  const metadata = createAxonMetadata(connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))

  // Local segment — handlers that execute on this node
  const localSegment = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  // Bidirectional stream for handler registration + inbound command handling
  let outbound = createOutboundStream<any>()
  let streamStarted = false
  let permits = 0n
  let streamRetryCount = 0

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    // Grant initial permits
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits = PERMITS

    // Open stream using connection.commands (always gets current client after reconnect)
    const inbound = connection.commands.openStream(outbound.iterable, { metadata })
    processInboundCommands(inbound)
  }

  /**
   * Re-establish the bidirectional stream and re-subscribe all handlers.
   * Called on stream error or when the connection reconnects.
   */
  function reestablishStream() {
    outbound.close()
    outbound = createOutboundStream<any>()
    streamStarted = false
    ensureStreamStarted()
    // Re-subscribe all handlers
    for (const commandName of localSegment.keys()) {
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          command: commandName,
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
          loadFactor: commandLoadFactor ?? 100,
        },
        instructionId: generateIdentifier(),
      })
    }
    streamRetryCount = 0
  }

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      reestablishStream()
    }
  })

  async function processInboundCommands(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        if (!message.command) continue

        permits--
        const proto = message.command
        const commandName = proto.name
        const handler = localSegment.get(commandName)

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (handler) {
          try {
            const commandMessage: CommandMessage = {
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(commandName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            // Execute inbound command within a UnitOfWork
            resultPayload = await unitOfWorkRunner(commandMessage.metadata, async () => {
              return handler(commandMessage)
            })
          } catch (err) {
            errorCode = AxonServerErrorCode.COMMAND_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No local handler for command "${commandName}"`
        }

        // Send response back to Axon Server
        outbound.send({
          commandResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: resultPayload !== undefined
              ? serializePayload("result", resultPayload)
              : undefined,
            metaData: {},
            processingInstructions: [],
          },
          instructionId: "",
        })

        // Refill permits when running low
        if (permits <= THRESHOLD) {
          outbound.send({
            flowControl: { clientId: connection.config.clientId, permits: PERMITS },
            instructionId: "",
          })
          permits += PERMITS
        }
      }
    } catch (err) {
      if (shutdownLatch.shuttingDown) return
      if (String(err).includes("Connection dropped")) return

      console.error("Distributed command bus: inbound stream error, attempting re-establishment", err)

      // Re-establish stream with exponential backoff
      const delay = Math.min(2000 * Math.pow(2, streamRetryCount), 30000)
      streamRetryCount++
      await new Promise((r) => setTimeout(r, delay))

      if (!shutdownLatch.shuttingDown) {
        reestablishStream()
      }
    }
  }

  return {
    async dispatch(message: CommandMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const commandName = qualifiedNameToString(message.name)

        const response = await connection.commands.dispatch({
          messageIdentifier: message.identifier,
          name: commandName,
          timestamp: BigInt(message.timestamp),
          payload: serializePayload(commandName, message.payload),
          metaData: metadataToProto(message.metadata),
          processingInstructions: toProtoProcessingInstructions(message.metadata?.processingInstructions as ProcessingInstructions | undefined),
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
        }, { metadata })

        if (response.errorCode && response.errorCode !== "") {
          throw mapErrorCode(
            response.errorCode,
            response.errorMessage?.message ?? "Unknown error",
          )
        }

        return deserializePayload(response.payload?.data as Uint8Array | undefined)
      } finally {
        activity.end()
      }
    },

    subscribe(commandName: string, handler: (message: CommandMessage) => Promise<unknown>) {
      localSegment.set(commandName, handler)

      ensureStreamStarted()
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          command: commandName,
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
          loadFactor: commandLoadFactor ?? 100,
        },
        instructionId: generateIdentifier(),
      })
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Query Bus
// ---------------------------------------------------------------------------

/**
 * A query bus backed by Axon Server.
 *
 * Same architecture as the distributed command bus:
 * - **Outbound dispatch**: Always through Axon Server.
 * - **Local segment**: Handlers registered here are stored locally and
 *   registered with Axon Server for inbound routing. Inbound queries
 *   are executed within a UnitOfWork.
 */
function createDistributedQueryBus(
  connection: AxonServerConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  shortcutQueriesToLocalHandlers?: boolean,
  queryTimeoutMs?: number,
): QueryBus {
  const metadata = createAxonMetadata(connection.config)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  const localSegment = new Map<string, (message: QueryMessage) => Promise<unknown>>()

  // Local subscription store — subscription queries are handled locally
  const subscriptions = new Map<string, UpdateHandler>()

  let outbound = createOutboundStream<any>()
  let streamStarted = false
  let permits = 0n
  let streamRetryCount = 0

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits = PERMITS

    const inbound = connection.queries.openStream(outbound.iterable, { metadata })
    processInboundQueries(inbound)
  }

  /**
   * Re-establish the bidirectional stream and re-subscribe all handlers.
   * Called on stream error or when the connection reconnects.
   */
  function reestablishStream() {
    outbound.close()
    outbound = createOutboundStream<any>()
    streamStarted = false
    ensureStreamStarted()
    for (const queryName of localSegment.keys()) {
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          query: queryName,
          resultName: "",
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
        },
        instructionId: generateIdentifier(),
      })
    }
    streamRetryCount = 0
  }

  // Auto-reestablish when the connection reconnects (e.g., after heartbeat timeout)
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown && streamStarted) {
      reestablishStream()
    }
  })

  async function processInboundQueries(inbound: AsyncIterable<any>) {
    try {
      for await (const message of inbound) {
        if (!message.query) continue

        permits--
        const proto = message.query
        const queryName = proto.query
        const handler = localSegment.get(queryName)

        let resultPayload: unknown
        let errorCode = ""
        let errorMsg = ""

        if (handler) {
          try {
            const queryMessage: QueryMessage = {
              identifier: proto.messageIdentifier,
              name: qualifiedNameFromString(queryName),
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined),
              metadata: metadataFromProto(proto.metaData),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
              return handler(queryMessage)
            })
          } catch (err) {
            errorCode = AxonServerErrorCode.QUERY_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = AxonServerErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No local handler for query "${queryName}"`
        }

        outbound.send({
          queryResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: resultPayload !== undefined
              ? serializePayload("result", resultPayload)
              : undefined,
            metaData: {},
            processingInstructions: [],
          },
          instructionId: "",
        })

        outbound.send({
          queryComplete: {
            messageId: generateIdentifier(),
            requestId: proto.messageIdentifier,
          },
          instructionId: "",
        })

        if (permits <= THRESHOLD) {
          outbound.send({
            flowControl: { clientId: connection.config.clientId, permits: PERMITS },
            instructionId: "",
          })
          permits += PERMITS
        }
      }
    } catch (err) {
      if (shutdownLatch.shuttingDown) return
      if (String(err).includes("Connection dropped")) return

      console.error("Distributed query bus: inbound stream error, attempting re-establishment", err)

      const delay = Math.min(2000 * Math.pow(2, streamRetryCount), 30000)
      streamRetryCount++
      await new Promise((r) => setTimeout(r, delay))

      if (!shutdownLatch.shuttingDown) {
        reestablishStream()
      }
    }
  }

  return {
    async query(message: QueryMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      try {
        const queryName = qualifiedNameToString(message.name)

        // Local shortcut — handle locally if handler is co-located
        if (shortcutQueriesToLocalHandlers) {
          const localHandler = localSegment.get(queryName)
          if (localHandler) {
            return unitOfWorkRunner(message.metadata, async () => {
              return localHandler(message)
            })
          }
        }

        const responseStream = connection.queries.query({
          messageIdentifier: message.identifier,
          query: queryName,
          timestamp: BigInt(message.timestamp),
          payload: serializePayload(queryName, message.payload),
          metaData: metadataToProto(message.metadata),
          processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
          clientId: connection.config.clientId,
          componentName: connection.config.componentName,
        }, { metadata })

        for await (const response of responseStream) {
          if (response.errorCode && response.errorCode !== "") {
            throw mapErrorCode(
              response.errorCode,
              response.errorMessage?.message ?? "Unknown error",
            )
          }
          return deserializePayload(response.payload?.data as Uint8Array | undefined)
        }

        throw new Error(`No response for query "${queryName}"`)
      } finally {
        activity.end()
      }
    },

    subscribe(queryName: string, handler: (message: QueryMessage) => Promise<unknown>) {
      localSegment.set(queryName, handler)

      ensureStreamStarted()
      outbound.send({
        subscribe: {
          messageId: generateIdentifier(),
          query: queryName,
          resultName: "",
          componentName: connection.config.componentName,
          clientId: connection.config.clientId,
        },
        instructionId: generateIdentifier(),
      })
    },

    subscriptionQuery(message: QueryMessage, bufferSize?: number): SubscriptionQueryResult {
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      const queryName = qualifiedNameToString(message.name)
      const subscriptionId = generateIdentifier()

      const outboundSub = createOutboundStream<any>()

      outboundSub.send({
        subscribe: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: BigInt(bufferSize ?? 256),
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(queryName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      outboundSub.send({
        getInitialResult: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: 1n,
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serializePayload(queryName, message.payload),
            metaData: metadataToProto(message.metadata),
            processingInstructions: defaultQueryInstructions(queryTimeoutMs ?? 3600000),
            clientId: connection.config.clientId,
            componentName: connection.config.componentName,
          },
        },
      })

      const responseStream = connection.queries.subscription(outboundSub.iterable, { metadata })

      let initialResultResolve: ((value: unknown) => void) | null = null
      let initialResultReject: ((error: Error) => void) | null = null
      const initialResult = new Promise<unknown>((resolve, reject) => {
        initialResultResolve = resolve
        initialResultReject = reject
      })

      ;(async () => {
        try {
          for await (const response of responseStream) {
            if (response.initialResult) {
              if (response.errorCode && response.errorCode !== "") {
                initialResultReject?.(mapErrorCode(response.errorCode, response.errorMessage?.message ?? "Unknown error"))
              } else {
                initialResultResolve?.(deserializePayload(response.initialResult.payload?.data as Uint8Array | undefined))
              }
              initialResultResolve = null
              initialResultReject = null
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data as Uint8Array | undefined)
              updateHandler.offer(update)
            } else if (response.complete) {
              updateHandler.complete()
              break
            } else if (response.completeExceptionally) {
              updateHandler.completeExceptionally(
                new Error(response.completeExceptionally.errorMessage?.message ?? "Subscription query failed"),
              )
              break
            }
          }
        } catch (err) {
          const error = err instanceof Error ? err : new Error(String(err))
          if (initialResultReject) {
            initialResultReject(error)
            initialResultResolve = null
            initialResultReject = null
          }
          updateHandler.completeExceptionally(error)
        } finally {
          subscriptions.delete(queryId)
        }
      })()

      return {
        initialResult,
        updates: updateHandler.iterable,
        close: () => {
          outboundSub.send({
            unsubscribe: {
              subscriptionIdentifier: subscriptionId,
            },
          })
          outboundSub.close()
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    subscribeToUpdates(message: QueryMessage, bufferSize?: number): AsyncIterable<unknown> & { close(): void } {
      const queryId = message.identifier
      if (subscriptions.has(queryId)) {
        throw new Error(`Subscription query already registered for identifier "${queryId}"`)
      }

      const updateHandler = createUpdateHandler(message, bufferSize)
      subscriptions.set(queryId, updateHandler)

      return {
        [Symbol.asyncIterator]: () => updateHandler.iterable[Symbol.asyncIterator](),
        close: () => {
          subscriptions.delete(queryId)
          updateHandler.complete()
        },
      }
    },

    async emitUpdate(
      queryName: string,
      filter: (queryPayload: unknown) => boolean,
      update: unknown,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          if (!handler.active) {
            subscriptions.delete(id)
            continue
          }
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (!filter(handler.query.payload)) continue

          const accepted = handler.offer(update)
          if (!accepted) {
            handler.completeExceptionally(new Error("Subscription query update buffer overflow"))
            subscriptions.delete(id)
          }
        }
      })
    },

    async completeSubscription(
      queryName: string,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue
          handler.complete()
          subscriptions.delete(id)
        }
      })
    },

    async completeSubscriptionExceptionally(
      queryName: string,
      error: Error,
      filter?: (queryPayload: unknown) => boolean,
    ): Promise<void> {
      runAfterCommitOrImmediately(() => {
        for (const [id, handler] of subscriptions) {
          const handlerQueryName = qualifiedNameToString(handler.query.name)
          if (handlerQueryName !== queryName) continue
          if (filter && !filter(handler.query.payload)) continue
          handler.completeExceptionally(error)
          subscriptions.delete(id)
        }
      })
    },
  }
}
