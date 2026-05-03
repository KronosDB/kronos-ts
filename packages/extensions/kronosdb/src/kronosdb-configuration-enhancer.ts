import {
  qualifiedNameToString,
  qualifiedNameFromString,
  generateIdentifier,
  type Serializer,
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
import type { KronosDbConnectionConfig } from "./connection.js"
import { connectToKronosDb, type KronosDbConnection } from "./connection.js"
import { createKronosMetadata } from "./connection.js"
import { createKronosDbEventStore } from "./kronosdb-event-store.js"
import { createKronosDbSnapshotStore } from "./kronosdb-snapshot-store.js"
import { metadataToProto, metadataFromProto } from "./metadata-conversion.js"
import { createOutboundStream } from "./outbound-stream.js"
import { Metadata } from "nice-grpc"
import { mapErrorCode, KronosDbErrorCode } from "./errors.js"
import { createShutdownLatch, type ShutdownLatch } from "./shutdown-latch.js"
import { createPlatformConnection, type PlatformConnection, type PlatformServiceOptions } from "./platform-service.js"

const DEFAULT_PERMITS = 5000n
const DEFAULT_THRESHOLD = 2500n

export interface FlowControlConfig {
  permits?: number
  refillThreshold?: number
}

export interface ProcessingInstructions {
  routingKey?: string
  priority?: number
  timeoutMs?: number
}

// Processing instruction keys — aligned with KronosDB's ProcessingKey enum
const INSTRUCTION_KEY = {
  ROUTING_KEY: 0,
  PRIORITY: 1,
  TIMEOUT: 2,
  NR_OF_RESULTS: 3,
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

function defaultQueryInstructions(timeoutMs: number): any[] {
  return [
    { key: INSTRUCTION_KEY.TIMEOUT, value: { numberValue: BigInt(timeoutMs) } },
    { key: INSTRUCTION_KEY.NR_OF_RESULTS, value: { numberValue: 1n } },
  ]
}

/**
 * Configuration enhancer that replaces local infrastructure with
 * KronosDB-backed implementations.
 *
 * - Replaces the event store with KronosDB event store over gRPC
 * - Replaces the snapshot store with KronosDB snapshot store
 * - Replaces the command bus with a distributed command bus
 * - Replaces the query bus with a distributed query bus
 *
 * ```ts
 * EventSourcingConfigurer.create()
 *   .configure(configureCourses)
 *   .registerEnhancer(kronosDbConfigurationEnhancer({
 *     componentName: "university-service",
 *   }))
 *   .start()
 * ```
 */
export function kronosDbConfigurationEnhancer(
  serverConfig: KronosDbConnectionConfig & {
    commandFlowControl?: FlowControlConfig
    queryFlowControl?: FlowControlConfig
    platformService?: PlatformServiceOptions
    shortcutQueriesToLocalHandlers?: boolean
    commandLoadFactor?: number
    commandTimeoutMs?: number
    queryTimeoutMs?: number
  },
): ConfigurationEnhancer {
  let connection: KronosDbConnection | undefined
  let platform: PlatformConnection | undefined
  const busLatches: Array<{ initiateShutdown(): Promise<void> }> = []

  function getConnection(): KronosDbConnection {
    connection = connection ?? connectToKronosDb(serverConfig)
    return connection
  }

  return {
    order: -100,

    enhance(registry: ComponentRegistry) {
      registry.register(ComponentKeys.EVENT_STORE, (config) => {
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        return createKronosDbEventStore(getConnection(), serializer)
      })

      registry.register(ComponentKeys.SNAPSHOT_STORE, (config) => {
        const serializer = config.getComponent<Serializer>(ComponentKeys.SERIALIZER)
        return createKronosDbSnapshotStore(getConnection(), serializer)
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
      platform = createPlatformConnection(getConnection(), serverConfig.platformService)

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

      // Eagerly build buses so handler subscriptions are sent to KronosDB
      config.getComponent(ComponentKeys.COMMAND_BUS)
      config.getComponent(ComponentKeys.QUERY_BUS)

      // Give KronosDB time to process handler subscriptions
      await new Promise((r) => setTimeout(r, 1000))
    },

    async onStop() {
      await Promise.all(busLatches.map((l) => l.initiateShutdown()))
      platform?.stop()
      connection?.close()
    },
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

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
 * A command bus backed by KronosDB.
 *
 * Uses KronosDB's CommandService which has the same architecture as Axon Server:
 * - Bidirectional stream for handler registration + inbound command delivery
 * - Unary Dispatch RPC for sending commands
 * - Flow control via permits
 *
 * Key difference: KronosDB uses `SerializedObject` for command payloads
 * (not raw bytes), and metadata uses `MetadataValue` (same as Axon).
 */
function createDistributedCommandBus(
  connection: KronosDbConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  commandLoadFactor?: number,
): CommandBus {
  const metadata = createKronosMetadata(connection.config)
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))

  const localSegment = new Map<string, (message: CommandMessage) => Promise<unknown>>()

  let outbound = createOutboundStream<any>()
  let streamStarted = false
  let permits = 0n
  let streamRetryCount = 0

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    // Open the stream — permits are granted AFTER handler subscriptions
    // (KronosDB requires handlers to be registered before permits are meaningful)
    const inbound = connection.commands.openStream(outbound.iterable, { metadata })
    processInboundCommands(inbound)
  }

  function grantPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function reestablishStream() {
    outbound.close()
    outbound = createOutboundStream<any>()
    streamStarted = false
    permits = 0n
    ensureStreamStarted()
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
    // Grant permits AFTER all subscriptions are sent
    grantPermits()
    streamRetryCount = 0
  }

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
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(commandMessage.metadata, async () => {
              return handler(commandMessage)
            })
          } catch (err) {
            errorCode = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_COMMAND
          errorMsg = `No local handler for command "${commandName}"`
        }

        // Send CommandResponse back
        const responseSerialized = resultPayload !== undefined
          ? serializePayload("result", resultPayload)
          : undefined

        outbound.send({
          commandResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: responseSerialized,
            metadata: {},
            processingInstructions: [],
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

      console.error("Distributed command bus: inbound stream error, attempting re-establishment", err)

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
        const serialized = serializePayload(commandName, message.payload)

        const response = await connection.commands.dispatch({
          messageIdentifier: message.identifier,
          name: commandName,
          timestamp: BigInt(message.timestamp),
          payload: serialized,
          metadata: metadataToProto(message.metadata),
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

        return deserializePayload(response.payload?.data as Uint8Array | undefined, response.payload?.type, response.payload?.revision)
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
      // Grant permits AFTER subscription — KronosDB requires handler
      // to exist before permits can be associated with it
      grantPermits()
    },
  }
}

// ---------------------------------------------------------------------------
// Distributed Query Bus
// ---------------------------------------------------------------------------

/**
 * A query bus backed by KronosDB.
 *
 * Uses KronosDB's QueryService which supports:
 * - Point-to-point and scatter-gather query dispatch
 * - Subscription queries with initial result + live updates
 * - Flow-controlled bidirectional streams for handler registration
 */
function createDistributedQueryBus(
  connection: KronosDbConnection,
  unitOfWorkRunner: UoWRunner,
  shutdownLatch: ShutdownLatch,
  serializer: Serializer,
  flowControl?: FlowControlConfig,
  shortcutQueriesToLocalHandlers?: boolean,
  queryTimeoutMs?: number,
): QueryBus {
  const metadata = createKronosMetadata(connection.config)
  const PERMITS = BigInt(flowControl?.permits ?? Number(DEFAULT_PERMITS))
  const THRESHOLD = BigInt(flowControl?.refillThreshold ?? Number(DEFAULT_THRESHOLD))
  const { serializePayload, deserializePayload } = createPayloadHelpers(serializer)

  const localSegment = new Map<string, (message: QueryMessage) => Promise<unknown>>()
  const subscriptions = new Map<string, UpdateHandler>()

  let outbound = createOutboundStream<any>()
  let streamStarted = false
  let permits = 0n
  let streamRetryCount = 0

  function ensureStreamStarted() {
    if (streamStarted) return
    streamStarted = true

    const inbound = connection.queries.openStream(outbound.iterable, { metadata })
    processInboundQueries(inbound)
  }

  function grantQueryPermits() {
    outbound.send({
      flowControl: { clientId: connection.config.clientId, permits: PERMITS },
      instructionId: "",
    })
    permits += PERMITS
  }

  function reestablishStream() {
    outbound.close()
    outbound = createOutboundStream<any>()
    streamStarted = false
    permits = 0n
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
    grantQueryPermits()
    streamRetryCount = 0
  }

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
              payload: deserializePayload(proto.payload?.data as Uint8Array | undefined, proto.payload?.type, proto.payload?.revision),
              metadata: metadataFromProto(proto.metadata ?? {}),
              timestamp: Number(proto.timestamp),
            }

            resultPayload = await unitOfWorkRunner(queryMessage.metadata, async () => {
              return handler(queryMessage)
            })
          } catch (err) {
            errorCode = KronosDbErrorCode.QUERY_EXECUTION_ERROR
            errorMsg = err instanceof Error ? err.message : String(err)
          }
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_QUERY
          errorMsg = `No local handler for query "${queryName}"`
        }

        const responseSerialized = resultPayload !== undefined
          ? serializePayload("result", resultPayload)
          : undefined

        outbound.send({
          queryResponse: {
            messageIdentifier: generateIdentifier(),
            requestIdentifier: proto.messageIdentifier,
            errorCode,
            errorMessage: errorCode
              ? { message: errorMsg, location: connection.config.componentName, details: [], errorCode }
              : undefined,
            payload: responseSerialized,
            metadata: {},
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

        // Local shortcut
        if (shortcutQueriesToLocalHandlers) {
          const localHandler = localSegment.get(queryName)
          if (localHandler) {
            return unitOfWorkRunner(message.metadata, async () => {
              return localHandler(message)
            })
          }
        }

        const serialized = serializePayload(queryName, message.payload)

        const responseStream = connection.queries.query({
          messageIdentifier: message.identifier,
          query: queryName,
          timestamp: BigInt(message.timestamp),
          payload: serialized,
          metadata: metadataToProto(message.metadata),
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
          return deserializePayload(response.payload?.data as Uint8Array | undefined, response.payload?.type, response.payload?.revision)
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
      // Grant permits AFTER subscription
      grantQueryPermits()
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
      const serialized = serializePayload(queryName, message.payload)

      const outboundSub = createOutboundStream<any>()

      // Send subscribe request
      outboundSub.send({
        subscribe: {
          subscriptionIdentifier: subscriptionId,
          numberOfPermits: BigInt(bufferSize ?? 256),
          queryRequest: {
            messageIdentifier: message.identifier,
            query: queryName,
            timestamp: BigInt(message.timestamp),
            payload: serialized,
            metadata: metadataToProto(message.metadata),
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
              if (response.initialResult.errorCode && response.initialResult.errorCode !== "") {
                initialResultReject?.(mapErrorCode(response.initialResult.errorCode, response.initialResult.errorMessage?.message ?? "Unknown error"))
              } else {
                initialResultResolve?.(deserializePayload(response.initialResult.payload?.data as Uint8Array | undefined, response.initialResult.payload?.type, response.initialResult.payload?.revision))
              }
              initialResultResolve = null
              initialResultReject = null
            } else if (response.update) {
              const update = deserializePayload(response.update.payload?.data as Uint8Array | undefined, response.update.payload?.type, response.update.payload?.revision)
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
