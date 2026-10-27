/**
 * The KronosDB command bus, over the native protocol.
 *
 * A command goes out as one DispatchCommand on a stream of its own and comes
 * back as one CommandResponse; commands the server routes HERE arrive on a
 * handler stream (see `native-handlers.ts`) and are answered on the stream
 * they came in on.
 */
import {
  messagingAdmission,
  positiveInteger,
  qualifiedNameFromString,
  qualifiedNameToString,
  type CommandBus,
  type CommandMessage,
  type QualifiedName,
  type SerializedObject,
  type UnitOfWork,
} from "@kronos-ts/core"
import { KronosDbErrorCode, mapErrorCode } from "./errors.js"
import type { BusHost } from "./kronosdb.js"
import { handlerClientId, handlerStream, type HandlerSession } from "./native-handlers.js"
import {
  NO_INSTRUCTIONS,
  creditWindow,
  deserializeResult,
  instructionBytes,
  requestTimeout,
  readMessage,
  readResult,
  writeMessage,
  writeResult,
  type FlowControlConfig,
  type IncomingMessage,
  type OutgoingMessage,
  type ProcessingInstructions,
} from "./native-messaging.js"
import { busStream, unexpected } from "./native-requests.js"
import type { FrameSink, NativeStream } from "./native-transport.js"
import {
  FrameKind,
  encodeCredit,
  encodeOpenCommandHandlers,
  encodeSubscribeCommand,
  reader,
  writeU16,
  type Writer,
} from "./protocol/kronosdb-protocol.js"
import type { ResilienceConfig } from "./resilience.js"
import { shutdownLatch as shutdownLatchValue, type ActivityHandle, type ShutdownLatch } from "./shutdown-latch.js"
import type { MessagingLimits } from "@kronos-ts/core"

/** Per-bus routing knobs. Everything shared lives on the connection. */
export type KronosDbCommandBusOptions = {
  /** Client-side request deadline. Default: 30000ms. */
  timeoutMs?: number
  /** Receive credits, refilled on receipt; these do not cap running handlers. */
  flowControl?: FlowControlConfig
  /** Relative share of routed work this instance advertises. Default: 100. */
  loadFactor?: number
  /** Per-extension resilience config (D-100 / D-101). */
  resilience?: Partial<ResilienceConfig>
  /** Bounded admission; excess nested work receives an overload error. */
  limits?: MessagingLimits
}

/**
 * A command bus backed by KronosDB.
 *
 * ## correlation and the interceptor layer
 *
 * The returned bus stamps no correlation of its own. A host that wants roots
 * seeded wraps the OUTERMOST bus with `interceptingCommandBus(bus, correlation)`,
 * so whatever a host transforms runs BEFORE the message is serialized onto the
 * wire. A command born inside a handler already carries its correlation by then:
 * `correlatingHandler` overlaid the task's map onto it before any bus saw it.
 *
 * This mirrors AxonFramework, where dispatch interception always sits outside
 * the routing bus. AF4's `AxonServerCommandBus.dispatch` is
 * `doDispatch(dispatchInterceptors.intercept(commandMessage), cb)`; AF5 expresses
 * the same thing through decorator order —
 * `DISTRIBUTED_COMMAND_BUS_ORDER = InterceptingCommandBus.DECORATION_ORDER - 50`
 * stacks the buses `InterceptingCommandBus → DistributedCommandBus → LocalCommandBus`.
 *
 * Double application of `correlation` is harmless: both of its fields are `??`
 * seeds, so a `next` that is itself intercepting simply sees them already set.
 *
 * ## The next segment is a real bus
 *
 * `next` is a `CommandBus`, not a private handler map. `subscribe` registers
 * on it AND announces the name to the server; a command the SERVER routes back
 * here is dispatched into it. That is what makes the unit-of-work policy you
 * chose for `next` — `postgresUnitOfWork(unitOfWork, pg)`, say — apply to
 * server-routed work exactly as it applies to anything else.
 *
 * Server-side routing is unchanged: KronosDB is a smart hub, so an outbound
 * dispatch ALWAYS goes to the server, even for a command this instance handles.
 * There is no client-side prefer-next fork here (that is RabbitMQ's model).
 *
 * ## The bus is a NAME, not a context
 *
 * `bus` is the server-scoped messaging namespace this wrapper subscribes and
 * dispatches on — a plain string, independent of any event store context
 * (server ADR-0006). The connection binds it to a small id the first time it
 * is used, and every request carries that id; omitted, it is the server's
 * `default` bus. There is NO server-side fallback from bus to context:
 * contexts address logs, buses address messaging, and a host that wants them
 * 1:1 names the bus after the context — one visible decision at this call
 * site. With the 0.9 fabric the name is also the CLUSTER-WIDE identity:
 * subscribe handlers on this bus via any node, dispatch via any node, and the
 * server forwards.
 *
 * ## One handler stream per client and bus
 *
 * The handler stream is registered under this connection's `clientId`; a newer
 * stream of the same client on the same bus REPLACES an older one (that is how
 * a reconnecting handler takes over from its stale stream). A second command
 * bus built for the same bus name on one connection is not a restart, so it is
 * registered under a client id of its own (`<clientId>#2`) and the two coexist.
 */
export function kronosDbCommandBus<U extends UnitOfWork = UnitOfWork>(
  next: CommandBus<U>,
  kdb: BusHost,
  bus: string = "default",
  options: KronosDbCommandBusOptions = {},
): CommandBus<U> {
  const { flowControl, loadFactor: commandLoadFactor, resilience } = options
  const serializer = kdb.serializer
  const connection = kdb.connection
  const link = connection.native
  const { clientId, componentName } = connection.config
  const handlerId = handlerClientId(connection, "command", bus)
  const shutdownLatch = shutdownLatchValue()
  const requestTimeoutMs = requestTimeout(options.timeoutMs)
  const inboundAdmission = messagingAdmission("inbound handlers", options.limits?.maxConcurrentHandlers ?? 128, options.limits?.observe)
  const outboundAdmission = messagingAdmission("pending requests", options.limits?.maxPendingRequests ?? 1024, options.limits?.observe)
  kdb.registerShutdownLatch(shutdownLatch)

  const { permits: PERMITS, threshold: THRESHOLD } = creditWindow(flowControl)
  const loadFactor = commandLoadFactor ?? 100

  // Names this instance announced. Kept so a reconnect can re-announce them and
  // so an unroutable inbound command still gets NO_HANDLER_FOR_COMMAND rather
  // than whatever `next.dispatch` happens to throw for an unknown name.
  const localHandlers = new Set<string>()
  const names = new Map<string, QualifiedName>()
  const nameOf = (name: string): QualifiedName => {
    let known = names.get(name)
    if (known === undefined) {
      known = qualifiedNameFromString(name)
      names.set(name, known)
    }
    return known
  }

  // The permits the server holds for this stream incarnation, as far as this
  // side can tell; undefined until the first grant. Permits belong to the
  // stream, not to a name: one is one command of any name it handles.
  let credit: number | undefined

  function grant(session: HandlerSession): void {
    session.send(FrameKind.Credit, (w) => encodeCredit(w, { credits: PERMITS }))
    credit = (credit ?? 0) + PERMITS
  }

  function subscribeOn(session: HandlerSession, commandName: string): void {
    session.instruct(FrameKind.SubscribeCommand, (w, instructionId) =>
      encodeSubscribeCommand(w, { instructionId, command: commandName, loadFactor }),
    )
  }

  const stream = handlerStream({
    link,
    bus,
    openKind: FrameKind.OpenCommandHandlers,
    writeOpen: (w: Writer, busId: number) => encodeOpenCommandHandlers(w, { bus: busId, clientId: handlerId, componentName }),
    canReconnect: () =>
      !shutdownLatch.shuttingDown &&
      connection.state !== "closed" &&
      connection.state !== "disconnected" &&
      connection.state !== "reconnecting",
    resilience,
    announce(session) {
      credit = undefined
      for (const commandName of localHandlers) subscribeOn(session, commandName)
      if (localHandlers.size > 0) grant(session)
    },
    onFrame(session, kind, buf, start, end) {
      if (kind !== FrameKind.HandleCommand) return
      const incoming = readMessage(reader(buf, start, end))
      // Credits bound delivery batches, not unfinished handlers. Replenish on
      // receipt: completion-based credits or a fixed handler semaphore can
      // deadlock when every admitted parent is waiting for a queued child.
      if (credit !== undefined) {
        credit -= 1
        if (credit <= THRESHOLD && !shutdownLatch.shuttingDown) grant(session)
      }
      void handleInbound(incoming, session).catch((err) => {
        console.error("KronosDB command bus: inbound response failed", err)
      })
    },
    onEnded() {
      credit = undefined
    },
  })

  shutdownLatch.onShutdown(stream.retire)
  connection.onDisconnect(() => stream.abandon())
  connection.onReconnect(() => {
    if (!shutdownLatch.shuttingDown) stream.restart()
  })
  kdb.registerReadiness?.(() => stream.acknowledged())

  async function handleInbound(incoming: IncomingMessage, session: HandlerSession): Promise<void> {
    let activity: ActivityHandle | undefined
    let admission: ReturnType<typeof inboundAdmission.enter> | undefined
    let result: SerializedObject | undefined
    let errorCode = ""
    let errorMessage = ""

    try {
      try {
        // Remote callers have no outbound dispatch activity on this adapter.
        // Track the entire handling, including result serialization and enqueue.
        // Registration also rejects new work once shutdown has begun.
        activity = shutdownLatch.registerActivity()
        admission = inboundAdmission.enter()
        if (localHandlers.has(incoming.name)) {
          const commandMessage: CommandMessage = {
            kind: "command",
            identifier: incoming.messageId,
            name: nameOf(incoming.name),
            payload: deserializeResult(serializer, incoming.payload),
            metadata: incoming.metadata,
            timestamp: incoming.timestamp,
          }

          // The local bus opens a fresh unit of work for EVERY wire command,
          // including children of handlers running on this same connection.
          const value = await next.dispatch(commandMessage)
          result = value !== undefined ? serializer.serialize(value, "result", "") : undefined
        } else {
          errorCode = KronosDbErrorCode.NO_HANDLER_FOR_COMMAND
          errorMessage = `No next handler for command "${incoming.name}"`
        }
      } catch (err) {
        // Decode, handler, and result-encoding failures belong to this request;
        // none should terminate the receive loop or reconnect the stream.
        errorCode = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
        errorMessage = err instanceof Error ? err.message : String(err)
        result = undefined
      }

      // `session` is the incarnation the command arrived on: a late handler
      // must not send an old request's response on a replacement stream.
      const respond = () =>
        session.send(FrameKind.CommandHandled, (w) =>
          writeResult(w, { requestId: incoming.messageId, errorCode, errorMessage, location: componentName, payload: result }),
        )
      try {
        respond()
      } catch (err) {
        // A response the frame cannot hold is still an answer: say so, rather
        // than leave the dispatcher to wait out its deadline.
        errorCode = KronosDbErrorCode.COMMAND_EXECUTION_ERROR
        errorMessage = err instanceof Error ? err.message : String(err)
        result = undefined
        respond()
      }
    } finally {
      admission?.end()
      activity?.end()
    }
  }

  /** Send one command and wait for its response, or the deadline. */
  function request(message: OutgoingMessage, instructions: Uint8Array): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      let native: NativeStream | undefined
      let done = false
      const timer = setTimeout(() => {
        if (done) return
        done = true
        native?.cancel()
        reject(new Error(`Messaging deadline exceeded after ${requestTimeoutMs}ms`))
      }, requestTimeoutMs)

      const sink: FrameSink = (kind, _flags, buf, start, end) => {
        if (done) return
        done = true
        clearTimeout(timer)
        try {
          if (kind !== FrameKind.CommandResponse) throw unexpected(kind, "CommandResponse")
          const result = readResult(reader(buf, start, end))
          if (result.errorCode !== "") throw mapErrorCode(result.errorCode, result.errorMessage || "Unknown error")
          resolve(deserializeResult(serializer, result.payload))
        } catch (error) {
          reject(error)
        }
      }
      const fail = (error: Error): void => {
        if (done) return
        done = true
        clearTimeout(timer)
        reject(error)
      }

      try {
        const opened = busStream(
          link,
          bus,
          FrameKind.DispatchCommand,
          (w, busId) => {
            writeU16(w, busId)
            writeMessage(w, message, instructions, clientId, componentName)
          },
          true,
          sink,
          fail,
        )
        if (opened instanceof Promise) {
          opened.then((s) => {
            native = s
            if (done) s.cancel()
          }, fail)
        } else {
          native = opened
        }
      } catch (error) {
        fail(error instanceof Error ? error : new Error(String(error)))
      }
    })
  }

  return {
    async dispatch(unstamped: CommandMessage): Promise<unknown> {
      const activity = shutdownLatch.registerActivity()
      let admission: ReturnType<typeof outboundAdmission.enter> | undefined
      try {
        admission = outboundAdmission.enter(unstamped.identifier)
        const commandName = qualifiedNameToString(unstamped.name)
        // A transport is not a task: it has no unit of work, so it has no clock.
        // A message that reaches the wire with no instant yet gets one from system
        // time here — the envelope crosses a process boundary and must be fully
        // formed.
        const outgoing: OutgoingMessage = {
          messageId: unstamped.identifier,
          name: commandName,
          timestamp: unstamped.timestamp ?? Date.now(),
          payload: serializer.serialize(unstamped.payload, commandName, ""),
          metadata: unstamped.metadata,
        }
        const hints = unstamped.metadata?.processingInstructions as ProcessingInstructions | undefined
        return await request(outgoing, hints === undefined ? NO_INSTRUCTIONS : instructionBytes(hints))
      } finally {
        admission?.end()
        activity.end()
      }
    },

    subscribe(commandName, handler) {
      localHandlers.add(commandName)
      next.subscribe(commandName, handler)

      const session = stream.session
      if (session === undefined) {
        // Not open yet, or coming back: opening announces every name.
        stream.start()
        return
      }
      subscribeOn(session, commandName)
      // The stream's window covers this name too; only a stream that has
      // none yet needs one.
      if (credit === undefined) grant(session)
    },
  }
}
