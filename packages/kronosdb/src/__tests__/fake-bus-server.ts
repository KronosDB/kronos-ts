// A KronosDB hub in a few hundred lines, over the fake native server: just
// enough routing for the buses' tests to cross the real wire.
//
// It does what the real server does where a bus can tell the difference —
// handler streams that are subscribed to by name and granted credits (one pool
// per stream, whatever the name), one command per credit, answers routed back to
// the stream that dispatched, the handler of a lost stream failing its commands
// with KRONOSDB-4006, a newer stream of the same client replacing an older one
// — and nothing more. Every frame it receives stays in `connection.received`,
// so a test asserts on what actually left the process.

import {
  ErrorCode,
  FLAG_END,
  type Command,
  type CommandResult,
  type Frame,
  type ProcessingInstruction,
  type QueryResult,
  type SubscriptionEvent,
  type SubscriptionQuery,
} from "../protocol/kronosdb-protocol.js"
import { fakeNativeServer, type FakeConnection, type FakeNativeServer, type FakeNativeServerOptions, type ReceivedFrame } from "./fake-native-server.js"

type HandlerKind = "command" | "query"

export type FakeHandlerStream = {
  readonly kind: HandlerKind
  readonly connection: FakeConnection
  readonly stream: number
  readonly bus: string
  readonly clientId: string
  readonly componentName: string
  /** Names subscribed. */
  readonly names: Map<string, { loadFactor: number }>
  /** Permits left on the stream, shared by every name. */
  credit: number
  /** Every Credit frame received, in order. */
  readonly credits: number[]
  ended: boolean
}

type PendingCommand = {
  readonly dispatcher: FakeConnection
  readonly stream: number
  readonly command: Command
  readonly bus: string
  handler: FakeHandlerStream | undefined
  timer: ReturnType<typeof setTimeout> | undefined
  holdTimer: ReturnType<typeof setTimeout> | undefined
}

type PendingQuery = {
  readonly dispatcher: FakeConnection
  readonly stream: number
  readonly query: Frame & { name: "HandleQuery" }
  readonly waitingFor: Set<FakeHandlerStream>
  readonly handlers: FakeHandlerStream[]
}

type Subscription = {
  readonly dispatcher: FakeConnection
  readonly stream: number
  readonly handler: FakeHandlerStream
}

export type FakeBusOptions = FakeNativeServerOptions & {
  /** Answer a command nobody answers within this long with KRONOSDB-4005. Default: never. */
  commandTimeoutMs?: number
  /** Delay acknowledging a subscription by this long. Default: answer at once. */
  ackDelayMs?: number
}

export type FakeBusServer = FakeNativeServer & {
  /** Every handler stream ever opened. */
  readonly handlerStreams: FakeHandlerStream[]
  /** Every command dispatched, in order. */
  readonly dispatchedCommands: Command[]
  /** Every query dispatched, in order. */
  readonly dispatchedQueries: Array<{ bus: string; name: string; messageId: string; instructions: ReadonlyArray<ProcessingInstruction> }>
  /** Every subscription query opened. */
  readonly subscriptions: SubscriptionQuery[]
  /** Every platform stream ever opened. */
  readonly platforms: Array<{ connection: FakeConnection; stream: number; clientId: string }>
  /** The names of the bus ids a connection bound. */
  busNames(connection: FakeConnection): Map<number, string>
  /** When set, queries are never ended: a QueryEnd that was lost. */
  stallQueryEnd: boolean
  /** Send the platform stream a frame. */
  toPlatform(index: number, frame: Frame): void
  /** Hold a command (instead of failing it) while nobody handles it, up to this long. */
  holdUnhandled: number
}

const fail = (connection: FakeConnection, stream: number, code: number, message: string) =>
  connection.reply(stream, FLAG_END, { name: "RequestFailed", body: { code, message } })

export async function fakeBusServer(options: FakeBusOptions = {}): Promise<FakeBusServer> {
  const buses = new WeakMap<FakeConnection, Map<number, string>>()
  const handlerStreams: FakeHandlerStream[] = []
  const handlerByStream = new Map<string, FakeHandlerStream>()
  const dispatchedCommands: Command[] = []
  const dispatchedQueries: FakeBusServer["dispatchedQueries"] = []
  const subscriptions: SubscriptionQuery[] = []
  const platforms: FakeBusServer["platforms"] = []
  const waitingCommands: PendingCommand[] = []
  const commands = new Map<string, PendingCommand>()
  const queries = new Map<string, PendingQuery>()
  const subscriptionsById = new Map<string, Subscription>()
  const subscriptionByStream = new Map<string, string>()
  const dispatchByStream = new Map<string, { kind: "command" | "query"; id: string }>()

  const key = (connection: FakeConnection, stream: number) => `${connections.indexOf(connection)}:${stream}`
  const busOf = (connection: FakeConnection, id: number): string => buses.get(connection)?.get(id) ?? (id === 0 ? "default" : `?${id}`)

  const state: { server?: FakeBusServer } = {}
  let connections: FakeConnection[] = []

  function ack(connection: FakeConnection, stream: number, instructionId: string): void {
    if (instructionId === "") return
    const send = () => {
      if (!connection.closed) connection.reply(stream, 0, { name: "ServerAck", body: { ack: { instructionId, success: true } } })
    }
    if (options.ackDelayMs) setTimeout(send, options.ackDelayMs)
    else send()
  }

  function handlersFor(kind: HandlerKind, bus: string, name: string): FakeHandlerStream[] {
    return handlerStreams.filter((h) => !h.ended && h.kind === kind && h.bus === bus && h.names.has(name))
  }

  /** Queries and subscriptions waiting for a handler with credit; tried again whenever one gains any. */
  const retries = new Set<() => void>()

  function route(): void {
    for (const retry of [...retries]) retry()
    for (const pending of [...waitingCommands]) {
      const handler = handlersFor("command", pending.bus, pending.command.name).find((h) => h.credit > 0)
      if (handler === undefined) continue
      waitingCommands.splice(waitingCommands.indexOf(pending), 1)
      clearTimeout(pending.holdTimer)
      handler.credit--
      pending.handler = handler
      handler.connection.reply(handler.stream, 0, { name: "HandleCommand", body: { command: pending.command } })
    }
  }

  function answerCommand(id: string, result: CommandResult): void {
    const pending = commands.get(id)
    if (pending === undefined) return
    commands.delete(id)
    dispatchByStream.delete(key(pending.dispatcher, pending.stream))
    clearTimeout(pending.timer)
    clearTimeout(pending.holdTimer)
    pending.dispatcher.reply(pending.stream, FLAG_END, { name: "CommandResponse", body: { result } })
  }

  function lost(code: string, id: string, pending: PendingCommand): CommandResult {
    return {
      messageId: "",
      requestId: id,
      errorCode: code,
      error: { message: code === "KRONOSDB-4006" ? "the handler's connection was lost" : "no answer in time", location: "server", details: [], errorCode: code },
      payload: undefined,
      metadata: [],
      instructions: [],
    }
  }

  function endHandlerStream(handler: FakeHandlerStream): void {
    if (handler.ended) return
    handler.ended = true
    for (const [id, pending] of [...commands]) {
      if (pending.handler === handler) answerCommand(id, lost("KRONOSDB-4006", id, pending))
    }
    for (const [id, query] of queries) {
      query.waitingFor.delete(handler)
      if (query.waitingFor.size === 0) finishQuery(id, query)
    }
    for (const [id, sub] of [...subscriptionsById]) {
      if (sub.handler === handler) {
        sub.dispatcher.reply(sub.stream, FLAG_END, {
          name: "SubscriptionQueryEvent",
          body: { subscriptionId: id, event: { kind: "completeExceptionally", value: { clientId: "", componentName: "", errorCode: "KRONOSDB-4006", error: undefined } } },
        })
        subscriptionsById.delete(id)
      }
    }
  }

  function finishQuery(id: string, query: PendingQuery): void {
    if (!queries.delete(id)) return
    dispatchByStream.delete(key(query.dispatcher, query.stream))
    if (state.server?.stallQueryEnd) return
    query.dispatcher.reply(query.stream, FLAG_END, { name: "QueryEnd", body: {} })
  }

  const base = await fakeNativeServer({
    ...options,
    onClose(connection) {
      for (const handler of handlerStreams) if (handler.connection === connection) endHandlerStream(handler)
      options.onClose?.(connection)
    },
    onFrame(connection, received: ReceivedFrame) {
      options.onFrame?.(connection, received)
      const { stream, frame } = received
      switch (frame.name) {
        case "BindBus": {
          let names = buses.get(connection)
          if (names === undefined) buses.set(connection, (names = new Map([[0, "default"]])))
          names.set(frame.body.id, frame.body.name)
          connection.reply(stream, FLAG_END, { name: "BusBound", body: { id: frame.body.id } })
          return
        }
        case "OpenCommandHandlers":
        case "OpenQueryHandlers": {
          const kind: HandlerKind = frame.name === "OpenCommandHandlers" ? "command" : "query"
          const bus = busOf(connection, frame.body.bus)
          const handler: FakeHandlerStream = {
            kind,
            connection,
            stream,
            bus,
            clientId: frame.body.clientId,
            componentName: frame.body.componentName,
            names: new Map(),
            credit: 0,
            credits: [],
            ended: false,
          }
          for (const older of handlerStreams) {
            if (!older.ended && older.kind === kind && older.bus === bus && older.clientId === handler.clientId) {
              endHandlerStream(older)
              fail(older.connection, older.stream, ErrorCode.unavailable, "a newer handler stream of this client replaced this one")
            }
          }
          handlerStreams.push(handler)
          handlerByStream.set(key(connection, stream), handler)
          return
        }
        case "DispatchCommand": {
          const bus = busOf(connection, frame.body.bus)
          const command = frame.body.command
          dispatchedCommands.push(command)
          const pending: PendingCommand = { dispatcher: connection, stream, command, bus, handler: undefined, timer: undefined, holdTimer: undefined }
          if (handlersFor("command", bus, command.name).length === 0) {
            if (state.server!.holdUnhandled <= 0) {
              fail(connection, stream, ErrorCode.unavailable, `no handler available for command '${command.name}'`)
              return
            }
            pending.holdTimer = setTimeout(() => {
              const at = waitingCommands.indexOf(pending)
              if (at < 0) return
              waitingCommands.splice(at, 1)
              commands.delete(command.messageId)
              fail(connection, stream, ErrorCode.unavailable, `no handler available for command '${command.name}'`)
            }, state.server!.holdUnhandled)
          }
          if (options.commandTimeoutMs !== undefined) {
            pending.timer = setTimeout(() => answerCommand(command.messageId, lost("KRONOSDB-4005", command.messageId, pending)), options.commandTimeoutMs)
          }
          commands.set(command.messageId, pending)
          dispatchByStream.set(key(connection, stream), { kind: "command", id: command.messageId })
          waitingCommands.push(pending)
          route()
          return
        }
        case "DispatchQuery": {
          const bus = busOf(connection, frame.body.bus)
          const query = frame.body.query
          dispatchedQueries.push({ bus, name: query.name, messageId: query.messageId, instructions: query.instructions })
          dispatchByStream.set(key(connection, stream), { kind: "query", id: query.messageId })
          const deadline = Date.now() + state.server!.holdUnhandled
          let routed = false
          const attempt = (): void => {
            // The dispatcher may have given up while the query waited.
            if (routed || dispatchByStream.get(key(connection, stream))?.id !== query.messageId) return
            const all = handlersFor("query", bus, query.name).filter((h) => h.credit > 0)
            if (all.length === 0) {
              if (Date.now() < deadline) {
                retries.add(attempt)
                setTimeout(attempt, 20)
                return
              }
              retries.delete(attempt)
              dispatchByStream.delete(key(connection, stream))
              fail(connection, stream, ErrorCode.unavailable, `no handler available for query '${query.name}'`)
              return
            }
            routed = true
            retries.delete(attempt)
            const single = query.instructions.some((i) => i.key === 3 && i.value?.kind === "number" && i.value.value === 1n)
            const chosen = single ? all.slice(0, 1) : all
            const pending: PendingQuery = {
              dispatcher: connection,
              stream,
              query: { name: "HandleQuery", body: { query } },
              waitingFor: new Set(chosen),
              handlers: chosen,
            }
            queries.set(query.messageId, pending)
            for (const handler of chosen) {
              handler.credit--
              handler.connection.reply(handler.stream, 0, pending.query)
            }
          }
          attempt()
          return
        }
        case "OpenSubscriptionQuery": {
          const bus = busOf(connection, frame.body.bus)
          const subscription = frame.body.subscription
          subscriptions.push(subscription)
          subscriptionByStream.set(key(connection, stream), subscription.subscriptionId)
          const deadline = Date.now() + state.server!.holdUnhandled
          let routed = false
          const attempt = (): void => {
            // Cancelled while it waited.
            if (routed || subscriptionByStream.get(key(connection, stream)) !== subscription.subscriptionId) return
            const handler = handlersFor("query", bus, subscription.query.name).find((h) => h.credit > 0)
            if (handler === undefined) {
              if (Date.now() < deadline) {
                retries.add(attempt)
                setTimeout(attempt, 20)
                return
              }
              retries.delete(attempt)
              subscriptionByStream.delete(key(connection, stream))
              fail(connection, stream, ErrorCode.unavailable, `no handler available for query '${subscription.query.name}'`)
              return
            }
            routed = true
            retries.delete(attempt)
            handler.credit--
            subscriptionsById.set(subscription.subscriptionId, { dispatcher: connection, stream, handler })
            handler.connection.reply(handler.stream, 0, { name: "HandleSubscriptionQuery", body: { subscription } })
          }
          attempt()
          return
        }
        case "OpenPlatform": {
          platforms.push({ connection, stream, clientId: frame.body.client.clientId })
          connection.reply(stream, 0, { name: "NodeInfo", body: { nodeName: "fake", version: "0", contexts: ["default"] } })
          return
        }
        case "SubscribeCommand":
        case "SubscribeQuery": {
          const handler = handlerByStream.get(key(connection, stream))
          if (handler === undefined) return
          const name = frame.name === "SubscribeCommand" ? frame.body.command : frame.body.query
          if (!handler.names.has(name)) {
            handler.names.set(name, { loadFactor: frame.name === "SubscribeCommand" ? frame.body.loadFactor : 0 })
          }
          ack(connection, stream, frame.body.instructionId)
          route()
          return
        }
        case "UnsubscribeCommand":
        case "UnsubscribeQuery": {
          const handler = handlerByStream.get(key(connection, stream))
          if (handler === undefined) return
          handler.names.delete(frame.name === "UnsubscribeCommand" ? frame.body.command : frame.body.query)
          ack(connection, stream, frame.body.instructionId)
          return
        }
        case "Credit": {
          const handler = handlerByStream.get(key(connection, stream))
          if (handler !== undefined) {
            handler.credits.push(frame.body.credits)
            handler.credit += frame.body.credits
            route()
            return
          }
          const subscriptionId = subscriptionByStream.get(key(connection, stream))
          const sub = subscriptionId === undefined ? undefined : subscriptionsById.get(subscriptionId)
          if (sub !== undefined && subscriptionId !== undefined) {
            sub.handler.connection.reply(sub.handler.stream, 0, {
              name: "SubscriptionQueryPermits",
              body: { subscriptionId, permits: BigInt(frame.body.credits) },
            })
          }
          return
        }
        case "CommandHandled":
          answerCommand(frame.body.result.requestId, frame.body.result)
          return
        case "QueryHandled": {
          const query = queries.get(frame.body.result.requestId)
          if (query === undefined) return
          query.dispatcher.reply(query.stream, 0, { name: "QueryResponse", body: { result: frame.body.result } })
          return
        }
        case "QueryDone": {
          const handler = handlerByStream.get(key(connection, stream))
          const query = queries.get(frame.body.requestId)
          if (query === undefined || handler === undefined) return
          query.waitingFor.delete(handler)
          if (query.waitingFor.size === 0) finishQuery(frame.body.requestId, query)
          return
        }
        case "SubscriptionQueryHandled": {
          const sub = subscriptionsById.get(frame.body.subscriptionId)
          if (sub === undefined) return
          const event: SubscriptionEvent = frame.body.event
          const terminal = event.kind === "complete" || event.kind === "completeExceptionally"
          sub.dispatcher.reply(sub.stream, terminal ? FLAG_END : 0, {
            name: "SubscriptionQueryEvent",
            body: { subscriptionId: frame.body.subscriptionId, event },
          })
          if (terminal) {
            subscriptionsById.delete(frame.body.subscriptionId)
            subscriptionByStream.delete(key(sub.dispatcher, sub.stream))
          }
          return
        }
        case "Cancel": {
          const handler = handlerByStream.get(key(connection, stream))
          if (handler !== undefined) {
            endHandlerStream(handler)
            fail(connection, stream, ErrorCode.cancelled, "cancelled")
            return
          }
          const dispatch = dispatchByStream.get(key(connection, stream))
          if (dispatch?.kind === "command") {
            const pending = commands.get(dispatch.id)
            commands.delete(dispatch.id)
            clearTimeout(pending?.timer)
            clearTimeout(pending?.holdTimer)
            const at = waitingCommands.findIndex((p) => p.command.messageId === dispatch.id)
            if (at >= 0) waitingCommands.splice(at, 1)
          } else if (dispatch?.kind === "query") {
            const query = queries.get(dispatch.id)
            queries.delete(dispatch.id)
            for (const h of query?.waitingFor ?? []) {
              h.connection.reply(h.stream, 0, { name: "CancelQuery", body: { requestId: dispatch.id } })
            }
          }
          dispatchByStream.delete(key(connection, stream))
          const subscriptionId = subscriptionByStream.get(key(connection, stream))
          if (subscriptionId !== undefined) {
            const sub = subscriptionsById.get(subscriptionId)
            subscriptionsById.delete(subscriptionId)
            subscriptionByStream.delete(key(connection, stream))
            sub?.handler.connection.reply(sub.handler.stream, 0, { name: "CancelSubscriptionQuery", body: { subscriptionId } })
          }
          fail(connection, stream, ErrorCode.cancelled, "cancelled")
          return
        }
        default:
          return
      }
    },
  })

  connections = base.connections
  const server: FakeBusServer = Object.assign(base, {
    handlerStreams,
    dispatchedCommands,
    dispatchedQueries,
    subscriptions,
    platforms,
    busNames: (connection: FakeConnection) => buses.get(connection) ?? new Map([[0, "default"]]),
    stallQueryEnd: false,
    holdUnhandled: 0,
    toPlatform(index: number, frame: Frame) {
      const platform = platforms[index]
      if (!platform) throw new Error(`no platform stream ${index}`)
      platform.connection.reply(platform.stream, 0, frame)
    },
  })
  state.server = server
  return server
}
