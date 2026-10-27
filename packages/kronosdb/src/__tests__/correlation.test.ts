/**
 * correlation on the KronosDB data path.
 *
 * The KronosDB command bus REPLACES `@kronos-ts/core`'s default in-memory bus in
 * `components`, and that default bus was the only place
 * `correlationDataDispatchInterceptor` was ever registered. So before the fix,
 * every command an app dispatched over KronosDB left the process with no
 * `correlationId` / `causationId` — while the inbound side faithfully rebuilt a
 * UnitOfWork from `message.metadata` that no longer carried any correlation.
 *
 * These tests assert on the frames the server received, i.e. the bytes that
 * actually leave the process.
 */
import { afterEach, describe, expect, it } from "bun:test"
import { emptyMetadata, generateIdentifier, qn, type Metadata } from "@kronos-ts/core"
import {
  correlatingHandler,
  commandHandlerContext,
  localCommandBus,
  localQueryBus,
  unitOfWork,
  type CommandMessage,
} from "@kronos-ts/core"
import { kronosDbCommandBus } from "../kronosdb-command-bus.js"
import { kronosDbQueryBus } from "../kronosdb-query-bus.js"
import { metadataFromEntries } from "../native-messaging.js"
import { rig, type Rig } from "./bus-test-kit.js"

function commandMessage(): CommandMessage {
  return {
    kind: "command",
    identifier: generateIdentifier(),
    name: qn("correlation", "Finish"),
    payload: { id: "x" },
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

/**
 * Correlation rides on the message: `correlatingHandler` overlays the task's
 * carried map onto what `ctx.send` / `ctx.query` give birth to, BEFORE the bus
 * sees the message, so it is on `metadata` for the local and the remote branch
 * alike. These descriptors are the minimum shape those capabilities read.
 */
const Finish = { name: qn("correlation", "Finish") } as never
const FindThing = { name: qn("correlation", "FindThing") } as never

/**
 * The command a handler is handling — the PARENT of whatever it gives birth to.
 * `messageOrigin`, the default cargo `correlatingHandler` uses, reads its
 * `correlationId` as the chain and its identifier as the cause, which is what
 * the assertions below name.
 */
function causingCommand(): CommandMessage {
  return {
    kind: "command",
    identifier: "cause-1",
    name: qn("correlation", "Start"),
    payload: { id: "x" },
    metadata: { correlationId: "corr-root" },
    timestamp: Date.now(),
  }
}

let r: Rig
afterEach(() => r?.close())

describe("KronosDB distributed command bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched command", async () => {
    r = await rig()
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host)

    const parent = causingCommand()
    const uow = unitOfWork()
    await uow.execute(async () => {
      const ctx = commandHandlerContext({ uow, commandBus: bus })
      const handler = correlatingHandler(async (_m, c: typeof ctx) => {
        // Nobody handles it: the wire is what is under test.
        await c.send(Finish, { id: "x" }).catch(() => {})
      })
      await handler(parent, ctx)
    })

    expect(r.server.dispatchedCommands).toHaveLength(1)
    const onTheWire: Metadata = metadataFromEntries(r.server.dispatchedCommands[0]!.metadata)
    expect(onTheWire.correlationId).toBe("corr-root")
    expect(onTheWire.causationId).toBe("cause-1")
  })

  it("leaves a primary dispatch (no handler, nothing carrying) untouched", async () => {
    r = await rig()
    const bus = kronosDbCommandBus(localCommandBus(unitOfWork), r.client.host)

    await bus.dispatch(commandMessage()).catch(() => {})

    const onTheWire: Metadata = metadataFromEntries(r.server.dispatchedCommands[0]!.metadata)
    expect(onTheWire.correlationId).toBeUndefined()
    expect(onTheWire.causationId).toBeUndefined()
  })
})

describe("KronosDB distributed query bus — correlation", () => {
  it("stamps correlationId/causationId onto the dispatched query", async () => {
    r = await rig()
    const bus = kronosDbQueryBus(localQueryBus(unitOfWork), r.client.host)

    const parent = causingCommand()
    const uow = unitOfWork()
    await uow.execute(async () => {
      const ctx = commandHandlerContext({ uow, queryBus: bus })
      const handler = correlatingHandler(async (_m, c: typeof ctx) => {
        await c.query(FindThing, { id: "x" }).catch(() => {})
      })
      await handler(parent, ctx)
    })

    expect(r.server.dispatchedQueries).toHaveLength(1)
    const dispatched = await captureQueryMetadata()
    expect(dispatched.correlationId).toBe("corr-root")
    expect(dispatched.causationId).toBe("cause-1")
  })
})

/** The metadata of the one query the hub received, as the wire carried it. */
async function captureQueryMetadata(): Promise<Metadata> {
  const frame = r.server.connections.flatMap((c) => c.received).find((f) => f.frame.name === "DispatchQuery")!.frame
  if (frame.name !== "DispatchQuery") throw new Error("no query was dispatched")
  return metadataFromEntries(frame.body.query.metadata)
}
