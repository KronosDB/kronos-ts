import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn, type CommandMessage, type QueryMessage } from "../../messaging/messages.js"
import type { CommandBus } from "../../command-handling/bus.js"
import type { QueryBus } from "../../query-handling/bus.js"
import { localCommandBus } from "../../command-handling/local-bus.js"
import { localQueryBus } from "../../query-handling/local-bus.js"
import { unitOfWork } from "../../unit-of-work/unit-of-work.js"
import { localFirstCommandBus, localFirstQueryBus } from "../local-first-bus.js"

/** A transport double: subscribes on the local bus it wraps, and records what it sends. */
function wireCommands(local: CommandBus) {
  const sent: string[] = []
  const bus: CommandBus & { readonly extra: string } = {
    extra: "kept",
    async dispatch(message) {
      sent.push(message.identifier)
      return "remote"
    },
    subscribe(name, handler) {
      local.subscribe(name, handler)
    },
  }
  return { bus, sent }
}

function wireQueries(local: QueryBus) {
  const sent: string[] = []
  const bus: QueryBus = {
    async query(message) {
      sent.push(message.identifier)
      return "remote"
    },
    subscribe(name, handler) {
      local.subscribe(name, handler)
    },
  }
  return { bus, sent }
}

const commandMessage = (name: string): CommandMessage => ({
  kind: "command",
  identifier: `id-${name}`,
  name: qn("lf", name),
  payload: {},
  metadata: emptyMetadata(),
})

const queryMessage = (name: string): QueryMessage => ({
  kind: "query",
  identifier: `id-${name}`,
  name: qn("lf", name),
  payload: {},
  metadata: emptyMetadata(),
})

describe("localFirstCommandBus", () => {
  it("hands a command subscribed through it to the local bus, and the rest to next", async () => {
    const local = localCommandBus(unitOfWork)
    const wire = wireCommands(local)
    const bus = localFirstCommandBus(wire.bus, local)
    bus.subscribe("lf.Here", async () => "local")

    expect(await bus.dispatch(commandMessage("Here"))).toBe("local")
    expect(await bus.dispatch(commandMessage("Elsewhere"))).toBe("remote")
    expect(wire.sent).toEqual(["id-Elsewhere"])
  })

  it("keeps what the bus it wraps carried", () => {
    const local = localCommandBus(unitOfWork)
    expect(localFirstCommandBus(wireCommands(local).bus, local).extra).toBe("kept")
  })
})

describe("localFirstQueryBus", () => {
  it("asks the local bus for a query subscribed through it, and next for the rest", async () => {
    const local = localQueryBus(unitOfWork)
    const wire = wireQueries(local)
    const bus = localFirstQueryBus(wire.bus, local)
    bus.subscribe("lf.Here", async () => "local")

    expect(await bus.query(queryMessage("Here"))).toBe("local")
    expect(await bus.query(queryMessage("Elsewhere"))).toBe("remote")
    expect(wire.sent).toEqual(["id-Elsewhere"])
  })
})
