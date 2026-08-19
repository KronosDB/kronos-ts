import { describe, it, expect } from "bun:test"
import { z } from "zod"
import {
  command,
  commandHandler,
  event,
  eventHandler,
  eventProcessor,
  inMemoryEventStore,
  inMemorySnapshotStore,
  inMemoryTokenStore,
  kronos,
  qn,
  query as queryVerb,
  send,
  simpleCommandBus,
  simpleQueryBus,
  state,
  stamped,
  unitOfWork,
  type CommandMessage,
  type EventMessage,
  type QueryMessage,
} from "../../index.js"

// ---------------------------------------------------------------------------
// One clock per task. The edge verb builds the message; the bus that mints the
// unit of work stamps the instant — so a frozen clock freezes EVERY timestamp
// under the task, commands, queries and appended events alike.
// ---------------------------------------------------------------------------

const FROZEN = 1_700_000_000_000

const OpenTicket = command({
  name: qn("clock", "OpenTicket"),
  payload: z.object({ ticketId: z.string() }),
})

const TicketOpened = event({
  name: qn("clock", "TicketOpened"),
  payload: z.object({ ticketId: z.string() }),
  tags: { ticketId: (p) => p.ticketId },
})

const Ticket = state({
  name: "ClockTicket",
  id: { ticketId: z.string() },
  initial: () => ({ open: false }),
  tags: (id) => ({ ticketId: id.ticketId }),
  evolve: [[TicketOpened, (s) => ({ ...s, open: true })]],
})

describe("stamped", () => {
  const bare = {
    kind: "command" as const,
    identifier: "id-1",
    name: qn("clock", "OpenTicket"),
    payload: {},
    metadata: {},
  }

  it("fills in the instant a message was born without", () => {
    expect(stamped<CommandMessage>(bare, () => FROZEN).timestamp).toBe(FROZEN)
  })

  it("leaves a message that already carries one alone — it is idempotent", () => {
    const already = { ...bare, timestamp: 7 }
    const twice = stamped<CommandMessage>(
      stamped<CommandMessage>(already, () => FROZEN),
      () => 9,
    )
    expect(twice.timestamp).toBe(7)
    expect(twice).toBe(already)
  })
})

describe("the clock seam", () => {
  it("the verb builds no timestamp and the bus stamps it from the task's clock", async () => {
    let seen: CommandMessage | undefined
    const bus = simpleCommandBus(() => unitOfWork(() => FROZEN))
    bus.subscribe("clock.OpenTicket", async (message) => {
      seen = message
      return undefined
    })

    await send(bus, OpenTicket, { ticketId: "t-1" })

    expect(seen?.timestamp).toBe(FROZEN)
  })

  it("stamps a query the same way, and a NESTED query from the task it joins", async () => {
    const GetTicket = queryVerb({
      name: qn("clock", "GetTicket"),
      payload: z.object({ ticketId: z.string() }),
    })

    let seen: QueryMessage | undefined
    const bus = simpleQueryBus(() => unitOfWork(() => FROZEN))
    bus.subscribe("clock.GetTicket", async (message) => {
      seen = message
      return undefined
    })

    await queryVerb(bus, GetTicket, { ticketId: "t-1" })
    expect(seen?.timestamp).toBe(FROZEN)

    // Nested: the caller's unit of work is handed in, so ITS clock stamps.
    await unitOfWork(() => 42).execute(async (uow) =>
      bus.query(
        {
          kind: "query",
          identifier: "q-2",
          name: GetTicket.name,
          payload: { ticketId: "t-1" },
          metadata: {},
        },
        uow,
      ),
    )
    expect(seen?.timestamp).toBe(42)
  })

  it("stamps the events a handler appends from the same task instant", async () => {
    const eventStore = inMemoryEventStore()
    const commandBus = simpleCommandBus(() => unitOfWork(() => FROZEN))
    const queryBus = simpleQueryBus(() => unitOfWork(() => FROZEN))

    const openTicket = commandHandler(OpenTicket, async ({ payload }, ctx) => {
      await ctx.load(Ticket, { ticketId: payload.ticketId })
      ctx.append(TicketOpened, { ticketId: payload.ticketId })
    })

    const app = kronos({
      states: [{ ...Ticket, eventStore }],
      commandHandlers: [{ ...openTicket, eventStore, commandBus, queryBus }],
    })
    try {
      await send(commandBus, OpenTicket, { ticketId: "t-1" })
      const { events } = await eventStore.source({ query: { tags: { ticketId: "t-1" } } })
      expect(events.map((e) => e.timestamp)).toEqual([FROZEN])
    } finally {
      await app.stop()
    }
  })

  it("a processor batch stamps what its handlers send from the batch's clock", async () => {
    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()
    const commandBus = simpleCommandBus(() => unitOfWork(() => 1))
    const queryBus = simpleQueryBus(() => unitOfWork(() => 1))

    let sent: CommandMessage | undefined
    commandBus.subscribe("clock.OpenTicket", async (message) => {
      sent = message
      return undefined
    })

    const onOpened = eventHandler(TicketOpened, async ({ payload }, ctx) => {
      await ctx.send(OpenTicket, { ticketId: `${payload.ticketId}-echo` })
    })

    const processor = eventProcessor({
      name: "clock-echo",
      eventStore,
      tokenStore,
      // The BATCH's clock — deliberately a different instant from the command
      // bus's, so what stamps `ctx.send` is unambiguous.
      unitOfWork: () => unitOfWork(() => 555),
    })

    const app = kronos({
      eventHandlers: [{ ...onOpened, commandBus, queryBus, processor }],
    })
    try {
      const seed: EventMessage = {
        kind: "event",
        identifier: "e-1",
        name: TicketOpened.name,
        version: TicketOpened.version,
        payload: { ticketId: "t-1" },
        metadata: {},
        timestamp: 0,
        tags: TicketOpened.tags!({ ticketId: "t-1" }),
      }
      await eventStore.append([seed])

      const deadline = Date.now() + 2000
      while (sent === undefined && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 5))
      }
      expect(sent?.timestamp).toBe(555)
    } finally {
      await app.stop()
    }
  })

  it("scheduleAfter measures the delay from the task's instant, not wall time", async () => {
    const armed: Array<{ event: EventMessage; at: Date }> = []
    const eventStore = inMemoryEventStore()
    const snapshotStore = inMemorySnapshotStore()
    const commandBus = simpleCommandBus(() => unitOfWork(() => FROZEN))
    const queryBus = simpleQueryBus(() => unitOfWork(() => FROZEN))
    const eventScheduler = {
      async schedule(e: EventMessage, at: Date) {
        armed.push({ event: e, at })
        return { id: "s-1" }
      },
      async cancel() {
        return { kind: "not-found" } as const
      },
    }

    const openTicket = commandHandler(OpenTicket, async ({ payload }, ctx) => {
      await ctx.scheduleAfter(TicketOpened, { ticketId: payload.ticketId }, 60_000)
    })

    const app = kronos({
      commandHandlers: [
        { ...openTicket, eventStore, snapshotStore, commandBus, queryBus, eventScheduler },
      ],
    })
    try {
      await send(commandBus, OpenTicket, { ticketId: "t-1" })
      expect(armed).toHaveLength(1)
      expect(armed[0]!.at.getTime()).toBe(FROZEN + 60_000)
      expect(armed[0]!.event.timestamp).toBe(FROZEN)
    } finally {
      await app.stop()
    }
  })
})
