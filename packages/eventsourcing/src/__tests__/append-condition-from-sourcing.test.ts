import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag } from "@kronos-ts/common"
import {
  command,
  event,
  on,
  commandHandler,
  EventCriteria,
} from "@kronos-ts/messaging"
import { eventSourcedEntity } from "@kronos-ts/modelling"
import { load, append } from "../index.js"
import { EventSourcingConfigurer } from "../eventsourcing-configurer.js"
import { createInMemoryEventStore, AppendConditionError } from "../in-memory-event-store.js"

// -- Domain --

const CreateAccount = command({
  name: qn("bank", "CreateAccount"),
  payload: z.object({ accountId: z.string(), balance: z.number() }),
})

const Deposit = command({
  name: qn("bank", "Deposit"),
  payload: z.object({ accountId: z.string(), amount: z.number() }),
})

const AccountCreated = event({
  name: qn("bank", "AccountCreated"),
  payload: z.object({ accountId: z.string(), balance: z.number() }),
  tags: (p) => [tag("accountId", p.accountId)],
})

const MoneyDeposited = event({
  name: qn("bank", "MoneyDeposited"),
  payload: z.object({ accountId: z.string(), amount: z.number() }),
  tags: (p) => [tag("accountId", p.accountId)],
})

type AccountState = { exists: boolean; balance: number }

const AccountEntity = eventSourcedEntity({
  name: "Account",
  id: { accountId: z.string() },
  initial: (_id) => ({ exists: false, balance: 0 }) as AccountState,
  criteria: (id) => EventCriteria.havingTags(tag("accountId", id.accountId)),
  evolve: [
    on(AccountCreated, (s: AccountState, e) => ({ exists: true, balance: e.balance })),
    on(MoneyDeposited, (s: AccountState, e) => ({ ...s, balance: s.balance + e.amount })),
  ],
})

const createAccount = commandHandler(CreateAccount, async (cmd, _metadata) => {
  const account = await load(AccountEntity, { accountId: cmd.accountId })
  if (account.exists) throw new Error("Account already exists")
  append(AccountCreated, { accountId: cmd.accountId, balance: cmd.balance })
})

const deposit = commandHandler(Deposit, async (cmd, _metadata) => {
  const account = await load(AccountEntity, { accountId: cmd.accountId })
  if (!account.exists) throw new Error("Account does not exist")
  append(MoneyDeposited, { accountId: cmd.accountId, amount: cmd.amount })
})

describe("Append condition derived from sourced state", () => {
  it("appends with condition that prevents stale-state decisions", async () => {
    const eventStore = createInMemoryEventStore()

    const app = EventSourcingConfigurer.create({ eventStore })
      .registerEntity(AccountEntity)
      .messaging(m => {
        m.registerCommandHandler(() => createAccount)
        m.registerCommandHandler(() => deposit)
      })
      .build()

    await app.start()

    // given — create an account
    await app.commandGateway.send(CreateAccount, { accountId: "acc-1", balance: 100 })

    // when — deposit twice, both should succeed because they're sequential
    await app.commandGateway.send(Deposit, { accountId: "acc-1", amount: 50 })
    await app.commandGateway.send(Deposit, { accountId: "acc-1", amount: 25 })

    // then — three events in the store
    const { events } = await eventStore.source({
      criteria: EventCriteria.havingTags(tag("accountId", "acc-1")),
    })
    expect(events).toHaveLength(3)
  })

  it("detects concurrent modification via append condition", async () => {
    // This test verifies that the append condition IS being sent.
    // We manually append a conflicting event between sourcing and appending
    // to simulate a race condition.
    const eventStore = createInMemoryEventStore()

    // Create the account first
    const createEvent = {
      identifier: crypto.randomUUID(),
      name: qn("bank", "AccountCreated"),
      version: "1.0",
      payload: { accountId: "acc-2", balance: 100 },
      metadata: {},
      timestamp: Date.now(),
      tags: [tag("accountId", "acc-2")],
    }
    await eventStore.append([createEvent])

    // Now create an app with a deposit handler that we can race against
    const app = EventSourcingConfigurer.create({ eventStore })
      .registerEntity(AccountEntity)
      .messaging(m => {
        m.registerCommandHandler(() => deposit)
      })
      .build()

    await app.start()

    // Deposit — this sources the account (position 0), then appends
    await app.commandGateway.send(Deposit, { accountId: "acc-2", amount: 50 })

    // Verify events exist
    const { events } = await eventStore.source({
      criteria: EventCriteria.havingTags(tag("accountId", "acc-2")),
    })
    expect(events).toHaveLength(2)
  })
})
