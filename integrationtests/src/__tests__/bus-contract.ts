/**
 * THE ERROR CONTRACT OF A REMOTE BUS, against any transport.
 *
 * Two instances share one transport: handlers run on one, and the other sends
 * and queries. A handler that fails with a contract kind of the handling fails
 * the caller with the same kind; any other failure, a handler's own nested
 * no-handler included, reaches the caller without one, with its message. A name
 * nothing subscribed to fails with no-handler. Commands and queries alike.
 */
import assert from "node:assert/strict"
import { beforeAll, it } from "bun:test"
import {
  AppendConditionError,
  generateIdentifier,
  isConflict,
  isUnavailable,
  UnavailableError,
  isNoHandler,
  NoHandlerError,
  qn,
  type CommandBus,
  type CommandMessage,
  type QueryBus,
  type QueryMessage,
} from "@kronos-ts/core"

export type BusInstance = {
  readonly commandBus: CommandBus
  readonly queryBus: QueryBus
}

export type BusContractTarget = {
  /** The instance handlers subscribe on, and the one that calls them. Called once, before the checks. */
  readonly instances: () => Promise<{ readonly handling: BusInstance; readonly calling: BusInstance }>
}

type Outcome = "conflict" | "unavailable" | "nested-no-handler" | "plain"

function failWith(outcome: Outcome): never {
  if (outcome === "conflict") throw new AppendConditionError("lost the race")
  if (outcome === "unavailable") throw new UnavailableError("the store is down")
  if (outcome === "nested-no-handler") throw new NoHandlerError('No handler registered for command "bus-contract.Nested"')
  throw new RangeError("out of stock")
}

async function failureOf(work: Promise<unknown>): Promise<unknown> {
  return work.then(
    () => assert.fail("expected the call to fail"),
    (error: unknown) => error,
  )
}

export function busContract(target: BusContractTarget): void {
  const run = generateIdentifier().slice(0, 8)
  const name = (what: string) => qn("bus-contract", `${what}-${run}`)
  const command = (what: string): CommandMessage => ({
    kind: "command",
    identifier: generateIdentifier(),
    name: name(what),
    payload: {},
    metadata: {},
    timestamp: Date.now(),
  })
  const query = (what: string): QueryMessage => ({
    kind: "query",
    identifier: generateIdentifier(),
    name: name(what),
    payload: {},
    metadata: {},
    timestamp: Date.now(),
  })
  let calling: BusInstance

  beforeAll(async () => {
    const both = await target.instances()
    calling = both.calling
    const { commandBus, queryBus } = both.handling
    const ready = `bus-contract.ready-${run}`
    commandBus.subscribe(ready, async () => "ready")
    queryBus.subscribe(`bus-contract.ready-query-${run}`, async () => "ready")
    for (const outcome of ["conflict", "unavailable", "nested-no-handler", "plain"] as const) {
      commandBus.subscribe(`bus-contract.${outcome}-${run}`, async () => failWith(outcome))
      queryBus.subscribe(`bus-contract.${outcome}-query-${run}`, async () => failWith(outcome))
    }
    // A subscription reaches the other instance asynchronously: wait until both kinds route.
    const deadline = Date.now() + 30_000
    for (;;) {
      try {
        await calling.commandBus.dispatch(command("ready"))
        await calling.queryBus.query(query("ready-query"))
        return
      } catch (error) {
        if (Date.now() > deadline) throw error
        await new Promise((r) => setTimeout(r, 100))
      }
    }
  }, 60_000)

  it("fails a command with a conflict when its handler lost a race on the other instance", async () => {
    const failure = await failureOf(calling.commandBus.dispatch(command("conflict")))
    assert.ok(isConflict(failure), `expected a conflict, got ${String(failure)}`)
    assert.match((failure as Error).message, /lost the race/)
  })

  it("fails a command without a kind, with the handler's message, when its handler failed otherwise", async () => {
    const failure = await failureOf(calling.commandBus.dispatch(command("plain")))
    assert.ok(!isConflict(failure), `expected no kind, got ${String(failure)}`)
    assert.match((failure as Error).message, /out of stock/)
  })

  it("fails a query with a conflict when its handler lost a race on the other instance", async () => {
    const failure = await failureOf(calling.queryBus.query(query("conflict-query")))
    assert.ok(isConflict(failure), `expected a conflict, got ${String(failure)}`)
    assert.match((failure as Error).message, /lost the race/)
  })

  it("fails a query without a kind, with the handler's message, when its handler failed otherwise", async () => {
    const failure = await failureOf(calling.queryBus.query(query("plain-query")))
    assert.ok(!isConflict(failure), `expected no kind, got ${String(failure)}`)
    assert.match((failure as Error).message, /out of stock/)
  })

  it("fails a command as unavailable when its handler could not reach its store on the other instance", async () => {
    const failure = await failureOf(calling.commandBus.dispatch(command("unavailable")))
    assert.ok(isUnavailable(failure), `expected unavailable, got ${String(failure)}`)
    assert.ok(!isConflict(failure))
  })

  it("fails a query as unavailable when its handler could not reach its store on the other instance", async () => {
    const failure = await failureOf(calling.queryBus.query(query("unavailable-query")))
    assert.ok(isUnavailable(failure), `expected unavailable, got ${String(failure)}`)
  })

  it("does not call a handler's plain failure unavailable", async () => {
    assert.ok(!isUnavailable(await failureOf(calling.commandBus.dispatch(command("plain")))))
    assert.ok(!isUnavailable(await failureOf(calling.queryBus.query(query("plain-query")))))
  })

  it("fails a command nothing subscribed to with no-handler", async () => {
    const failure = await failureOf(calling.commandBus.dispatch(command("nobody")))
    assert.ok(isNoHandler(failure), `expected no-handler, got ${String(failure)}`)
  })

  it("fails a query nothing subscribed to with no-handler", async () => {
    const failure = await failureOf(calling.queryBus.query(query("nobody-query")))
    assert.ok(isNoHandler(failure), `expected no-handler, got ${String(failure)}`)
  })

  it("fails a command without a kind when its handler's own nested command had no handler", async () => {
    const failure = await failureOf(calling.commandBus.dispatch(command("nested-no-handler")))
    assert.ok(!isNoHandler(failure), `expected no kind, got ${String(failure)}`)
    assert.match((failure as Error).message, /bus-contract\.Nested/)
  })

  it("fails a query without a kind when its handler's own nested command had no handler", async () => {
    const failure = await failureOf(calling.queryBus.query(query("nested-no-handler-query")))
    assert.ok(!isNoHandler(failure), `expected no kind, got ${String(failure)}`)
    assert.match((failure as Error).message, /bus-contract\.Nested/)
  })
}
