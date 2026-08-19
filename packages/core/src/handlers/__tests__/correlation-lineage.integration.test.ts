/**
 * Correlation lineage across the event-processor boundary.
 *
 * When an event handler (an automation) reacts to an event and dispatches a
 * command, the command — and any events it produces — must inherit the
 * triggering event's correlationId, with causationId pointing at the event.
 * This proves the chain spans command -> event -> processor -> command.
 *
 * NOTE ON COMPOSITION. There is no CorrelationDataProvider seam any more, and
 * nothing is configured here. `ctx` carries the HANDLED MESSAGE'S metadata
 * outward on `send` / `query` / `append` — uniformly, command leg and event leg
 * alike — and the processor stamps the lineage rule from the event's own
 * identifier so an automation's outgoing command is caused by the event that
 * triggered it. The buses below are deliberately BARE: everything asserted here
 * falls out of that one mechanism.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { type Metadata } from "../../primitives/metadata.js"
import { command, event, commandHandler, eventHandler, eventProcessor, interceptingCommandBus, interceptingQueryBus, send, unitOfWork, simpleCommandBus, simpleQueryBus, inMemoryTokenStore, type EventMessage, type UnitOfWork } from "../../index.js"
import { state } from "../../state/state.js"
import { kronos } from "../../assembly/kronos.js"
import { inMemoryEventStore } from "../../stores/in-memory-event-store.js"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `simpleCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: simpleCommandBus(uow),
    queryBus: simpleQueryBus(uow),
  }
}

const EnrollStudent = command({
  name: qn("uni", "EnrollStudent"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
})
const StudentEnrolled = event({
  name: qn("uni", "StudentEnrolled"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
  tags: { studentId: (p) => p.studentId },
})
const NotifyRegistry = command({
  name: qn("uni", "NotifyRegistry"),
  payload: z.object({ studentId: z.string() }),
})

const Student = state({
  name: "Student",
  id: { studentId: z.string() },
  initial: () => ({ enrolled: false, name: "" }),
  tags: ({ studentId }) => ({ studentId: studentId }),
  evolve: [[StudentEnrolled, (s, { payload: e }) => ({ enrolled: true, name: e.name })]],
})

const enrollStudent = commandHandler(EnrollStudent, async ({ payload: cmd }, ctx) => {
  const s = await ctx.load(Student, { studentId: cmd.studentId })
  if (s.enrolled) throw new Error("Already enrolled")
  ctx.append(StudentEnrolled, { studentId: cmd.studentId, name: cmd.name })
})

async function waitFor(check: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (Date.now() - start < timeoutMs) {
    if (check()) return
    await new Promise((r) => setTimeout(r, 20))
  }
  throw new Error("Timed out")
}

describe("Correlation lineage: command -> event -> processor -> command", () => {
  it("an event handler's dispatched command inherits the event's correlationId and is caused by the event", async () => {
    let triggeringEvent: EventMessage | undefined
    let notifyMetadata: Metadata | undefined

    // Automation: react to StudentEnrolled, dispatch a follow-up command.
    const onEnrolled = eventHandler(StudentEnrolled, async (message, ctx) => {
      triggeringEvent = message
      await ctx.send(NotifyRegistry, { studentId: message.payload.studentId })
    })

    const notifyRegistry = commandHandler(NotifyRegistry, async ({ metadata }, ctx) => {
      notifyMetadata = metadata
    })

    const uow = unitOfWork
    const buses = inMemoryBuses(uow)
    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()

    const app = kronos({
      states: [{ ...Student, eventStore }],
      commandHandlers: [
        { ...enrollStudent, ...buses, eventStore },
        { ...notifyRegistry, ...buses, eventStore },
      ],
      // The automation is an ORDINARY entry now — one of the four lists — and
      // the processor it names is a value the host built.
      eventHandlers: [
        {
          ...onEnrolled,
          ...buses,
          processor: eventProcessor({
            name: "registry-automation",
            eventStore,
            tokenStore,
            unitOfWork: uow,
          }),
        },
      ],
    })

    try {
      // Originating command carries a correlationId from the edge (e.g. an HTTP boundary).
      await send(
        buses.commandBus,
        EnrollStudent,
        { studentId: "s-1", name: "Alice" },
        { correlationId: "corr-root" },
      )

      await waitFor(() => notifyMetadata !== undefined)

      // The appended event inherited the originating command's correlationId.
      expect(triggeringEvent?.metadata.correlationId).toBe("corr-root")

      // The command dispatched from the event handler inherits the same
      // correlationId (the chain spans the automation boundary) and is caused
      // by the triggering event.
      expect(notifyMetadata?.correlationId).toBe("corr-root")
      expect(notifyMetadata?.causationId).toBe(triggeringEvent?.identifier)
    } finally {
      await app.stop()
    }
  })
})
