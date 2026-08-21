/**
 * correlation across the event-processor boundary.
 *
 * When an event handler (an automation) reacts to an event and dispatches a
 * command, the command — and any events it produces — must inherit the
 * triggering event's correlationId, with causationId pointing at the event.
 * This proves the chain spans command -> event -> processor -> command.
 *
 * NOTE ON COMPOSITION. Nothing in core carries anything. This host COMPOSES the
 * mechanism, in the two places a host composes it:
 *
 *   - its tasks are `correlating(unitOfWork())`, so they can carry a map;
 *   - every handler is `correlatingHandler(h, correlationFrom)`, so each
 *     invocation attaches its own message's cargo and overlays it onto what it
 *     gives birth to.
 *
 * That is the WHOLE mechanism, and it is uniform: the command leg and the event
 * leg are the same wrapper doing the same thing, because an automation reacting
 * to an event is a handler handling a message like any other. Take the wrapper
 * off and every assertion below goes to `undefined` — which is the point of it
 * being opt-in.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, type Metadata } from "../../messaging/messages.js"
import { command, event, commandHandler, eventHandler, eventProcessor, correlating, correlatingHandler, send, unitOfWork, localCommandBus, localQueryBus, inMemoryTokenStore, type EventMessage } from "../../index.js"
import { state } from "../../event-sourcing/state.js"
import { kronos } from "../../kronos.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"

// The id-pair cargo, written out as any host writes it: the chain is inherited
// or seeded; the cause is the parent, unconditionally.
const correlationFrom = (parent: Message): Metadata => ({
  correlationId: String(parent.metadata.correlationId ?? parent.identifier),
  causationId: String(parent.identifier),
})

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `localCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => ReturnType<typeof correlating>) {
  return {
    commandBus: localCommandBus(uow),
    queryBus: localQueryBus(uow),
  }
}

/** What a host does to a handler to make it carry. One line, one cargo choice. */
const carrying = <H extends { handler: any }>(h: H): H => ({
  ...h,
  handler: correlatingHandler(h.handler, correlationFrom),
})

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
  id: { studentId: z.string() },
  tags: ({ studentId }) => ({ studentId: studentId }),
  evolve: [() => ({ enrolled: false, name: "" }), [StudentEnrolled, (s, { payload: e }) => ({ enrolled: true, name: e.name })]],
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

describe("correlation: command -> event -> processor -> command", () => {
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

    const uow = () => correlating(unitOfWork())
    const buses = inMemoryBuses(uow)
    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()

    const app = kronos({
      commandHandlers: [
        { ...carrying(enrollStudent), ...buses, eventStore },
        { ...carrying(notifyRegistry), ...buses, eventStore },
      ],
      // The automation is an ORDINARY entry now — one of the four lists — and
      // the processor it names is a value the host built.
      eventHandlers: [
        {
          ...carrying(onEnrolled),
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
      // by the triggering EVENT — not by the command that appended it. That is
      // the hop rule: `correlationFrom` reads causation off the parent's
      // identifier, unconditionally, so the causal graph is a chain you can
      // walk one link at a time.
      expect(notifyMetadata?.correlationId).toBe("corr-root")
      expect(notifyMetadata?.causationId).toBe(triggeringEvent?.identifier)
    } finally {
      await app.stop()
    }
  })
})
