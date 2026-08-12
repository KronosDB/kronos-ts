/**
 * Correlation lineage across the event-processor boundary.
 *
 * When an event handler (an automation) reacts to an event and dispatches a
 * command, the command — and any events it produces — must inherit the
 * triggering event's correlationId, with causationId pointing at the event.
 * This proves the chain spans command -> event -> processor -> command.
 *
 * NOTE ON COMPOSITION. Lineage across the processor boundary needs two pieces
 * wired together:
 *   1. the processor seeds correlation data from the triggering event
 *      (`correlationDataProviders: [messageOriginProvider()]`), and
 *   2. the command bus applies that data to outgoing commands
 *      (`correlationDataDispatchInterceptor()`).
 * `createApp` builds processors with `correlationDataProviders: []` and hands
 * out a bare `createSimpleCommandBus`, so it supplies neither — there is no
 * "framework default" for lineage any more. The test therefore composes the
 * automation explicitly on top of the app's components, which is what the
 * functional root asks for: the app owns the command side, the block below owns
 * the automation. The assertions are unchanged.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, type Metadata } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  eventHandler,
  correlationDataDispatchInterceptor,
  createInterceptingCommandBus,
  createTrackingEventProcessor,
  EventCriteria,
  messageOriginProvider,
  type EventMessage
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { createApp, inMemoryComponents, module } from "@kronos-ts/app"
import { append } from "../append.js"
import { load } from "../load.js"

const EnrollStudent = command({
  name: qn("uni", "EnrollStudent"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
})
const StudentEnrolled = event({
  name: qn("uni", "StudentEnrolled"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
  tags: (p) => [tag("studentId", p.studentId)],
})
const NotifyRegistry = command({
  name: qn("uni", "NotifyRegistry"),
  payload: z.object({ studentId: z.string() }),
})

const Student = state({
  name: "Student",
  id: { studentId: z.string() },
  initial: () => ({ enrolled: false, name: "" }),
  criteria: ({ studentId }) => EventCriteria.havingTags(tag("studentId", studentId)),
  evolve: (on) => [on(StudentEnrolled, (s, { payload: e }) => ({ enrolled: true, name: e.name }))],
})

const enrollStudent = commandHandler(EnrollStudent, async ({ payload: cmd }, ctx) => {
  const s = await load(Student, { studentId: cmd.studentId })
  if (s.enrolled) throw new Error("Already enrolled")
  append(StudentEnrolled, { studentId: cmd.studentId, name: cmd.name })
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

    // The app's components, with the command bus wrapped so correlation data
    // collected in the active UoW is applied to outgoing commands.
    const base = inMemoryComponents()
    const commandBus = createInterceptingCommandBus(base.commandBus)
    commandBus.registerDispatchInterceptor(correlationDataDispatchInterceptor())
    const components = { ...base, commandBus }

    const app = createApp({
      components,
      modules: [module("uni", Student, enrollStudent, notifyRegistry)],
    })

    // The automation, composed by hand on the app's components so it can seed
    // lineage from the triggering event.
    const automation = createTrackingEventProcessor({
      name: "registry-automation",
      eventSource: components.eventStore as never,
      eventHandlers: [onEnrolled],
      stateManager: app.stateManagers.get("uni"),
      commandBus,
      queryBus: components.queryBus,
      correlationDataProviders: [messageOriginProvider()],
      unitOfWorkRunner: components.unitOfWorkFactory,
      tokenStore: components.tokenStore,
    })
    await automation.start()

    try {
      // Originating command carries a correlationId from the edge (e.g. an HTTP boundary).
      await app.commandGateway.send(
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
      automation.stop()
      await app.stop()
    }
  })
})
