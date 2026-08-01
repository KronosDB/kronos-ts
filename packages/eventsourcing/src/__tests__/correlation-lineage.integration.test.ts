/**
 * Correlation lineage across the event-processor boundary.
 *
 * When an event handler (an automation) reacts to an event and dispatches a
 * command, the command — and any events it produces — must inherit the
 * triggering event's correlationId, with causationId pointing at the event.
 * This proves the chain spans command -> event -> processor -> command, with no
 * explicit correlation configuration (the framework default applies).
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, type Metadata } from "@kronos-ts/common"
import {
  command,
  event,
  commandHandler,
  eventHandler,
  EventCriteria,
  trackingProcessor,
  type EventMessage
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos } from "@kronos-ts/app"
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

    const running = await kronos({ quiet: true })
      .states(Student)
      .commands(enrollStudent, notifyRegistry)
      .processors(trackingProcessor("registry-automation").eventHandlers(onEnrolled).build())
      .start()
    try {
      // Originating command carries a correlationId from the edge (e.g. an HTTP boundary).
      await running.commandGateway.send(
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
      await running.stop()
    }
  })
})
