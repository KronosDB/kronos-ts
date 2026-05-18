/**
 * Plan 09-01 Task 3 — unskips the original Plan 08-04 deferred coverage.
 *
 * Original test (deleted with EventSourcingConfigurer.messaging(...)
 * legacy handler-registration chain in Plan 08-04) covered the full flow:
 *   command -> event store -> tracking processor -> projection -> query handler.
 *
 * Resolution path: kronos() exposes the full primitive set required —
 * `app.processors(trackingProcessor(...).build())` for the explicit
 * processor module surface.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, tag, emptyMetadata } from "@kronos-ts/common"
import {
  command,
  event,
  query,
  on,
  commandHandler,
  eventHandler,
  queryHandler,
  EventCriteria,
  trackingProcessor,
} from "@kronos-ts/messaging"
import { state } from "@kronos-ts/modelling"
import { kronos } from "@kronos-ts/core"
import { load, append } from "../index.js"

// ─── Domain ─────────────────────────────────────────────────────────────────

const EnrollStudent = command({
  name: qn("uni", "EnrollStudent"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
})
const StudentEnrolled = event({
  name: qn("uni", "StudentEnrolled"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
  tags: (p) => [tag("studentId", p.studentId)],
})
const GetStudent = query({
  name: qn("uni", "GetStudent"),
  payload: z.object({ studentId: z.string() }),
  result: z.object({ studentId: z.string(), name: z.string() }).optional(),
})

const Student = state({
  name: "Student",
  id: { studentId: z.string() },
  initial: () => ({ enrolled: false, name: "" }),
  criteria: ({ studentId }) => EventCriteria.havingTags(tag("studentId", studentId)),
  evolve: [on(StudentEnrolled, (s, e) => ({ enrolled: true, name: e.name }))],
})

const enrollStudent = commandHandler(EnrollStudent, async (cmd) => {
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

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Full flow: command -> event -> processor -> projection -> query", () => {
  it("command produces events, processor delivers to projection, query reads it", async () => {
    const view = new Map<string, { studentId: string; name: string }>()
    const onStudentEnrolled = eventHandler(StudentEnrolled, async (e) => {
      view.set(e.studentId, { studentId: e.studentId, name: e.name })
    })
    const getStudent = queryHandler(GetStudent, async (p, _metadata) => view.get(p.studentId))

    const running = await kronos({ quiet: true })
      .states(Student)
      .commands(enrollStudent)
      .queries(getStudent)
      .processors(
        trackingProcessor("student-projection")
          .eventHandlers(onStudentEnrolled)
          .build(),
      )
      .start()
    try {
      await running.commandGateway.send(
        EnrollStudent,
        { studentId: "s-1", name: "Alice" },
        emptyMetadata(),
      )
      await waitFor(() => view.has("s-1"))
      const result = await running.queryGateway.query(
        GetStudent,
        { studentId: "s-1" },
        emptyMetadata(),
      )
      expect(result).toEqual({ studentId: "s-1", name: "Alice" })
    } finally {
      await running.stop()
    }
  })

  it("multiple commands produce events that update the projection", async () => {
    const view = new Map<string, { studentId: string; name: string }>()
    const onStudentEnrolled = eventHandler(StudentEnrolled, async (e) => {
      view.set(e.studentId, { studentId: e.studentId, name: e.name })
    })

    const running = await kronos({ quiet: true })
      .states(Student)
      .commands(enrollStudent)
      .processors(
        trackingProcessor("student-projection")
          .eventHandlers(onStudentEnrolled)
          .build(),
      )
      .start()
    try {
      await running.commandGateway.send(EnrollStudent, { studentId: "s-1", name: "Alice" }, emptyMetadata())
      await running.commandGateway.send(EnrollStudent, { studentId: "s-2", name: "Bob" }, emptyMetadata())
      await running.commandGateway.send(EnrollStudent, { studentId: "s-3", name: "Carol" }, emptyMetadata())
      await waitFor(() => view.size === 3)
      expect(view.get("s-1")?.name).toBe("Alice")
      expect(view.get("s-2")?.name).toBe("Bob")
      expect(view.get("s-3")?.name).toBe("Carol")
    } finally {
      await running.stop()
    }
  })

  it("processor handles events across multiple command handler slices", async () => {
    // Second slice registered alongside the first to prove fan-in.
    const SecondCmd = command({
      name: qn("uni", "RenameStudent"),
      payload: z.object({ studentId: z.string(), newName: z.string() }),
    })
    const SecondEvent = event({
      name: qn("uni", "StudentRenamed"),
      payload: z.object({ studentId: z.string(), newName: z.string() }),
      tags: (p) => [tag("studentId", p.studentId)],
    })
    const renameStudent = commandHandler(SecondCmd, async (cmd) => {
      append(SecondEvent, { studentId: cmd.studentId, newName: cmd.newName })
    })

    const enrolled: string[] = []
    const renamed: string[] = []
    const onStudentEnrolled = eventHandler(StudentEnrolled, async (e) => {
      enrolled.push(e.studentId)
    })
    const onStudentRenamed = eventHandler(SecondEvent, async (e) => {
      renamed.push(e.studentId)
    })

    const running = await kronos({ quiet: true })
      .states(Student)
      .commands(enrollStudent, renameStudent)
      .processors(
        trackingProcessor("multi-slice-projection")
          .eventHandlers(onStudentEnrolled, onStudentRenamed)
          .build(),
      )
      .start()
    try {
      await running.commandGateway.send(EnrollStudent, { studentId: "m-1", name: "Mia" }, emptyMetadata())
      await running.commandGateway.send(SecondCmd, { studentId: "m-1", newName: "Maria" }, emptyMetadata())
      await waitFor(() => enrolled.includes("m-1") && renamed.includes("m-1"))
      expect(enrolled).toContain("m-1")
      expect(renamed).toContain("m-1")
    } finally {
      await running.stop()
    }
  })
})
