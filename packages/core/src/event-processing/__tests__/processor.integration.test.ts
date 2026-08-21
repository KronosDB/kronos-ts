/**
 * The full flow:
 *   command -> event store -> tracking processor -> projection -> query handler.
 *
 * Composition: one flat handler list carries the state, the command
 * handlers, the query handler and the processor module — each is told apart
 * by the `kind` it already carries, so there are no typed buckets. Every
 * handler below shares one `eventStore` object, so `kronos` groups them into
 * one repository set / state manager.
 */
import { describe, expect, it } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "../../messaging/messages.js"
import { command, event, query, commandHandler, eventHandler, eventProcessor, queryHandler, inMemoryTokenStore, correlation, interceptingCommandBus, interceptingQueryBus, send, unitOfWork, localCommandBus, localQueryBus, type UnitOfWork } from "../../index.js"
import { state } from "../../event-sourcing/state.js"
import { kronos } from "../../kronos.js"
import { inMemoryEventStore } from "../../event-sourcing/in-memory.js"

/**
 * The two things `kronos` needs that are not handlers. The UoW runner is
 * named once and handed to `localCommandBus` (which captures it at
 * construction) — writing it on an adjacent line is what makes that checkable.
 */
function inMemoryBuses(uow: () => UnitOfWork = unitOfWork) {
  return {
    commandBus: interceptingCommandBus(localCommandBus(uow), correlation),
    queryBus: interceptingQueryBus(localQueryBus(uow), correlation),
  }
}

// ─── Domain ─────────────────────────────────────────────────────────────────

const EnrollStudent = command({
  name: qn("uni", "EnrollStudent"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
})
const StudentEnrolled = event({
  name: qn("uni", "StudentEnrolled"),
  payload: z.object({ studentId: z.string(), name: z.string() }),
  tags: { studentId: (p) => p.studentId },
})
const GetStudent = query({
  name: qn("uni", "GetStudent"),
  payload: z.object({ studentId: z.string() }),
  result: z.object({ studentId: z.string(), name: z.string() }).optional(),
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

// ─── Tests ──────────────────────────────────────────────────────────────────

describe("Full flow: command -> event -> processor -> projection -> query", () => {
  it("command produces events, processor delivers to projection, query reads it", async () => {
    const view = new Map<string, { studentId: string; name: string }>()
    const onStudentEnrolled = eventHandler(StudentEnrolled, async ({ payload: e }) => {
      view.set(e.studentId, { studentId: e.studentId, name: e.name })
    })
    const getStudent = queryHandler(GetStudent, async ({ payload: p }) => view.get(p.studentId))

    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...enrollStudent, ...buses, eventStore }],
      queryHandlers: [{ ...getStudent, ...buses, eventStore }],
      eventHandlers: [
        {
          ...onStudentEnrolled,
          ...buses,
          processor: eventProcessor({
            name: "student-projection",
            eventStore,
            tokenStore,
            unitOfWork,
          }),
        },
      ],
    })
    try {
      await send(buses.commandBus, 
        EnrollStudent,
        { studentId: "s-1", name: "Alice" },
        emptyMetadata(),
      )
      await waitFor(() => view.has("s-1"))
      const result = await query(buses.queryBus, 
        GetStudent,
        { studentId: "s-1" },
        emptyMetadata(),
      )
      expect(result).toEqual({ studentId: "s-1", name: "Alice" })
    } finally {
      await app.stop()
    }
  })

  it("multiple commands produce events that update the projection", async () => {
    const view = new Map<string, { studentId: string; name: string }>()
    const onStudentEnrolled = eventHandler(StudentEnrolled, async ({ payload: e }) => {
      view.set(e.studentId, { studentId: e.studentId, name: e.name })
    })

    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()
    const buses = inMemoryBuses()
    const app = kronos({
      commandHandlers: [{ ...enrollStudent, ...buses, eventStore }],
      eventHandlers: [
        {
          ...onStudentEnrolled,
          ...buses,
          processor: eventProcessor({
            name: "student-projection",
            eventStore,
            tokenStore,
            unitOfWork,
          }),
        },
      ],
    })
    try {
      await send(buses.commandBus, EnrollStudent, { studentId: "s-1", name: "Alice" }, emptyMetadata())
      await send(buses.commandBus, EnrollStudent, { studentId: "s-2", name: "Bob" }, emptyMetadata())
      await send(buses.commandBus, EnrollStudent, { studentId: "s-3", name: "Carol" }, emptyMetadata())
      await waitFor(() => view.size === 3)
      expect(view.get("s-1")?.name).toBe("Alice")
      expect(view.get("s-2")?.name).toBe("Bob")
      expect(view.get("s-3")?.name).toBe("Carol")
    } finally {
      await app.stop()
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
      tags: { studentId: (p) => p.studentId },
    })
    const renameStudent = commandHandler(SecondCmd, async ({ payload: cmd }, ctx) => {
      ctx.append(SecondEvent, { studentId: cmd.studentId, newName: cmd.newName })
    })

    const enrolled: string[] = []
    const renamed: string[] = []
    const onStudentEnrolled = eventHandler(StudentEnrolled, async ({ payload: e }) => {
      enrolled.push(e.studentId)
    })
    const onStudentRenamed = eventHandler(SecondEvent, async ({ payload: e }) => {
      renamed.push(e.studentId)
    })

    const eventStore = inMemoryEventStore()
    const tokenStore = inMemoryTokenStore()
    const buses = inMemoryBuses()
    const multiSlice = eventProcessor({
      name: "multi-slice-projection",
      eventStore,
      tokenStore,
      unitOfWork,
    })
    const app = kronos({
      commandHandlers: [
        { ...enrollStudent, ...buses, eventStore },
        { ...renameStudent, ...buses, eventStore },
      ],
      // Both handlers name ONE processor value — one cursor, one delivery.
      eventHandlers: [
        { ...onStudentEnrolled, ...buses, processor: multiSlice },
        { ...onStudentRenamed, ...buses, processor: multiSlice },
      ],
    })
    try {
      await send(buses.commandBus, EnrollStudent, { studentId: "m-1", name: "Mia" }, emptyMetadata())
      await send(buses.commandBus, SecondCmd, { studentId: "m-1", newName: "Maria" }, emptyMetadata())
      await waitFor(() => enrolled.includes("m-1") && renamed.includes("m-1"))
      expect(enrolled).toContain("m-1")
      expect(renamed).toContain("m-1")
    } finally {
      await app.stop()
    }
  })
})
