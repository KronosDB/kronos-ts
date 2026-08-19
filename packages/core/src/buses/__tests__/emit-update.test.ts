import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, qualifiedNameToString } from "../../primitives/qualified-name.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { unitOfWork, NoActiveUnitOfWork, WrongUoWPhase, query } from "../../index.js"
import { emitUpdateFunction } from "../emit-update.js"
import type { QueryBus } from "../query-bus.js"

const GetCourse = query({
  name: qn("university", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

// Minimal QueryBus mock (only emitUpdate is relevant here)
function makeMockQueryBus() {
  const emitUpdateSpy = mock(
    (_name: string, _filter: (q: unknown) => boolean, _update: unknown, _uow?: unknown) => {},
  )
  const bus: QueryBus = {
    query: mock(() => Promise.resolve(null)),
    subscribe: mock(() => {}),
    emitUpdate: emitUpdateSpy,
    subscriptionQuery: mock(() => ({ initialResult: Promise.resolve(null), updates: { subscribe: mock(() => () => {}) } } as any)),
  } as unknown as QueryBus
  return { bus, emitUpdateSpy }
}

describe("emitUpdate", () => {
  it("throws NoActiveUnitOfWork when its unit of work has closed", async () => {
    let emitUpdate!: ReturnType<typeof emitUpdateFunction>
    await unitOfWork().execute(async (uow) => {
      emitUpdate = emitUpdateFunction({ uow })
    })
    expect(() => emitUpdate(GetCourse, () => true, { name: "Updated" })).toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase outside the INVOCATION phase", () => {
    const emitUpdate = emitUpdateFunction({ uow: unitOfWork() })
    expect(() => emitUpdate(GetCourse, () => true, { name: "Updated" })).toThrow(WrongUoWPhase)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const emitUpdate = emitUpdateFunction({ uow })
      uow.onPrepareCommit(async () => {
        try {
          emitUpdate(GetCourse, () => true, { name: "Updated" })
        } catch (e) {
          capturedError = e
        }
      })
    })

    expect(capturedError).toBeInstanceOf(WrongUoWPhase)
    expect((capturedError as WrongUoWPhase).message).toContain("INVOCATION")
  })

  it("throws 'No query bus configured' when no bus was bound", async () => {
    await unitOfWork().execute(async (uow) => {
      const emitUpdate = emitUpdateFunction({ uow })
      expect(() => emitUpdate(GetCourse, () => true, { name: "Updated" })).toThrow(/query bus/)
    })
  })

  it("delegates to bus.emitUpdate with stringified queryName + filter + update + uow", async () => {
    const { bus, emitUpdateSpy } = makeMockQueryBus()
    const filter = (q: { courseId: string }) => q.courseId === "c1"
    const update = { name: "Intro to TS" }
    let captured: unknown

    await unitOfWork().execute(async (uow) => {
      captured = uow
      emitUpdateFunction({ uow, queryBus: bus })(GetCourse, filter, update)
    })

    expect(emitUpdateSpy).toHaveBeenCalledTimes(1)
    const [queryName, passedFilter, passedUpdate, passedUow] = emitUpdateSpy.mock.calls[0]!
    expect(queryName).toBe(qualifiedNameToString(GetCourse.name))
    expect(passedFilter).toBe(filter)
    expect(passedUpdate).toBe(update)
    expect(passedUow).toBe(captured)
  })
})
