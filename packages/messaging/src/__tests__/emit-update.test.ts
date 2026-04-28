import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata, qualifiedNameToString } from "@kronos-ts/common"
import {
  runInNewUoW,
  NoActiveUnitOfWork,
  WrongUoWPhase,
  onPrepareCommit,
  query,
} from "../index.js"
import { setResource } from "../processing-state.js"
import { emitUpdate, QUERY_BUS_KEY } from "../emit-update.js"
import type { QueryBus } from "../query-bus.js"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const GetCourse = query({
  name: qn("university", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

// Minimal QueryBus mock (only emitUpdate is relevant here)
function makeMockQueryBus() {
  const emitUpdateSpy = mock((_name: string, _filter: (q: unknown) => boolean, _update: unknown) => {})
  const bus: QueryBus = {
    query: mock(() => Promise.resolve(null)),
    subscribe: mock(() => {}),
    emitUpdate: emitUpdateSpy,
    subscriptionQuery: mock(() => ({ initialResult: Promise.resolve(null), updates: { subscribe: mock(() => () => {}) } } as any)),
  }
  return { bus, emitUpdateSpy }
}

describe("emitUpdate", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", () => {
    expect(() => emitUpdate(GetCourse, () => true, { name: "Updated" })).toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(async () => {
        try {
          emitUpdate(GetCourse, () => true, { name: "Updated" })
        } catch (e) {
          capturedError = e
        }
      })
    })

    expect(capturedError).toBeInstanceOf(WrongUoWPhase)
    const err = capturedError as WrongUoWPhase
    expect(err.message).toContain("INVOCATION")
  })

  it("throws 'No query bus configured' when QUERY_BUS_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      expect(() => emitUpdate(GetCourse, () => true, { name: "Updated" })).toThrow(/query bus/)
    })
  })

  it("delegates to bus.emitUpdate with stringified queryName + filter + update", async () => {
    const { bus, emitUpdateSpy } = makeMockQueryBus()
    const filter = (q: { courseId: string }) => q.courseId === "c1"
    const update = { name: "Intro to TS" }

    await runInNewUoW(emptyMetadata(), async () => {
      setResource(QUERY_BUS_KEY, bus)
      emitUpdate(GetCourse, filter, update)
    })

    expect(emitUpdateSpy).toHaveBeenCalledTimes(1)
    const [queryName, passedFilter, passedUpdate] = emitUpdateSpy.mock.calls[0]!
    expect(queryName).toBe(qualifiedNameToString(GetCourse.name))
    expect(passedFilter).toBe(filter)
    expect(passedUpdate).toBe(update)
  })
})
