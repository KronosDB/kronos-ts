import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "../../messaging/messages.js"
import { z } from "zod"
import {
  unitOfWork,
  NoActiveUnitOfWork,
  WrongUoWPhase,
  query as queryDescriptor,
} from "../../index.js"
import { queryFunction } from "../query.js"

const GetCourse = queryDescriptor({
  name: qn("university", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

// ---------------------------------------------------------------------------
// `query` is the handler-internal consult helper — `ctx.query`. Its happy path
// (delegating to the query bus, NESTING in the caller's UnitOfWork, carrying
// caller metadata) is exercised end-to-end in the app package's ctx-query test
// against a real bus. These unit tests pin the phase guards, mirroring
// send.test.ts.
// ---------------------------------------------------------------------------

describe("query — UnitOfWork guards", () => {
  it("throws NoActiveUnitOfWork when its unit of work has closed", async () => {
    let query!: ReturnType<typeof queryFunction>
    await unitOfWork().execute(async (uow) => {
      query = queryFunction({ uow })
    })
    await expect(query(GetCourse, { courseId: "c1" })).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase outside the INVOCATION phase", async () => {
    const query = queryFunction({ uow: unitOfWork() })
    await expect(query(GetCourse, { courseId: "c1" })).rejects.toThrow(WrongUoWPhase)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const query = queryFunction({ uow })
      uow.onPrepareCommit(async () => {
        try {
          await query(GetCourse, { courseId: "c1" })
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
      const query = queryFunction({ uow })
      await expect(query(GetCourse, { courseId: "c1" })).rejects.toThrow(/query bus/)
    })
  })

  it("hands the caller's unit of work to the bus so the read nests", async () => {
    const seen: unknown[] = []
    await unitOfWork().execute(async (uow) => {
      const query = queryFunction({
        uow,
        queryBus: {
          query: async (_message: unknown, passedUow?: unknown) => {
            seen.push(passedUow)
            return null
          },
        } as never,
      })
      await query(GetCourse, { courseId: "c1" })
      expect(seen).toEqual([uow])
    })
  })
})
