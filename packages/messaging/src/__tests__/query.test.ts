import { describe, expect, it } from "bun:test"
import { emptyMetadata, qn } from "@kronos-ts/common"
import { z } from "zod"
import { NoActiveUnitOfWork, onPrepareCommit, runInNewUoW, WrongUoWPhase } from "../index.js"
import { query as queryDescriptor } from "../descriptor.js"
import { query } from "../query.js"

// ---------------------------------------------------------------------------
// Test descriptor
// ---------------------------------------------------------------------------

const GetCourse = queryDescriptor({
  name: qn("university", "GetCourse"),
  payload: z.object({ courseId: z.string() }),
})

// ---------------------------------------------------------------------------
// `query` is the handler-internal consult helper — `ctx.query`. Its happy
// path (delegating to the query bus inside the active UnitOfWork, carrying
// caller metadata) is exercised end-to-end in the app package's ctx-query
// test, against a real bus. These unit tests pin what an e2e flow can't
// reach naturally: the UnitOfWork phase guards, mirroring send.test.ts.
// ---------------------------------------------------------------------------

describe("query — UnitOfWork guards", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", async () => {
    await expect(query(GetCourse, { courseId: "c1" })).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(async () => {
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

  it("throws 'No query bus configured' when QUERY_BUS_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(query(GetCourse, { courseId: "c1" })).rejects.toThrow(/query bus/)
    })
  })
})
