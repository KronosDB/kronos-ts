import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  runInNewUoW,
  NoActiveUnitOfWork,
  WrongUoWPhase,
  onPrepareCommit,
  command,
} from "../index.js"
import { send } from "../send.js"

// ---------------------------------------------------------------------------
// Test descriptor
// ---------------------------------------------------------------------------

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

// ---------------------------------------------------------------------------
// `send` is the handler-internal command helper. Its happy path — delegating
// to the command bus in a fresh UnitOfWork, carrying caller metadata, stamping
// identifier + timestamp — is exercised end-to-end by the stateful-automation
// integration tests (an event handler that reacts by calling `send()`), where
// it runs against a real bus and event store. These unit tests pin the two
// things an e2e flow can't reach naturally: the UnitOfWork phase guards.
// ---------------------------------------------------------------------------

describe("send — UnitOfWork guards", () => {
  it("throws NoActiveUnitOfWork when called outside a UoW", async () => {
    await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(NoActiveUnitOfWork)
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(async () => {
        try {
          await send(CreateCourse, { courseId: "c1", name: "Intro" })
        } catch (e) {
          capturedError = e
        }
      })
    })

    expect(capturedError).toBeInstanceOf(WrongUoWPhase)
    expect((capturedError as WrongUoWPhase).message).toContain("INVOCATION")
  })

  it("throws 'No command bus configured' when COMMAND_BUS_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(/command bus/)
    })
  })
})
