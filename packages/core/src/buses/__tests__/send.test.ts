import { describe, it, expect } from "bun:test"
import { z } from "zod"
import { qn } from "../../primitives/qualified-name.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { unitOfWork, NoActiveUnitOfWork, WrongUoWPhase, command } from "../../index.js"
import { sendFunction } from "../../handlers/ctx-send.js"

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

// ---------------------------------------------------------------------------
// `send` is the handler-internal command helper behind `ctx.send`. Its happy
// path — delegating to the command bus in a fresh UnitOfWork, carrying caller
// metadata + lineage, stamping identifier + timestamp — is exercised end-to-end
// by the stateful-automation integration tests. These unit tests pin the two
// things an e2e flow can't reach naturally: the UnitOfWork phase guards.
// ---------------------------------------------------------------------------

describe("send — UnitOfWork guards", () => {
  it("throws NoActiveUnitOfWork when its unit of work has closed", async () => {
    let send!: ReturnType<typeof sendFunction>
    await unitOfWork().execute(async (uow) => {
      send = sendFunction({ uow })
    })
    await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(
      NoActiveUnitOfWork,
    )
  })

  it("throws WrongUoWPhase outside the INVOCATION phase", async () => {
    const send = sendFunction({ uow: unitOfWork() })
    await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(
      WrongUoWPhase,
    )
  })

  it("throws WrongUoWPhase when called from onPrepareCommit", async () => {
    let capturedError: unknown = null

    await unitOfWork().execute(async (uow) => {
      const send = sendFunction({ uow })
      uow.onPrepareCommit(async () => {
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

  it("throws 'No command bus configured' when no bus was bound", async () => {
    await unitOfWork().execute(async (uow) => {
      const send = sendFunction({ uow })
      await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(
        /command bus/,
      )
    })
  })
})
