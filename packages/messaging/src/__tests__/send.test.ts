import { describe, it, expect, mock } from "bun:test"
import { z } from "zod"
import { qn, emptyMetadata } from "@kronos-ts/common"
import {
  runInNewUoW,
  NoActiveUnitOfWork,
  WrongUoWPhase,
  onPrepareCommit,
  command,
} from "../index.js"
import { setResource } from "../processing-state.js"
import { send, COMMAND_BUS_KEY } from "../send.js"
import type { CommandBus } from "../command-bus.js"

// ---------------------------------------------------------------------------
// Test descriptors
// ---------------------------------------------------------------------------

const CreateCourse = command({
  name: qn("university", "CreateCourse"),
  payload: z.object({ courseId: z.string(), name: z.string() }),
})

describe("send", () => {
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
    const err = capturedError as WrongUoWPhase
    expect(err.message).toContain("INVOCATION")
  })

  it("throws 'No command bus configured' when COMMAND_BUS_KEY is unset", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      await expect(send(CreateCourse, { courseId: "c1", name: "Intro" })).rejects.toThrow(/command bus/)
    })
  })

  it("delegates to bus.dispatch with descriptor name + payload + UoW metadata", async () => {
    const dispatchSpy = mock(() => Promise.resolve("ok"))
    const bus: CommandBus = { dispatch: dispatchSpy, subscribe: mock(() => {}) }
    const meta = { correlationId: "abc" } as any

    await runInNewUoW(meta, async () => {
      setResource(COMMAND_BUS_KEY, bus)
      await send(CreateCourse, { courseId: "c1", name: "Intro" })
    })

    expect(dispatchSpy).toHaveBeenCalledTimes(1)
    const arg = dispatchSpy.mock.calls[0]![0]
    expect(arg.name).toEqual(CreateCourse.name)
    expect(arg.payload).toEqual({ courseId: "c1", name: "Intro" })
    expect(arg.metadata).toEqual(meta)
  })

  it("dispatched command message has identifier and timestamp", async () => {
    const dispatchSpy = mock(() => Promise.resolve("ok"))
    const bus: CommandBus = { dispatch: dispatchSpy, subscribe: mock(() => {}) }

    await runInNewUoW(emptyMetadata(), async () => {
      setResource(COMMAND_BUS_KEY, bus)
      await send(CreateCourse, { courseId: "c1", name: "Intro" })
    })

    const arg = dispatchSpy.mock.calls[0]![0]
    expect(arg.identifier).toBeDefined()
    expect(typeof arg.timestamp).toBe("number")
  })
})
