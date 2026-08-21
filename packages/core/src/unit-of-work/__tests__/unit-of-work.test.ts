import { describe, expect, it } from "bun:test"
import { Phase, unitOfWork, requireInvocation, requireLive, NoActiveUnitOfWork, WrongUoWPhase, type UnitOfWork } from "../unit-of-work.js"

/**
 * The unit of work is handed to the action as a parameter. These tests assert
 * the contract that used to be split between the runner and the ALS-backed
 * processing state:
 *
 *   - phase ordering around the action, and late-registration draining
 *   - lifecycle hooks fire in their phase; onError/whenComplete are exclusive
 *   - the handle's own state: buffers, closed flag — and NOT correlation,
 *     which is composed on top by `correlating(unitOfWork())` and tested there
 *   - every `unitOfWork()` call mints a fresh one; each executes exactly once
 */
describe("unitOfWork — the () => UnitOfWork primitive", () => {
  describe("lifecycle", () => {
    it("executes action and returns its result", async () => {
      const result = await unitOfWork().execute(async () => 42)
      expect(result).toBe(42)
    })

    it("executes phases in correct order around the action", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        uow.on(Phase.PRE_INVOCATION, () => { log.push("pre") })
        uow.on(Phase.COMMIT, () => { log.push("commit") })
        uow.on(Phase.AFTER_COMMIT, () => { log.push("after") })
        log.push("handler")
      })
      // PRE_INVOCATION is already past by the time the action runs — late
      // registrations for an earlier phase are dropped. COMMIT and
      // AFTER_COMMIT still fire.
      expect(log).toEqual(["handler", "commit", "after"])
    })

    it("supports onPrepareCommit / onCommit / onAfterCommit", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        log.push("handler")
        uow.onPrepareCommit(() => { log.push("prepare") })
        uow.onCommit(() => { log.push("commit") })
        uow.onAfterCommit(() => { log.push("after") })
      })
      expect(log).toEqual(["handler", "prepare", "commit", "after"])
    })

    it("on(Phase.COMMIT, ...) is equivalent to onCommit", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        uow.on(Phase.COMMIT, () => { log.push("commit-via-on") })
        log.push("invocation")
      })
      expect(log).toEqual(["invocation", "commit-via-on"])
    })

    it("drains actions registered DURING their own phase, in the same phase", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        uow.onPrepareCommit(() => {
          log.push("prepare-1")
          // Registered while PREPARE_COMMIT is running — must still run in
          // PREPARE_COMMIT, before COMMIT.
          uow.onPrepareCommit(() => { log.push("prepare-2") })
        })
        uow.onCommit(() => { log.push("commit") })
      })
      expect(log).toEqual(["prepare-1", "prepare-2", "commit"])
    })

    it("runs onError on failure", async () => {
      const errors: unknown[] = []
      await expect(
        unitOfWork().execute(async (uow) => {
          uow.onError((err) => { errors.push(err) })
          throw new Error("boom")
        }),
      ).rejects.toThrow("boom")
      expect(errors).toHaveLength(1)
      expect((errors[0] as Error).message).toBe("boom")
    })

    it("reports the failing phase to onError", async () => {
      let failedPhase: number | undefined
      await expect(
        unitOfWork().execute(async (uow) => {
          uow.onError((_err, phase) => { failedPhase = phase })
          uow.onCommit(() => { throw new Error("commit-boom") })
        }),
      ).rejects.toThrow("commit-boom")
      expect(failedPhase).toBe(Phase.COMMIT)
    })

    it("runs whenComplete on success", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        uow.whenComplete(() => { log.push("complete") })
      })
      expect(log).toEqual(["complete"])
    })

    it("does NOT run whenComplete on failure", async () => {
      const log: string[] = []
      await expect(
        unitOfWork().execute(async (uow) => {
          uow.whenComplete(() => { log.push("complete") })
          uow.onError(() => { log.push("error") })
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual(["error"])
    })

    it("skips remaining phases on error", async () => {
      const log: string[] = []
      await expect(
        unitOfWork().execute(async (uow) => {
          uow.onCommit(() => { log.push("commit") })
          uow.onAfterCommit(() => { log.push("after") })
          log.push("handler")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual(["handler"])
    })

    it("propagates errors from the action", async () => {
      await expect(
        unitOfWork().execute(async () => {
          throw new Error("boom")
        }),
      ).rejects.toThrow("boom")
    })
  })

  describe("the handle", () => {
    it("reports INVOCATION as the phase while the action runs", async () => {
      await unitOfWork().execute(async (uow) => {
        expect(uow.phase).toBe(Phase.INVOCATION)
      })
    })

    it("is open during the action and closed after it completes", async () => {
      let captured!: UnitOfWork
      await unitOfWork().execute(async (uow) => {
        captured = uow
        expect(uow.closed).toBe(false)
      })
      expect(captured.closed).toBe(true)
    })

    it("is closed after a failed unit of work too", async () => {
      let captured!: UnitOfWork
      await expect(
        unitOfWork().execute(async (uow) => {
          captured = uow
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(captured.closed).toBe(true)
    })

    it("starts with empty buffers and an empty state cache", async () => {
      await unitOfWork().execute(async (uow) => {
        expect(uow.events.buffered).toEqual([])
        expect(uow.events.sourcingInfos).toEqual([])
        expect(uow.events.flushRegistered).toBe(false)
        expect(uow.stateCache.entries.size).toBe(0)
        expect(uow.stateCache.modules.size).toBe(0)
      })
    })

    it("buffers survive across phases within one unit of work", async () => {
      let seenAtPrepare = 0
      await unitOfWork().execute(async (uow) => {
        uow.events.buffered.push({ kind: "event" } as never)
        uow.onPrepareCommit(() => { seenAtPrepare = uow.events.buffered.length })
      })
      expect(seenAtPrepare).toBe(1)
    })

    it("has no correlation vocabulary at all — that is composed on top", async () => {
      // PURE TASK LIFECYCLE. Correlation is a carrying policy, and a policy is
      // not something a primitive is born knowing: `correlating(unitOfWork())`
      // is what adds the map, and `correlatingHandler(next, from)` is what
      // decides what goes in it. A host that never composes either has no way
      // to observe from here that the concept exists.
      await unitOfWork().execute(async (uow) => {
        expect("correlationData" in uow).toBe(false)
        expect("contributeCorrelationData" in uow).toBe(false)
        expect("attachCorrelationData" in uow).toBe(false)
      })
    })

    it("is not replaying by default", async () => {
      await unitOfWork().execute(async (uow) => {
        expect(uow.replaying).toBe(false)
      })
    })

    it("has no transaction vocabulary at all — that belongs to the adapter", async () => {
      // The handle is scoped to a task, not to a database. An adapter keeps its
      // transaction in its OWN table keyed by the unit of work and exports a
      // typed accessor pair for it; see `transaction.test.ts`.
      await unitOfWork().execute(async (uow) => {
        expect("transaction" in uow).toBe(false)
        expect("activeTransaction" in uow).toBe(false)
      })
    })
  })

  describe("transaction simulation via lifecycle hooks", () => {
    it("simulates commit/rollback ordering on success", async () => {
      const log: string[] = []
      await unitOfWork().execute(async (uow) => {
        uow.onCommit(() => { log.push("tx:commit") })
        uow.onError(() => { log.push("tx:rollback") })
        log.push("handler:work")
      })
      expect(log).toEqual(["handler:work", "tx:commit"])
    })

    it("rolls back on handler failure", async () => {
      const log: string[] = []
      await expect(
        unitOfWork().execute(async (uow) => {
          uow.onCommit(() => { log.push("tx:commit") })
          uow.onError(() => { log.push("tx:rollback") })
          log.push("handler:work")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual(["handler:work", "tx:rollback"])
    })

  })
})

/**
 * Nesting is no longer a runner's decision — there is no runner. A caller that
 * wants to nest passes its live handle to the seam (`bus.query(message, uow)`)
 * and the seam reuses it; a caller that wants a fresh task calls the factory.
 * What the primitive itself must guarantee is what these cover.
 */
describe("unitOfWork mints one fresh task per call", () => {
  it("creates a fresh unit of work per call, even when nested", async () => {
    let outer!: UnitOfWork
    let inner: UnitOfWork | undefined
    await unitOfWork().execute(async (uow) => {
      outer = uow
      await unitOfWork().execute(async (nested) => {
        inner = nested
      })
    })
    expect(inner).not.toBe(outer)
    expect(inner).toBeDefined()
  })

  it("takes no message — the message belongs to the binding, not the task", () => {
    // A clock is the ONLY thing a task takes, and it is optional: absent means
    // system time. Anything about a MESSAGE would be wrong here, because one
    // task can handle a whole batch of them.
    expect(unitOfWork.length).toBe(1)
    expect(unitOfWork()).not.toBe(unitOfWork())
  })

  it("reads `now` from the clock it was minted with; absent means system time", () => {
    expect(unitOfWork(() => 1_700_000_000_000).now()).toBe(1_700_000_000_000)

    let ticks = 0
    const uow = unitOfWork(() => ++ticks)
    expect(uow.now()).toBe(1)
    expect(uow.now()).toBe(2)

    const before = Date.now()
    const system = unitOfWork().now()
    expect(system).toBeGreaterThanOrEqual(before)
    expect(system).toBeLessThanOrEqual(Date.now())
  })

  it("executes exactly once — a second execute is a bug, not a re-run", async () => {
    const uow = unitOfWork()
    await uow.execute(async () => 1)
    expect(() => uow.execute(async () => 2)).toThrow("already been executed")
  })

  it("returns the action's result and propagates its errors", async () => {
    expect(await unitOfWork().execute(async () => 7)).toBe(7)
    await expect(
      unitOfWork().execute(async () => {
        throw new Error("boom-execute")
      }),
    ).rejects.toThrow("boom-execute")
  })

})

describe("guards", () => {
  it("requireInvocation passes during INVOCATION", async () => {
    await unitOfWork().execute(async (uow) => {
      expect(requireInvocation(uow)).toBe(uow)
    })
  })

  it("requireInvocation throws WrongUoWPhase from a lifecycle hook", async () => {
    let thrown: unknown
    await unitOfWork().execute(async (uow) => {
      uow.onCommit(() => {
        try { requireInvocation(uow) } catch (err) { thrown = err }
      })
    })
    expect(thrown).toBeInstanceOf(WrongUoWPhase)
    expect((thrown as WrongUoWPhase).currentPhase).toBe(Phase.COMMIT)
  })

  it("requireInvocation throws NoActiveUnitOfWork once the unit of work has closed", async () => {
    let captured!: UnitOfWork
    await unitOfWork().execute(async (uow) => { captured = uow })
    expect(() => requireInvocation(captured)).toThrow(NoActiveUnitOfWork)
  })

  it("requireLive tolerates any phase but not a closed unit of work", async () => {
    let captured!: UnitOfWork
    await unitOfWork().execute(async (uow) => {
      captured = uow
      uow.onCommit(() => { expect(requireLive(uow)).toBe(uow) })
    })
    expect(() => requireLive(captured)).toThrow(NoActiveUnitOfWork)
  })

  it("NoActiveUnitOfWork has a stable name and extends Error", () => {
    const err = new NoActiveUnitOfWork()
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe("NoActiveUnitOfWork")
  })
})

describe("unitOfWork() — a handle with no lifecycle driven", () => {
  it("is open, unphased, and usable as a test double", () => {
    const uow = unitOfWork()
    expect(uow.closed).toBe(false)
    expect(uow.phase).toBeNull()
    // Not in INVOCATION, so mutating capabilities refuse it.
    expect(() => requireInvocation(uow)).toThrow(WrongUoWPhase)
    expect(requireLive(uow)).toBe(uow)
  })
})
