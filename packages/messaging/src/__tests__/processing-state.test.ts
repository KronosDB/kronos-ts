import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { runInNewUoW } from "../unit-of-work.js"
import {
  processingStateStorage,
  NoActiveUnitOfWork,
  Phase,
  getResource,
  setResource,
  computeIfAbsent,
  removeResource,
  hasResource,
  updateResource,
  registerPhaseAction,
  registerErrorHandler,
  registerCompleteHandler,
  withOverride,
  on,
  onPrepareCommit,
  onCommit,
  onAfterCommit,
  onError,
  whenComplete,
} from "../processing-state.js"

function buildState() {
  return {
    resources: new Map<symbol, unknown>(),
    phaseActions: new Map<number, Array<() => Promise<void> | void>>(),
    errorHandlers: [] as Array<(e: unknown, p?: number) => Promise<void> | void>,
    completeHandlers: [] as Array<() => void>,
    currentPhase: null,
    status: "not_started" as const,
    metadata: emptyMetadata(),
  } as unknown as Parameters<typeof processingStateStorage.run>[0]
}

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

describe("NoActiveUnitOfWork", () => {
  it("getResource throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => getResource(key)).toThrow(NoActiveUnitOfWork)
  })

  it("setResource throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => setResource(key, "v")).toThrow(NoActiveUnitOfWork)
  })

  it("computeIfAbsent throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => computeIfAbsent(key, () => "x")).toThrow(NoActiveUnitOfWork)
  })

  it("registerPhaseAction throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => registerPhaseAction(Phase.COMMIT, () => {})).toThrow(NoActiveUnitOfWork)
  })

  it("registerErrorHandler throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => registerErrorHandler(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("registerCompleteHandler throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => registerCompleteHandler(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("removeResource throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => removeResource(key)).toThrow(NoActiveUnitOfWork)
  })

  it("hasResource throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => hasResource(key)).toThrow(NoActiveUnitOfWork)
  })

  it("updateResource throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    expect(() => updateResource(key, (current) => current ?? "x")).toThrow(NoActiveUnitOfWork)
  })

  it("withOverride throws NoActiveUnitOfWork when no UoW is active", () => {
    const key = resourceKey<string>("k")
    // withOverride calls requireState() synchronously before returning a Promise,
    // so the error surfaces as a synchronous throw — not as a promise rejection.
    expect(() => withOverride(key, "v", async () => "x")).toThrow(NoActiveUnitOfWork)
  })

  it("NoActiveUnitOfWork has stable name and extends Error", () => {
    const key = resourceKey<string>("k")
    try {
      getResource(key)
      throw new Error("should have thrown")
    } catch (err) {
      expect(err).toBeInstanceOf(Error)
      expect(err).toBeInstanceOf(NoActiveUnitOfWork)
      expect((err as NoActiveUnitOfWork).name).toBe("NoActiveUnitOfWork")
    }
  })
})

describe("accessors inside storage.run", () => {
  it("setResource returns undefined on first call and previous value on second", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      expect(setResource(key, "v1")).toBeUndefined()
      expect(setResource(key, "v2")).toBe("v1")
    })
  })

  it("getResource returns the value set by setResource (round-trip)", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "v")
      expect(getResource(key)).toBe("v")
    })
  })

  it("computeIfAbsent calls supplier exactly once on first miss, not on second", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      let calls = 0
      const supplier = () => {
        calls++
        return "x"
      }
      expect(computeIfAbsent(key, supplier)).toBe("x")
      expect(calls).toBe(1)
      expect(computeIfAbsent(key, supplier)).toBe("x")
      expect(calls).toBe(1)
    })
  })

  it("registerPhaseAction appends to state.phaseActions under the correct phase", () => {
    const state = buildState() as unknown as {
      phaseActions: Map<number, Array<() => void>>
    } & Parameters<typeof processingStateStorage.run>[0]
    processingStateStorage.run(state, () => {
      const action = () => {}
      registerPhaseAction(Phase.COMMIT, action)
      const bucket = state.phaseActions.get(Phase.COMMIT)
      expect(bucket).toBeDefined()
      expect(bucket!.length).toBe(1)
      expect(bucket![0]).toBe(action)
    })
  })

  it("registerErrorHandler appends to state.errorHandlers", () => {
    const state = buildState() as unknown as {
      errorHandlers: Array<(e: unknown, p?: number) => Promise<void> | void>
    } & Parameters<typeof processingStateStorage.run>[0]
    processingStateStorage.run(state, () => {
      const handler = () => {}
      registerErrorHandler(handler)
      expect(state.errorHandlers.length).toBe(1)
      expect(state.errorHandlers[0]).toBe(handler)
    })
  })

  it("registerCompleteHandler appends to state.completeHandlers", () => {
    const state = buildState() as unknown as {
      completeHandlers: Array<() => void>
    } & Parameters<typeof processingStateStorage.run>[0]
    processingStateStorage.run(state, () => {
      const handler = () => {}
      registerCompleteHandler(handler)
      expect(state.completeHandlers.length).toBe(1)
      expect(state.completeHandlers[0]).toBe(handler)
    })
  })
})

describe("withOverride", () => {
  it("resolves to the overridden value inside fn", async () => {
    const state = buildState()
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      const result = await withOverride(key, "override", async () => getResource(key))
      expect(result).toBe("override")
    })
  })

  it("parent's resource value is unchanged after withOverride returns", async () => {
    const state = buildState()
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      setResource(key, "parent")
      const inner = await withOverride(key, "override", async () => getResource(key))
      expect(inner).toBe("override")
      expect(getResource(key)).toBe("parent")
    })
  })

  it("forked resource Map isolates sibling writes (writes to other keys do NOT leak)", async () => {
    const state = buildState()
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      const otherKey = resourceKey<string>("other")
      setResource(key, "parent")

      await withOverride(key, "child", async () => {
        setResource(otherKey, "marker")
        expect(getResource(otherKey)).toBe("marker")
        expect(getResource(key)).toBe("child")
      })

      // After return: parent's resources Map is untouched — fork did not leak.
      expect(getResource(key)).toBe("parent")
      expect(getResource(otherKey)).toBeUndefined()
    })
  })

  it("lifecycle registrations inside withOverride DO leak to parent (phase actions)", async () => {
    const state = buildState() as unknown as {
      phaseActions: Map<number, Array<() => void>>
    } & Parameters<typeof processingStateStorage.run>[0]
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      const action = () => {}
      await withOverride(key, "child", async () => {
        registerPhaseAction(Phase.COMMIT, action)
      })
      const bucket = state.phaseActions.get(Phase.COMMIT)
      expect(bucket).toBeDefined()
      expect(bucket!.length).toBeGreaterThanOrEqual(1)
      expect(bucket!).toContain(action)
    })
  })

  it("lifecycle registrations inside withOverride DO leak to parent (error handlers)", async () => {
    const state = buildState() as unknown as {
      errorHandlers: Array<(e: unknown, p?: number) => Promise<void> | void>
    } & Parameters<typeof processingStateStorage.run>[0]
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      const handler = () => {}
      await withOverride(key, "child", async () => {
        registerErrorHandler(handler)
      })
      expect(state.errorHandlers.length).toBeGreaterThanOrEqual(1)
      expect(state.errorHandlers).toContain(handler)
    })
  })

  it("lifecycle registrations inside withOverride DO leak to parent (complete handlers)", async () => {
    const state = buildState() as unknown as {
      completeHandlers: Array<() => void>
    } & Parameters<typeof processingStateStorage.run>[0]
    await processingStateStorage.run(state, async () => {
      const key = resourceKey<string>("k")
      const handler = () => {}
      await withOverride(key, "child", async () => {
        registerCompleteHandler(handler)
      })
      expect(state.completeHandlers.length).toBeGreaterThanOrEqual(1)
      expect(state.completeHandlers).toContain(handler)
    })
  })

  it("parallel withOverride calls are isolated (no cross-leakage between awaits)", async () => {
    const state = buildState()
    const results = await processingStateStorage.run(state, async () => {
      const k = resourceKey<string>("k")
      return Promise.all([
        withOverride(k, "a", async () => {
          await sleep(10)
          return getResource(k)
        }),
        withOverride(k, "b", async () => {
          await sleep(5)
          return getResource(k)
        }),
      ])
    })
    expect(results).toEqual(["a", "b"])
  })

  it("withOverride propagates the return value from fn", async () => {
    const state = buildState()
    const result = await processingStateStorage.run(state, async () => {
      const k = resourceKey<number>("n")
      return withOverride(k, 1, async () => 42)
    })
    expect(result).toBe(42)
  })
})

describe("removeResource", () => {
  it("returns undefined when key absent and leaves Map size unchanged", () => {
    const state = buildState() as unknown as {
      resources: Map<symbol, unknown>
    } & Parameters<typeof processingStateStorage.run>[0]
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("absent")
      const sizeBefore = state.resources.size
      expect(removeResource(key)).toBeUndefined()
      expect(state.resources.size).toBe(sizeBefore)
    })
  })

  it("returns previous value and removes the entry when key present", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "v")
      expect(removeResource(key)).toBe("v")
      expect(getResource(key)).toBeUndefined()
    })
  })

  it("subsequent removeResource for the same key returns undefined", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "v")
      expect(removeResource(key)).toBe("v")
      expect(removeResource(key)).toBeUndefined()
    })
  })
})

describe("hasResource", () => {
  it("returns false when key absent", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("absent")
      expect(hasResource(key)).toBe(false)
    })
  })

  it("returns true when key present", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "v")
      expect(hasResource(key)).toBe(true)
    })
  })

  it("returns true even when stored value is undefined (key membership semantics)", () => {
    const state = buildState() as unknown as {
      resources: Map<symbol, unknown>
    } & Parameters<typeof processingStateStorage.run>[0]
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      // Bypass setResource (which can't write undefined cleanly via its return-prev contract)
      // and set the symbol directly to undefined to assert Map-membership semantics.
      state.resources.set(key.symbol, undefined)
      expect(hasResource(key)).toBe(true)
    })
  })

  it("returns false after removeResource removes the key", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "v")
      expect(hasResource(key)).toBe(true)
      removeResource(key)
      expect(hasResource(key)).toBe(false)
    })
  })
})

describe("updateResource", () => {
  it("calls updater with undefined when key absent and stores returned value", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      let received: string | undefined = "sentinel"
      const result = updateResource(key, (current) => {
        received = current
        return "new"
      })
      expect(received).toBeUndefined()
      expect(result).toBe("new")
      expect(getResource(key)).toBe("new")
    })
  })

  it("calls updater with current value when key present and stores returned value", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string>("k")
      setResource(key, "before")
      let received: string | undefined
      const result = updateResource(key, (current) => {
        received = current
        return `${current}-after`
      })
      expect(received).toBe("before")
      expect(result).toBe("before-after")
      expect(getResource(key)).toBe("before-after")
    })
  })

  it("supports list-style accumulation across multiple calls", () => {
    const state = buildState()
    processingStateStorage.run(state, () => {
      const key = resourceKey<string[]>("list")
      updateResource(key, (current) => [...(current ?? []), "a"])
      updateResource(key, (current) => [...(current ?? []), "b"])
      expect(getResource(key)).toEqual(["a", "b"])
    })
  })
})

// ─────────────────────────────────────────────────────────────────────────
// Lifecycle accessors (CTX-03, D-30) — module-level on / onError / whenComplete /
// onPrepareCommit / onCommit / onAfterCommit. Thin wrappers over the Phase 1
// accessors. Same fail-fast contract (D-31) — throw NoActiveUnitOfWork outside
// an active processingStateStorage.run.
// ─────────────────────────────────────────────────────────────────────────

describe("Lifecycle accessors — fail-fast outside an active UoW (D-31)", () => {
  it("on throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => on(Phase.COMMIT, () => {})).toThrow(NoActiveUnitOfWork)
  })

  it("onPrepareCommit throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => onPrepareCommit(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("onCommit throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => onCommit(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("onAfterCommit throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => onAfterCommit(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("onError throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => onError(() => {})).toThrow(NoActiveUnitOfWork)
  })

  it("whenComplete throws NoActiveUnitOfWork when no UoW is active", () => {
    expect(() => whenComplete(() => {})).toThrow(NoActiveUnitOfWork)
  })
})

describe("Lifecycle accessors — registered hooks fire during their phase", () => {
  it("onPrepareCommit registers an action that runs during the PREPARE_COMMIT phase", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(() => { log.push("prepare-commit") })
      log.push("invocation")
    })
    expect(log).toEqual(["invocation", "prepare-commit"])
  })

  it("onCommit registers for COMMIT phase", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      onPrepareCommit(() => { log.push("prepare-commit") })
      onCommit(() => { log.push("commit") })
      log.push("invocation")
    })
    expect(log).toEqual(["invocation", "prepare-commit", "commit"])
  })

  it("onAfterCommit registers for AFTER_COMMIT phase", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      onCommit(() => { log.push("commit") })
      onAfterCommit(() => { log.push("after-commit") })
      log.push("invocation")
    })
    expect(log).toEqual(["invocation", "commit", "after-commit"])
  })

  it("on(Phase.COMMIT, ...) is equivalent to onCommit", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      on(Phase.COMMIT, () => { log.push("commit-via-on") })
      log.push("invocation")
    })
    expect(log).toEqual(["invocation", "commit-via-on"])
  })

  it("onError fires on phase failure", async () => {
    const log: string[] = []
    await expect(
      runInNewUoW(emptyMetadata(), async () => {
        onError(() => { log.push("error-fired") })
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(log).toEqual(["error-fired"])
  })

  it("whenComplete fires on successful completion", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      whenComplete(() => { log.push("complete") })
      log.push("invocation")
    })
    expect(log).toEqual(["invocation", "complete"])
  })

  it("whenComplete does NOT fire when the UoW errors", async () => {
    const log: string[] = []
    await expect(
      runInNewUoW(emptyMetadata(), async () => {
        whenComplete(() => { log.push("complete") })
        onError(() => { log.push("error") })
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
    expect(log).toEqual(["error"])
  })
})
