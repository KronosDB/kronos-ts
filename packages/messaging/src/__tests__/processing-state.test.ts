import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { Phase } from "../processing-context.js"
import {
  processingStateStorage,
  NoActiveUnitOfWork,
  getResource,
  setResource,
  computeIfAbsent,
  registerPhaseAction,
  registerErrorHandler,
  registerCompleteHandler,
  withOverride,
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
