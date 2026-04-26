import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { runInUoW, runInNewUoW } from "../unit-of-work.js"
import {
  Phase,
  on,
  processingStateStorage,
  getResource,
  setResource,
  withOverride,
} from "../processing-state.js"
import {
  transactionalUnitOfWorkFactory,
  getActiveTransaction,
} from "../transaction.js"

const K = resourceKey<string>("test-k")

/**
 * Plan 03-04 (CTX-04 / D-34): the UoW interface (`createUnitOfWork`,
 * `executeWithResult`, instance `.on()` etc.) is gone. The Runner
 * (`runInUoW` / `runInNewUoW`) is the only entry point. These tests still
 * exercise the same ALS contract — module-level state, fresh resources Map
 * on entry, withOverride forking, runner result/error propagation, and
 * runInUoW / runInNewUoW nesting semantics.
 */

describe("UoWRunner + processingStateStorage", () => {
  it("has an active ALS state inside the runner action", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const store = processingStateStorage.getStore()
      expect(store).toBeDefined()
    })
  })

  it("state.metadata equals the metadata passed to the runner", async () => {
    const metadata = emptyMetadata()
    await runInNewUoW(metadata, async () => {
      expect(processingStateStorage.getStore()?.metadata).toBe(metadata)
    })
  })

  it("state.resources is an empty Map on entry", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const store = processingStateStorage.getStore()
      expect(store?.resources).toBeInstanceOf(Map)
      expect(store?.resources.size).toBe(0)
    })
  })

  it("state has the expected shape", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const store = processingStateStorage.getStore()
      expect(store?.phaseActions).toBeInstanceOf(Map)
      expect(Array.isArray(store?.errorHandlers)).toBe(true)
      expect(Array.isArray(store?.completeHandlers)).toBe(true)
      expect(store?.status).toBeDefined()
    })
  })

  it("module-level accessors work inside the runner", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      expect(setResource(K, "v")).toBeUndefined()
      expect(getResource(K)).toBe("v")
    })
  })

  it("withOverride works inside the runner", async () => {
    await runInNewUoW(emptyMetadata(), async () => {
      const observed = await withOverride(K, "override", async () => getResource(K))
      expect(observed).toBe("override")
    })
  })

  it("processingStateStorage.getStore() is undefined outside the runner", async () => {
    expect(processingStateStorage.getStore()).toBeUndefined()
    await runInNewUoW(emptyMetadata(), async () => {
      // inside: defined
    })
    expect(processingStateStorage.getStore()).toBeUndefined()
  })
})

describe("UoWRunner regression — runner semantics", () => {
  it("returns the action's result", async () => {
    const r = await runInNewUoW(emptyMetadata(), async () => 42)
    expect(r).toBe(42)
  })

  it("propagates errors from the action", async () => {
    await expect(
      runInNewUoW(emptyMetadata(), async () => {
        throw new Error("boom")
      }),
    ).rejects.toThrow("boom")
  })

  it("still runs phase hooks in order", async () => {
    const log: string[] = []
    await runInNewUoW(emptyMetadata(), async () => {
      // PRE_INVOCATION already past — late-registered hooks for it are
      // dropped, mirroring the legacy executePhases contract.
      on(Phase.COMMIT, () => { log.push("commit") })
      on(Phase.AFTER_COMMIT, () => { log.push("after") })
      log.push("handler")
    })
    expect(log).toEqual(["handler", "commit", "after"])
  })
})

describe("runInUoW / runInNewUoW — ALS-aware UoW runners (D-32, D-33, CTX-02)", () => {
  it("runInNewUoW creates a fresh UoW and runs action inside processingStateStorage.run", async () => {
    expect(processingStateStorage.getStore()).toBeUndefined()
    let observedStore: unknown
    await runInNewUoW(emptyMetadata(), async () => {
      observedStore = processingStateStorage.getStore()
    })
    expect(observedStore).toBeDefined()
    expect(processingStateStorage.getStore()).toBeUndefined()
  })

  it("runInUoW creates a new UoW when none is active", async () => {
    expect(processingStateStorage.getStore()).toBeUndefined()
    let observedStore: unknown
    await runInUoW(emptyMetadata(), async () => {
      observedStore = processingStateStorage.getStore()
    })
    expect(observedStore).toBeDefined()
  })

  it("runInUoW reuses the active UoW when one is already on the stack", async () => {
    let outerStore: unknown
    let innerStore: unknown
    await runInUoW(emptyMetadata(), async () => {
      outerStore = processingStateStorage.getStore()
      await runInUoW(emptyMetadata(), async () => {
        innerStore = processingStateStorage.getStore()
      })
    })
    expect(outerStore).toBeDefined()
    expect(innerStore).toBe(outerStore)
  })

  it("runInNewUoW always creates a new UoW even if one is active", async () => {
    let outerStore: unknown
    let innerStore: unknown
    await runInUoW(emptyMetadata(), async () => {
      outerStore = processingStateStorage.getStore()
      await runInNewUoW(emptyMetadata(), async () => {
        innerStore = processingStateStorage.getStore()
      })
    })
    expect(outerStore).toBeDefined()
    expect(innerStore).toBeDefined()
    expect(innerStore).not.toBe(outerStore)
  })

  it("runInUoW returns the action's result", async () => {
    const r = await runInUoW(emptyMetadata(), async () => 7)
    expect(r).toBe(7)
  })

  it("runInNewUoW returns the action's result", async () => {
    const r = await runInNewUoW(emptyMetadata(), async () => "hello")
    expect(r).toBe("hello")
  })

  it("runInUoW propagates errors from the action", async () => {
    await expect(
      runInUoW(emptyMetadata(), async () => {
        throw new Error("boom-runInUoW")
      }),
    ).rejects.toThrow("boom-runInUoW")
  })

  it("runInNewUoW propagates errors from the action", async () => {
    await expect(
      runInNewUoW(emptyMetadata(), async () => {
        throw new Error("boom-runInNewUoW")
      }),
    ).rejects.toThrow("boom-runInNewUoW")
  })

  it("nested runInUoW resources are visible to inner action (single state)", async () => {
    await runInUoW(emptyMetadata(), async () => {
      setResource(K, "outer-value")
      await runInUoW(emptyMetadata(), async () => {
        expect(getResource(K)).toBe("outer-value")
      })
    })
  })
})

describe("transactionalUnitOfWorkFactory — composable runner wrapper", () => {
  it("getActiveTransaction and processingStateStorage both resolve inside the handler", async () => {
    const tx = { id: "tx-1" }
    const txManager = {
      begin: async () => tx,
      commit: async () => {},
      rollback: async () => {},
    }
    const txRunner = transactionalUnitOfWorkFactory(runInUoW, txManager)
    await txRunner(emptyMetadata(), async () => {
      expect(getActiveTransaction()).toEqual({ id: "tx-1" })
      expect(processingStateStorage.getStore()).toBeDefined()
    })
  })
})
