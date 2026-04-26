import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { createUnitOfWork, runInUoW, runInNewUoW } from "../unit-of-work.js"
import { Phase } from "../processing-context.js"
import type { ProcessingContext } from "../processing-context.js"
import {
  processingStateStorage,
  getResource,
  setResource,
  withOverride,
} from "../processing-state.js"
import {
  transactionalUnitOfWorkFactory,
  getActiveTransaction,
} from "../transaction.js"
import { defaultUnitOfWorkFactory } from "../unit-of-work.js"

const K = resourceKey<string>("test-k")

describe("UnitOfWork + processingStateStorage", () => {
    it("has an active ALS state inside executeWithResult", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        const store = processingStateStorage.getStore()
        expect(store).toBeDefined()
      })
    })

    it("state.metadata equals the metadata passed to createUnitOfWork", async () => {
      const metadata = emptyMetadata()
      const uow = createUnitOfWork(metadata)
      await uow.executeWithResult(async () => {
        expect(processingStateStorage.getStore()?.metadata).toBe(metadata)
      })
    })

    it("state.resources is an empty Map on entry", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        const store = processingStateStorage.getStore()
        expect(store?.resources).toBeInstanceOf(Map)
        expect(store?.resources.size).toBe(0)
      })
    })

    it("state has the expected shape", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        const store = processingStateStorage.getStore()
        expect(store?.phaseActions).toBeInstanceOf(Map)
        expect(Array.isArray(store?.errorHandlers)).toBe(true)
        expect(Array.isArray(store?.completeHandlers)).toBe(true)
        expect(store?.currentPhase).toBeNull()
        expect(store?.status).toBe("not_started")
      })
    })

    it("accessors work inside executeWithResult", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        expect(setResource(K, "v")).toBeUndefined()
        expect(getResource(K)).toBe("v")
      })
    })

    it("withOverride works inside executeWithResult", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        const observed = await withOverride(K, "override", async () => getResource(K))
        expect(observed).toBe("override")
      })
    })

    it("processingStateStorage.getStore() is undefined outside executeWithResult", async () => {
      expect(processingStateStorage.getStore()).toBeUndefined()
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => {
        // inside: defined
      })
      expect(processingStateStorage.getStore()).toBeUndefined()
    })
})

describe("UnitOfWork regression — existing semantics intact", () => {
    it("returns the handler result", async () => {
      const uow = createUnitOfWork()
      const r = await uow.executeWithResult(async () => 42)
      expect(r).toBe(42)
    })

    it("propagates errors from the handler", async () => {
      const uow = createUnitOfWork()
      const boom = new Error("boom")
      await expect(
        uow.executeWithResult(async () => {
          throw boom
        }),
      ).rejects.toThrow("boom")
    })

    it("still runs phase hooks in order", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()
      uow.on(Phase.PRE_INVOCATION, () => { log.push("pre") })
      uow.on(Phase.COMMIT, () => { log.push("commit") })
      uow.on(Phase.AFTER_COMMIT, () => { log.push("after") })
      await uow.executeWithResult(async () => {
        log.push("handler")
      })
      expect(log).toEqual(["pre", "handler", "commit", "after"])
    })

    it("executed-twice guard still works", async () => {
      const uow = createUnitOfWork()
      await uow.executeWithResult(async () => 1)
      await expect(uow.executeWithResult(async () => 2)).rejects.toThrow(
        "UnitOfWork can only be executed once",
      )
    })
})

describe("runInUoW / runInNewUoW — ALS-aware UoW runners (D-32, D-33, CTX-02)", () => {
    it("runInNewUoW creates a fresh UoW and runs action inside processingStateStorage.run", async () => {
      expect(processingStateStorage.getStore()).toBeUndefined()
      let observedStore: unknown
      let observedCtx: ProcessingContext | undefined
      await runInNewUoW(emptyMetadata(), async (ctx) => {
        observedStore = processingStateStorage.getStore()
        observedCtx = ctx
      })
      expect(observedStore).toBeDefined()
      expect(observedCtx).toBeDefined()
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
      let outerCtx: ProcessingContext | undefined
      let innerCtx: ProcessingContext | undefined
      let outerStore: unknown
      let innerStore: unknown
      await runInUoW(emptyMetadata(), async (ctxOuter) => {
        outerCtx = ctxOuter
        outerStore = processingStateStorage.getStore()
        await runInUoW(emptyMetadata(), async (ctxInner) => {
          innerCtx = ctxInner
          innerStore = processingStateStorage.getStore()
        })
      })
      expect(outerCtx).toBeDefined()
      expect(innerCtx).toBeDefined()
      // Reuse — same ProcessingContext + same ALS state
      expect(innerCtx).toBe(outerCtx)
      expect(innerStore).toBe(outerStore)
    })

    it("runInNewUoW always creates a new UoW even if one is active", async () => {
      let outerCtx: ProcessingContext | undefined
      let innerCtx: ProcessingContext | undefined
      let outerStore: unknown
      let innerStore: unknown
      await runInUoW(emptyMetadata(), async (ctxOuter) => {
        outerCtx = ctxOuter
        outerStore = processingStateStorage.getStore()
        await runInNewUoW(emptyMetadata(), async (ctxInner) => {
          innerCtx = ctxInner
          innerStore = processingStateStorage.getStore()
        })
      })
      expect(outerCtx).toBeDefined()
      expect(innerCtx).toBeDefined()
      // New UoW — different ProcessingContext + different ALS state
      expect(innerCtx).not.toBe(outerCtx)
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

describe("UnitOfWork + transactionalUnitOfWorkFactory — single ALS boundary", () => {
    it("getActiveTransaction and processingStateStorage both resolve inside the handler", async () => {
      const tx = { id: "tx-1" }
      const txManager = {
        begin: async () => tx,
        commit: async () => {},
        rollback: async () => {},
      }
      const factory = transactionalUnitOfWorkFactory(defaultUnitOfWorkFactory(), txManager)
      const uow = factory()
      await uow.executeWithResult(async () => {
        expect(getActiveTransaction()).toEqual({ id: "tx-1" })
        expect(processingStateStorage.getStore()).toBeDefined()
      })
    })
})
