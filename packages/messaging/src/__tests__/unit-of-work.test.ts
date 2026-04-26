import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { runInNewUoW, runInUoW } from "../unit-of-work.js"
import {
  Phase,
  on,
  onPrepareCommit,
  onCommit,
  onAfterCommit,
  onError,
  whenComplete,
  setResource,
  getResource,
  computeIfAbsent,
  processingStateStorage,
} from "../processing-state.js"

/**
 * Plan 03-04 (CTX-04 / D-34): UnitOfWork is now a Runner — `runInUoW` /
 * `runInNewUoW`. The old `createUnitOfWork()` instance with `.on()` /
 * `.executeWithResult()` is gone. These tests assert the Runner contract:
 *
 *   - phase ordering around the action
 *   - lifecycle hooks registered via module-level accessors fire correctly
 *   - error path skips remaining phases and runs onError
 *   - whenComplete fires only on success
 *   - metadata propagates into the active state
 *   - runInUoW reuses the active UoW; runInNewUoW always nests a fresh one
 */
describe("UoWRunner — runInNewUoW", () => {
  describe("lifecycle", () => {
    it("executes action and returns its result", async () => {
      const result = await runInNewUoW(emptyMetadata(), async () => 42)
      expect(result).toBe(42)
    })

    it("executes phases in correct order around the action", async () => {
      const log: string[] = []
      await runInNewUoW(emptyMetadata(), async () => {
        on(Phase.PRE_INVOCATION, () => { log.push("pre") })
        on(Phase.COMMIT, () => { log.push("commit") })
        on(Phase.AFTER_COMMIT, () => { log.push("after") })
        log.push("handler")
      })
      // PRE_INVOCATION already past by the time the action runs in the current
      // runner — late-registered PRE_INVOCATION actions are dropped (mirrors
      // the legacy executePhases behavior). COMMIT and AFTER_COMMIT still fire.
      expect(log).toEqual(["handler", "commit", "after"])
    })

    it("supports onPrepareCommit / onCommit / onAfterCommit", async () => {
      const log: string[] = []
      await runInNewUoW(emptyMetadata(), async () => {
        log.push("handler")
        onPrepareCommit(() => { log.push("prepare") })
        onCommit(() => { log.push("commit") })
        onAfterCommit(() => { log.push("after") })
      })
      expect(log).toEqual(["handler", "prepare", "commit", "after"])
    })

    it("runs onError on failure", async () => {
      const errors: unknown[] = []
      await expect(
        runInNewUoW(emptyMetadata(), async () => {
          onError((err) => { errors.push(err) })
          throw new Error("boom")
        }),
      ).rejects.toThrow("boom")
      expect(errors).toHaveLength(1)
      expect((errors[0] as Error).message).toBe("boom")
    })

    it("runs whenComplete on success", async () => {
      const log: string[] = []
      await runInNewUoW(emptyMetadata(), async () => {
        whenComplete(() => { log.push("complete") })
      })
      expect(log).toEqual(["complete"])
    })

    it("does NOT run whenComplete on failure", async () => {
      const log: string[] = []
      await expect(
        runInNewUoW(emptyMetadata(), async () => {
          whenComplete(() => { log.push("complete") })
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual([])
    })

    it("skips remaining phases on error", async () => {
      const log: string[] = []
      await expect(
        runInNewUoW(emptyMetadata(), async () => {
          onCommit(() => { log.push("commit") })
          onAfterCommit(() => { log.push("after") })
          log.push("handler")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual(["handler"])
    })
  })

  describe("resource management", () => {
    it("handler can store and retrieve resources via module accessors", async () => {
      const key = resourceKey<string>("test")
      let retrieved: string | undefined
      await runInNewUoW(emptyMetadata(), async () => {
        setResource(key, "hello")
        retrieved = getResource(key)
      })
      expect(retrieved).toBe("hello")
    })

    it("computeIfAbsent caches across lifecycle phases", async () => {
      const key = resourceKey<string[]>("events")
      let calls = 0
      await runInNewUoW(emptyMetadata(), async () => {
        const list = computeIfAbsent(key, () => { calls++; return [] })
        list.push("a")
        onPrepareCommit(() => {
          const same = computeIfAbsent(key, () => { calls++; return [] })
          same.push("b")
        })
      })
      expect(calls).toBe(1)
    })
  })

  describe("metadata propagation", () => {
    it("passes metadata to the active processing state", async () => {
      await runInNewUoW({ correlationId: "test-123" } as any, async () => {
        const state = processingStateStorage.getStore()
        expect(state).toBeDefined()
        expect((state!.metadata as any).correlationId).toBe("test-123")
      })
    })

    it("defaults to empty metadata when undefined is passed", async () => {
      await runInNewUoW(undefined, async () => {
        const state = processingStateStorage.getStore()
        expect(state).toBeDefined()
        expect(state!.metadata).toBeDefined()
      })
    })
  })

  describe("transaction simulation via lifecycle hooks", () => {
    it("simulates commit/rollback ordering on success", async () => {
      // Note: PRE_INVOCATION/INVOCATION-phase hooks registered DURING the
      // action are dropped — the runner has already moved past those phases
      // by the time the action executes. Real transactional begin happens
      // via the runner wrapper (`transactionalUnitOfWorkFactory`), not via
      // late-registered hooks. The post-action phases (PREPARE_COMMIT,
      // COMMIT, AFTER_COMMIT) and onError still fire normally.
      const log: string[] = []
      await runInNewUoW(emptyMetadata(), async () => {
        onCommit(() => { log.push("tx:commit") })
        onError(() => { log.push("tx:rollback") })
        log.push("handler:work")
      })
      expect(log).toEqual(["handler:work", "tx:commit"])
    })

    it("rolls back on handler failure", async () => {
      const log: string[] = []
      await expect(
        runInNewUoW(emptyMetadata(), async () => {
          onCommit(() => { log.push("tx:commit") })
          onError(() => { log.push("tx:rollback") })
          log.push("handler:work")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")
      expect(log).toEqual(["handler:work", "tx:rollback"])
    })
  })
})

describe("UoWRunner — runInUoW (ALS-aware nesting)", () => {
  it("creates a fresh UoW when no state is active", async () => {
    let observedState: unknown
    await runInUoW(emptyMetadata(), async () => {
      observedState = processingStateStorage.getStore()
    })
    expect(observedState).toBeDefined()
  })

  it("reuses the active UoW when nested", async () => {
    let outer: unknown
    let inner: unknown
    await runInNewUoW(emptyMetadata(), async () => {
      outer = processingStateStorage.getStore()
      await runInUoW(emptyMetadata(), async () => {
        inner = processingStateStorage.getStore()
      })
    })
    expect(inner).toBe(outer)
  })

  it("runInNewUoW always creates a fresh state, even when nested", async () => {
    let outer: unknown
    let inner: unknown
    await runInNewUoW(emptyMetadata(), async () => {
      outer = processingStateStorage.getStore()
      await runInNewUoW(emptyMetadata(), async () => {
        inner = processingStateStorage.getStore()
      })
    })
    expect(inner).not.toBe(outer)
    expect(inner).toBeDefined()
  })
})
