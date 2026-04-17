import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { createUnitOfWork } from "../unit-of-work.js"
import { Phase } from "../processing-context.js"

describe("UnitOfWork", () => {
  describe("lifecycle", () => {
    it("executes action and returns result", async () => {
      const uow = createUnitOfWork()

      const result = await uow.executeWithResult(async () => {
        return 42
      })

      expect(result).toBe(42)
    })

    it("executes phases in correct order around the action", async () => {
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

    it("handler can register lifecycle hooks via ProcessingContext", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      await uow.executeWithResult(async (ctx) => {
        log.push("handler")
        ctx.onPrepareCommit(() => { log.push("prepare") })
        ctx.onCommit(() => { log.push("commit") })
        ctx.onAfterCommit(() => { log.push("after") })
      })

      expect(log).toEqual(["handler", "prepare", "commit", "after"])
    })

    it("runs error handlers on failure", async () => {
      const errors: unknown[] = []
      const uow = createUnitOfWork()

      uow.onError((_, err) => { errors.push(err) })

      await expect(
        uow.executeWithResult(async () => {
          throw new Error("boom")
        }),
      ).rejects.toThrow("boom")

      expect(errors).toHaveLength(1)
      expect((errors[0] as Error).message).toBe("boom")
    })

    it("runs whenComplete on success", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      uow.whenComplete(() => { log.push("complete") })

      await uow.executeWithResult(async () => {})

      expect(log).toEqual(["complete"])
    })

    it("does NOT run whenComplete on failure", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      uow.whenComplete(() => { log.push("complete") })

      await expect(
        uow.executeWithResult(async () => { throw new Error("fail") }),
      ).rejects.toThrow("fail")

      expect(log).toEqual([])
    })

    it("skips remaining phases on error", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      uow.on(Phase.COMMIT, () => { log.push("commit") })
      uow.on(Phase.AFTER_COMMIT, () => { log.push("after") })

      await expect(
        uow.executeWithResult(async () => {
          log.push("handler")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")

      // COMMIT and AFTER_COMMIT should NOT have run
      expect(log).toEqual(["handler"])
    })

    it("cannot execute twice", async () => {
      const uow = createUnitOfWork()

      await uow.executeWithResult(async () => {})

      await expect(
        uow.executeWithResult(async () => {}),
      ).rejects.toThrow("UnitOfWork can only be executed once")
    })

    it("cannot register hooks after execution starts", async () => {
      const uow = createUnitOfWork()

      await uow.executeWithResult(async () => {})

      expect(() => uow.on(Phase.COMMIT, () => {})).toThrow(
        "Cannot register hooks after execution has started",
      )
    })
  })

  describe("resource management", () => {
    it("handler can store and retrieve resources", async () => {
      const uow = createUnitOfWork()
      const key = resourceKey<string>("test")

      let retrieved: string | undefined

      await uow.executeWithResult(async (ctx) => {
        ctx.set(key, "hello")
        retrieved = ctx.get(key)
      })

      expect(retrieved).toBe("hello")
    })

    it("computeIfAbsent caches across lifecycle phases", async () => {
      const uow = createUnitOfWork()
      const key = resourceKey<string[]>("events")
      let calls = 0

      await uow.executeWithResult(async (ctx) => {
        const list = ctx.computeIfAbsent(key, () => { calls++; return [] })
        list.push("a")

        ctx.onPrepareCommit((c) => {
          const same = c.computeIfAbsent(key, () => { calls++; return [] })
          same.push("b")
        })
      })

      // Supplier should only be called once
      expect(calls).toBe(1)
    })
  })

  describe("metadata propagation", () => {
    it("passes metadata to ProcessingContext", async () => {
      const uow = createUnitOfWork({ correlationId: "test-123" })

      await uow.executeWithResult(async (ctx) => {
        expect(ctx.metadata.correlationId).toBe("test-123")
      })
    })

    it("defaults to empty metadata", async () => {
      const uow = createUnitOfWork()

      await uow.executeWithResult(async (ctx) => {
        expect(ctx.metadata).toBeDefined()
      })
    })
  })

  describe("transaction simulation", () => {
    it("simulates begin/commit/rollback via lifecycle hooks", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      // Simulate TransactionManager attachment
      uow.on(Phase.PRE_INVOCATION, () => { log.push("tx:begin") })
      uow.on(Phase.COMMIT, () => { log.push("tx:commit") })
      uow.onError(() => { log.push("tx:rollback") })

      await uow.executeWithResult(async () => {
        log.push("handler:work")
      })

      expect(log).toEqual(["tx:begin", "handler:work", "tx:commit"])
    })

    it("rolls back on handler failure", async () => {
      const log: string[] = []
      const uow = createUnitOfWork()

      uow.on(Phase.PRE_INVOCATION, () => { log.push("tx:begin") })
      uow.on(Phase.COMMIT, () => { log.push("tx:commit") })
      uow.onError(() => { log.push("tx:rollback") })

      await expect(
        uow.executeWithResult(async () => {
          log.push("handler:work")
          throw new Error("fail")
        }),
      ).rejects.toThrow("fail")

      expect(log).toEqual(["tx:begin", "handler:work", "tx:rollback"])
    })
  })
})
