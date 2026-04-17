import { describe, expect, it } from "bun:test"
import { emptyMetadata, resourceKey } from "@kronos-ts/common"
import { createProcessingContext } from "../default-processing-context.js"
import { Phase } from "../processing-context.js"

describe("ProcessingContext", () => {
  it("stores and retrieves typed resources", () => {
    const ctx = createProcessingContext(emptyMetadata())
    const key = resourceKey<string>("test-key")

    ctx.set(key, "hello")
    expect(ctx.get(key)).toBe("hello")
  })

  it("returns undefined for unset resources", () => {
    const ctx = createProcessingContext(emptyMetadata())
    const key = resourceKey<string>("missing")

    expect(ctx.get(key)).toBeUndefined()
  })

  it("two keys with the same label are distinct", () => {
    const ctx = createProcessingContext(emptyMetadata())
    const key1 = resourceKey<string>("same-label")
    const key2 = resourceKey<string>("same-label")

    ctx.set(key1, "first")
    ctx.set(key2, "second")

    expect(ctx.get(key1)).toBe("first")
    expect(ctx.get(key2)).toBe("second")
  })

  it("computeIfAbsent creates value on first access", () => {
    const ctx = createProcessingContext(emptyMetadata())
    const key = resourceKey<string[]>("list")

    const list = ctx.computeIfAbsent(key, () => [])
    list.push("a")

    const same = ctx.computeIfAbsent(key, () => [])
    expect(same).toEqual(["a"])
  })

  it("executes phase actions in order", async () => {
    const log: string[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.on(Phase.COMMIT, () => { log.push("commit") })
    ctx.on(Phase.PREPARE_COMMIT, () => { log.push("prepare") })
    ctx.on(Phase.AFTER_COMMIT, () => { log.push("after") })
    ctx.on(Phase.INVOCATION, () => { log.push("invoke") })
    ctx.on(Phase.PRE_INVOCATION, () => { log.push("pre") })

    await ctx.executePhases()

    expect(log).toEqual(["pre", "invoke", "prepare", "commit", "after"])
  })

  it("shorthand methods register in correct phases", async () => {
    const log: string[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.onAfterCommit(() => { log.push("after") })
    ctx.onCommit(() => { log.push("commit") })
    ctx.onPrepareCommit(() => { log.push("prepare") })

    await ctx.executePhases()

    expect(log).toEqual(["prepare", "commit", "after"])
  })

  it("picks up hooks registered during execution", async () => {
    const log: string[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.on(Phase.INVOCATION, () => {
      log.push("invoke")
      // Register a PREPARE_COMMIT hook during invocation
      ctx.onPrepareCommit(() => { log.push("prepare-from-handler") })
    })

    await ctx.executePhases()

    expect(log).toEqual(["invoke", "prepare-from-handler"])
  })

  it("runs error handlers with the error", async () => {
    const errors: unknown[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.onError((_, err) => { errors.push(err) })

    await ctx.runErrorHandlers(new Error("test error"))

    expect(errors).toHaveLength(1)
    expect((errors[0] as Error).message).toBe("test error")
  })

  it("runs completion handlers", () => {
    const log: string[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.whenComplete(() => { log.push("complete") })

    ctx.runCompleteHandlers()

    expect(log).toEqual(["complete"])
  })

  it("tracks status transitions", () => {
    const ctx = createProcessingContext(emptyMetadata())

    expect(ctx.isStarted).toBe(false)
    expect(ctx.isCompleted).toBe(false)
    expect(ctx.isError).toBe(false)

    ctx.markStarted()
    expect(ctx.isStarted).toBe(true)

    ctx.markCompleted()
    expect(ctx.isCompleted).toBe(true)
    expect(ctx.isError).toBe(false)
  })

  it("tracks error status", () => {
    const ctx = createProcessingContext(emptyMetadata())

    ctx.markStarted()
    ctx.markError()

    expect(ctx.isError).toBe(true)
    expect(ctx.isCompleted).toBe(true)
  })

  it("exposes metadata", () => {
    const ctx = createProcessingContext({ correlationId: "abc" })

    expect(ctx.metadata.correlationId).toBe("abc")
  })

  it("blocks registration in already-executed phases", async () => {
    const ctx = createProcessingContext(emptyMetadata())

    ctx.on(Phase.INVOCATION, () => {
      // Trying to register in the same phase (INVOCATION) should throw
      expect(() => {
        ctx.on(Phase.INVOCATION, () => {})
      }).toThrow("Cannot register action in phase")
    })

    await ctx.executePhases()
  })

  it("blocks registration in earlier phases", async () => {
    const ctx = createProcessingContext(emptyMetadata())

    ctx.on(Phase.COMMIT, () => {
      // Trying to register in INVOCATION (earlier) should throw
      expect(() => {
        ctx.on(Phase.INVOCATION, () => {})
      }).toThrow("Cannot register action in phase")
    })

    await ctx.executePhases()
  })

  it("allows registration in later phases during execution", async () => {
    const log: string[] = []
    const ctx = createProcessingContext(emptyMetadata())

    ctx.on(Phase.INVOCATION, () => {
      log.push("invoke")
      // Registering in later phase is fine
      ctx.onCommit(() => { log.push("commit-from-handler") })
    })

    await ctx.executePhases()

    expect(log).toEqual(["invoke", "commit-from-handler"])
  })
})
