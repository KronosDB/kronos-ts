import { describe, expect, it } from "bun:test"
import { inMemoryTokenStore } from "../token-store.js"
import { globalSequenceToken, replayToken } from "../tracking-token.js"

describe("InMemoryTokenStore", () => {
  it("returns undefined for uninitialized processor", async () => {
    const store = inMemoryTokenStore()

    const token = await store.get("my-processor", 0)

    expect(token).toBeUndefined()
  })

  it("stores and retrieves GlobalSequenceToken", async () => {
    const store = inMemoryTokenStore()

    await store.store("my-processor", 0, globalSequenceToken(42n))

    const token = await store.get("my-processor", 0)
    expect(token).toBeDefined()
    expect(token!.position()).toBe(42n)
  })

  it("stores and retrieves ReplayToken", async () => {
    const store = inMemoryTokenStore()

    const replay = replayToken(globalSequenceToken(100n), globalSequenceToken(25n))
    await store.store("my-processor", 0, replay)

    const token = await store.get("my-processor", 0)
    expect(token).toBeDefined()
    expect(token!.kind).toBe("replay")
    expect(token!.position()).toBe(25n)
  })

  it("isolates positions by processor name", async () => {
    const store = inMemoryTokenStore()

    await store.store("processor-a", 0, globalSequenceToken(10n))
    await store.store("processor-b", 0, globalSequenceToken(20n))

    expect((await store.get("processor-a", 0))!.position()).toBe(10n)
    expect((await store.get("processor-b", 0))!.position()).toBe(20n)
  })

  it("isolates positions by segment", async () => {
    const store = inMemoryTokenStore()

    await store.store("my-processor", 0, globalSequenceToken(10n))
    await store.store("my-processor", 1, globalSequenceToken(20n))

    expect((await store.get("my-processor", 0))!.position()).toBe(10n)
    expect((await store.get("my-processor", 1))!.position()).toBe(20n)
  })

  it("updates existing token", async () => {
    const store = inMemoryTokenStore()

    await store.store("my-processor", 0, globalSequenceToken(10n))
    await store.store("my-processor", 0, globalSequenceToken(50n))

    expect((await store.get("my-processor", 0))!.position()).toBe(50n)
  })

  it("initializeSegments does not overwrite existing tokens", async () => {
    const store = inMemoryTokenStore()

    await store.store("my-processor", 0, globalSequenceToken(42n))
    await store.initializeSegments("my-processor", 2)

    // Segment 0 should keep its existing token
    expect((await store.get("my-processor", 0))!.position()).toBe(42n)
    // Segment 1 should be initialized as undefined
    expect(await store.get("my-processor", 1)).toBeUndefined()
  })

  it("claims and releases tokens", async () => {
    const store = inMemoryTokenStore()
    await store.initializeSegments("my-processor", 2)

    // Claim segment 0
    await store.claimToken("my-processor", 0, "instance-a")

    // Same instance can re-claim
    await store.claimToken("my-processor", 0, "instance-a")

    // Different instance cannot claim
    expect(
      store.claimToken("my-processor", 0, "instance-b"),
    ).rejects.toThrow("already claimed")

    // Release and then claim from different instance
    await store.releaseClaim("my-processor", 0, "instance-a")
    await store.claimToken("my-processor", 0, "instance-b") // Should work now
  })

  it("fetchSegments returns all initialized segments", async () => {
    const store = inMemoryTokenStore()
    await store.initializeSegments("my-processor", 4)

    const segments = await store.fetchSegments("my-processor")
    expect(segments).toEqual([0, 1, 2, 3])
  })

  it("deleteToken removes a segment", async () => {
    const store = inMemoryTokenStore()
    await store.initializeSegments("my-processor", 2)
    await store.store("my-processor", 1, globalSequenceToken(10n))

    await store.deleteToken("my-processor", 1)

    const segments = await store.fetchSegments("my-processor")
    expect(segments).toEqual([0])
  })
})
