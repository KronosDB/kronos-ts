import { describe, it, expect } from "bun:test"
import { kronos } from "../kronos.js"

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

describe("AppImpl native lifecycle execution (D-77)", () => {
  it("Test 1: runs start hooks in forward stage order regardless of registration order", async () => {
    const order: string[] = []
    const app = kronos({ quiet: true })
      .onStart("serve", () => { order.push("serve") })
      .onStart("processors", () => { order.push("processors") })
      .onStart("warmup", () => { order.push("warmup") })
      .onStart("register", () => { order.push("register") })
      .onStart("connect", () => { order.push("connect") })

    const running = await app.start()
    expect(order).toEqual(["connect", "warmup", "register", "processors", "serve"])
    await running.stop()
  })

  it("Test 2: runs stop hooks in reverse stage order regardless of registration order", async () => {
    const order: string[] = []
    const app = kronos({ quiet: true })
      .onStop("connect", () => { order.push("connect") })
      .onStop("warmup", () => { order.push("warmup") })
      .onStop("register", () => { order.push("register") })
      .onStop("processors", () => { order.push("processors") })
      .onStop("serve", () => { order.push("serve") })

    const running = await app.start()
    await running.stop()
    expect(order).toEqual(["serve", "processors", "register", "warmup", "connect"])
  })

  it("Test 3: runs hooks within a stage concurrently (Promise.all, not sequential)", async () => {
    const startedAt: number[] = []
    const finishedAt: number[] = []
    const app = kronos({ quiet: true })
      .onStart("connect", async () => {
        startedAt.push(Date.now())
        await sleep(50)
        finishedAt.push(Date.now())
      })
      .onStart("connect", async () => {
        startedAt.push(Date.now())
        await sleep(50)
        finishedAt.push(Date.now())
      })

    const running = await app.start()
    expect(startedAt).toHaveLength(2)
    // Both hooks should start within ~10ms of each other (concurrent)
    expect(Math.abs(startedAt[0]! - startedAt[1]!)).toBeLessThan(20)
    await running.stop()
  })

  it("Test 4: warn-then-continue when a hook exceeds stageTimeoutMs (kronos partial-config override)", async () => {
    const warnings: string[] = []
    const app = kronos({
      quiet: true,
      stageTimeoutMs: 100,
      logger: { warn: (m: string) => warnings.push(m) },
    })
      .onStart("connect", async () => {
        await sleep(300) // exceeds 100ms timeout
      })

    const t0 = Date.now()
    const running = await app.start()
    const elapsed = Date.now() - t0
    // start() resolved before the slow hook finished — within ~250ms (timeout 100ms + small overhead)
    expect(elapsed).toBeLessThan(280)
    // A warning was emitted referencing the connect stage
    expect(warnings.some((m) => m.toLowerCase().includes("connect"))).toBe(true)
    await running.stop()
  })

  it("Test 5: kronos({ stageTimeoutMs }) override warns within the configured timeout", async () => {
    const warnings: string[] = []
    const app = kronos({
      quiet: true,
      stageTimeoutMs: 50,
      logger: { warn: (m: string) => warnings.push(m) },
    })
      .onStart("connect", async () => {
        await sleep(200)
      })

    const t0 = Date.now()
    const running = await app.start()
    const elapsed = Date.now() - t0
    // The stage timeout is 50ms — start() must resolve well under 200ms
    expect(elapsed).toBeLessThan(180)
    expect(warnings.length).toBeGreaterThan(0)
    await running.stop()
  })

  it("Test 6: typed-stage hooks run in stage union order, not registration order", async () => {
    const order: string[] = []
    // Force `register` registration BEFORE `connect` registration:
    const app = kronos({ quiet: true })
      .onStart("register", () => { order.push("register") })
      .onStart("connect", () => { order.push("connect") })

    const running = await app.start()
    expect(order).toEqual(["connect", "register"])
    await running.stop()
  })
})
