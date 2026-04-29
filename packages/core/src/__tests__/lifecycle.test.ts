import { describe, test, expect, mock } from "bun:test"
import { kronos, AppAlreadyStartedError, type App } from "../index.js"

describe("LifecycleStage hooks", () => {
  test("Test 1: forward stage order on start() — connect → warmup → register → processors → serve", async () => {
    const order: string[] = []
    // Register in randomized order to prove sort is by stage, not registration:
    const app = kronos({ quiet: true })
      .onStart("processors", () => { order.push("processors") })
      .onStart("connect",    () => { order.push("connect") })
      .onStart("serve",      () => { order.push("serve") })
      .onStart("register",   () => { order.push("register") })
      .onStart("warmup",     () => { order.push("warmup") })

    const running = await app.start()
    expect(order).toEqual(["connect", "warmup", "register", "processors", "serve"])
    await running.stop()
  })

  test("Test 2: reverse stage order on stop() — serve → processors → register → warmup → connect", async () => {
    const order: string[] = []
    const app = kronos({ quiet: true })
      .onStop("connect",    () => { order.push("connect") })
      .onStop("processors", () => { order.push("processors") })
      .onStop("warmup",     () => { order.push("warmup") })
      .onStop("serve",      () => { order.push("serve") })
      .onStop("register",   () => { order.push("register") })

    const running = await app.start()
    await running.stop()
    expect(order).toEqual(["serve", "processors", "register", "warmup", "connect"])
  })

  test("Test 3: within-stage registration order is preserved (FIFO)", async () => {
    const order: string[] = []
    const app = kronos({ quiet: true })
      .onStart("connect", () => { order.push("a") })
      .onStart("connect", () => { order.push("b") })
      .onStart("connect", () => { order.push("c") })

    const running = await app.start()
    expect(order).toEqual(["a", "b", "c"])
    await running.stop()
  })

  test("Test 4: Promise-returning hook is awaited before next stage", async () => {
    const order: string[] = []
    const app = kronos({ quiet: true })
      .onStart("connect", async () => {
        await new Promise((r) => setTimeout(r, 10))
        order.push("connect-done")
      })
      .onStart("warmup", () => {
        order.push("warmup-start")
      })

    const running = await app.start()
    expect(order).toEqual(["connect-done", "warmup-start"])
    await running.stop()
  })

  test("Test 5: .onStart / .onStop after .start() throws AppAlreadyStartedError", async () => {
    const app = kronos({ quiet: true })
    const running = await app.start()
    expect(() => app.onStart("connect", () => {})).toThrow(AppAlreadyStartedError)
    expect(() => app.onStop("connect", () => {})).toThrow(AppAlreadyStartedError)
    await running.stop()
  })

  test("Test 6: extension-style registration via .use((app) => { app.onStart(...); app.onStop(...) })", async () => {
    const startSpy = mock(() => {})
    const stopSpy  = mock(() => {})

    const app = kronos({ quiet: true }).use((a: App) => {
      a.onStart("connect", startSpy)
      a.onStop("connect",  stopSpy)
    })

    const running = await app.start()
    expect(startSpy).toHaveBeenCalledTimes(1)
    expect(stopSpy).toHaveBeenCalledTimes(0)
    await running.stop()
    expect(stopSpy).toHaveBeenCalledTimes(1)
  })
})
