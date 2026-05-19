import { describe, it, expect, afterEach } from "bun:test"
import { createWarningChannel } from "../warnings.js"

describe("WarningChannel", () => {
  const warnCalls: string[] = []
  const originalWarn = console.warn

  afterEach(() => {
    console.warn = originalWarn
    warnCalls.length = 0
  })

  it("Test 1: Default createWarningChannel() — emit('hi') calls console.warn exactly once with 'hi'", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const channel = createWarningChannel()
    channel.emit("hi")
    expect(warnCalls).toHaveLength(1)
    expect(warnCalls[0]).toBe("hi")
  })

  it("Test 2: createWarningChannel({ quiet: true }) — emit('hi') produces ZERO console.warn calls", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const channel = createWarningChannel({ quiet: true })
    channel.emit("hi")
    expect(warnCalls).toHaveLength(0)
  })

  it("Test 3: createWarningChannel({ logger: { warn: spy } }) — emit('hi') calls spy('hi') once and does NOT call console.warn", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const spyCalls: string[] = []
    const channel = createWarningChannel({ logger: { warn: (msg: string) => spyCalls.push(msg) } })
    channel.emit("hi")
    expect(spyCalls).toHaveLength(1)
    expect(spyCalls[0]).toBe("hi")
    expect(warnCalls).toHaveLength(0) // console.warn NOT called
  })

  it("Test 4: createWarningChannel({ quiet: true, logger: spyLogger }) — quiet wins; ZERO calls to either channel", () => {
    console.warn = (msg: string) => warnCalls.push(msg)
    const spyCalls: string[] = []
    const channel = createWarningChannel({
      quiet: true,
      logger: { warn: (msg: string) => spyCalls.push(msg) },
    })
    channel.emit("hi")
    expect(warnCalls).toHaveLength(0)
    expect(spyCalls).toHaveLength(0)
  })
})
