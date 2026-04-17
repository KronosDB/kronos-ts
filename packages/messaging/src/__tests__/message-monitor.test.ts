import { describe, expect, it } from "bun:test"
import { qn, generateIdentifier, emptyMetadata } from "@kronos-ts/common"
import type { CommandMessage } from "../message.js"
import type { MessageMonitor, MonitorCallback } from "../message-monitor.js"
import { noOpMessageMonitor, multiMessageMonitor } from "../message-monitor.js"
import { createMessageMonitorRegistry } from "../message-monitor-registry.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function commandMsg(name: string): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata: emptyMetadata(),
    timestamp: Date.now(),
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("MessageMonitor", () => {
  describe("noOpMessageMonitor", () => {
    it("returns a callback that does nothing", () => {
      // given
      const monitor = noOpMessageMonitor()

      // when
      const callback = monitor.onMessageIngested(commandMsg("DoSomething"))

      // then — no errors
      callback.reportSuccess()
      callback.reportFailure(new Error("test"))
    })
  })

  describe("multiMessageMonitor", () => {
    it("calls all monitors", () => {
      // given
      const events: string[] = []
      const monitor1: MessageMonitor = {
        onMessageIngested: () => ({
          reportSuccess: () => events.push("m1-success"),
          reportFailure: () => events.push("m1-failure"),
        }),
      }
      const monitor2: MessageMonitor = {
        onMessageIngested: () => ({
          reportSuccess: () => events.push("m2-success"),
          reportFailure: () => events.push("m2-failure"),
        }),
      }

      const multi = multiMessageMonitor([monitor1, monitor2])

      // when
      const callback = multi.onMessageIngested(commandMsg("DoSomething"))
      callback.reportSuccess()

      // then
      expect(events).toEqual(["m1-success", "m2-success"])
    })

    it("returns noOp for empty list", () => {
      // given
      const multi = multiMessageMonitor([])
      const callback = multi.onMessageIngested(commandMsg("DoSomething"))

      // then — no errors
      callback.reportSuccess()
    })
  })
})

describe("MessageMonitorRegistry", () => {
  it("combines generic and typed monitors", () => {
    // given
    const events: string[] = []
    const registry = createMessageMonitorRegistry()

    registry.registerMonitor({
      onMessageIngested: () => ({
        reportSuccess: () => events.push("generic"),
        reportFailure: () => {},
      }),
    })
    registry.registerCommandMonitor({
      onMessageIngested: () => ({
        reportSuccess: () => events.push("command"),
        reportFailure: () => {},
      }),
    })

    // when
    const monitor = registry.commandMonitor()
    monitor.onMessageIngested(commandMsg("DoSomething")).reportSuccess()

    // then — both generic and command-specific fire
    expect(events).toEqual(["generic", "command"])
  })

  it("command monitor does not fire for event monitor", () => {
    // given
    const events: string[] = []
    const registry = createMessageMonitorRegistry()

    registry.registerCommandMonitor({
      onMessageIngested: () => ({
        reportSuccess: () => events.push("command"),
        reportFailure: () => {},
      }),
    })

    // when
    const monitor = registry.eventMonitor()
    monitor.onMessageIngested(commandMsg("DoSomething") as any).reportSuccess()

    // then — command monitor doesn't fire for events
    expect(events).toEqual([])
  })

  it("returns noOp when no monitors registered", () => {
    // given
    const registry = createMessageMonitorRegistry()

    // when
    const monitor = registry.commandMonitor()
    const callback = monitor.onMessageIngested(commandMsg("DoSomething"))

    // then — no errors
    callback.reportSuccess()
  })
})
