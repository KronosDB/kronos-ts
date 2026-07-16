import { describe, expect, it } from "bun:test"
import type { App } from "@kronos-ts/app"
import { kronosDb } from "../kronosdb.js"

/**
 * Minimal fake App that records which slots the extension registers.
 * The extension only calls set/onStart/onStop synchronously; lifecycle
 * hooks are captured but not run, so no real connection is needed.
 */
function fakeApp() {
  const setSlots: string[] = []
  const app = {
    set(slot: string) {
      setSlots.push(slot)
    },
    onStart() {},
    onStop() {},
    processors() {
      return []
    },
  } as unknown as App
  return { app, setSlots }
}

describe("kronosDb extension slot wiring", () => {
  it("populates all four slots by default", () => {
    const { app, setSlots } = fakeApp()
    kronosDb({ componentName: "test" })(app)
    expect(setSlots.sort()).toEqual(["commandBus", "eventStore", "queryBus", "snapshotStore"])
  })

  it("populates all four slots with messaging: true", () => {
    const { app, setSlots } = fakeApp()
    kronosDb({ componentName: "test", messaging: true })(app)
    expect(setSlots.sort()).toEqual(["commandBus", "eventStore", "queryBus", "snapshotStore"])
  })

  it("populates only the store slots with messaging: false", () => {
    const { app, setSlots } = fakeApp()
    kronosDb({ componentName: "test", messaging: false })(app)
    expect(setSlots.sort()).toEqual(["eventStore", "snapshotStore"])
  })
})
