/**
 * RunningApp.eventProcessors() — the name-keyed registry a host / admin UI uses
 * to enumerate and operate processors (the AF5 EventProcessingConfiguration
 * .eventProcessors() analog). The framework ships no watchdog/auto-restart;
 * this is purely the control + status seam.
 */
import { describe, it, expect, afterEach } from "bun:test"
import { z } from "zod"
import { qn } from "@kronos-ts/common"
import {
  eventHandler,
  event,
  type EventProcessorModule,
  type TrackingEventProcessor,
} from "@kronos-ts/messaging"
import { kronos, type RunningApp } from "../index.js"

const Bumped = event({
  name: qn("reg", "Bumped"),
  payload: z.object({ id: z.string() }),
  tags: (p) => ({ id: p.id }),
})

function trackingModule(name: string): EventProcessorModule {
  return {
    kind: "tracking",
    name,
    eventHandlers: [eventHandler(Bumped, async () => {})],
  } as EventProcessorModule
}

describe("RunningApp.eventProcessors()", () => {
  let app: RunningApp | undefined
  afterEach(async () => {
    if (app) {
      await app.stop()
      app = undefined
    }
  })

  it("exposes built processors keyed by name", async () => {
    app = await kronos({ quiet: true })
      .processors(trackingModule("balances"), trackingModule("projections"))
      .start()

    const procs = app.eventProcessors()
    expect([...procs.keys()].sort()).toEqual(["balances", "projections"])
    expect(procs.get("balances")!.name).toBe("balances")
    expect(procs.get("balances")!.running).toBe(true)
  })

  it("a tracking processor in the registry reports a status() snapshot", async () => {
    app = await kronos({ quiet: true }).processors(trackingModule("balances")).start()

    const proc = app.eventProcessors().get("balances") as TrackingEventProcessor
    const status = proc.status()
    expect(status.running).toBe(true)
    expect(typeof status.position).toBe("bigint")
    expect(status.replaying).toBe(false)
  })

  it("returns an empty registry when no processors are configured", async () => {
    app = await kronos({ quiet: true }).start()
    expect(app.eventProcessors().size).toBe(0)
  })

  it("the registry can drive stop() on a processor", async () => {
    app = await kronos({ quiet: true }).processors(trackingModule("balances")).start()
    const proc = app.eventProcessors().get("balances")!
    expect(proc.running).toBe(true)
    proc.stop()
    expect(proc.running).toBe(false)
  })
})
