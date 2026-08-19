import { describe, expect, it } from "bun:test"
import { parseInstruction } from "../platform-service.js"

describe("parseInstruction", () => {
  describe("TopologyNotification (KDB-04 — new oneof arm field 11)", () => {
    it("returns null without throwing for a topologyNotification arm", () => {
      const message = {
        topologyNotification: {
          changeType: "ADD",
          messageType: "command",
          handlerKind: "h",
          clientId: "c",
          componentName: "comp",
        },
      }
      expect(parseInstruction(message)).toBeNull()
    })

    it("returns null for an empty message (sanity)", () => {
      expect(parseInstruction({})).toBeNull()
    })
  })

  describe("known instructions (regression lock)", () => {
    it("decodes pauseEventProcessor", () => {
      expect(parseInstruction({ pauseEventProcessor: { processorName: "proc-x" } }))
        .toEqual({ kind: "pause-processor", processorName: "proc-x" })
    })

    it("decodes startEventProcessor", () => {
      expect(parseInstruction({ startEventProcessor: { processorName: "proc-y" } }))
        .toEqual({ kind: "start-processor", processorName: "proc-y" })
    })

    it("decodes releaseSegment", () => {
      expect(parseInstruction({ releaseSegment: { processorName: "p", segmentIdentifier: 7 } }))
        .toEqual({ kind: "release-segment", processorName: "p", segmentId: 7 })
    })

    it("decodes splitEventProcessorSegment", () => {
      expect(parseInstruction({ splitEventProcessorSegment: { processorName: "p", segmentIdentifier: 8 } }))
        .toEqual({ kind: "split-segment", processorName: "p", segmentId: 8 })
    })

    it("decodes mergeEventProcessorSegment", () => {
      expect(parseInstruction({ mergeEventProcessorSegment: { processorName: "p", segmentIdentifier: 9 } }))
        .toEqual({ kind: "merge-segment", processorName: "p", segmentId: 9 })
    })

    it("decodes requestReconnect", () => {
      expect(parseInstruction({ requestReconnect: {} }))
        .toEqual({ kind: "reconnect-request" })
    })
  })

  describe("priority order documentation", () => {
    // The source code checks fields in this order:
    //   requestReconnect → pauseEventProcessor → startEventProcessor →
    //   releaseSegment → splitEventProcessorSegment → mergeEventProcessorSegment → return null
    // topologyNotification has no branch — co-occurrence with a known field
    // means the known field wins.
    it("when topologyNotification co-occurs with a known instruction, the known instruction wins (topologyNotification has no branch)", () => {
      const message = {
        topologyNotification: { changeType: "ADD", messageType: "x", handlerKind: "y", clientId: "z", componentName: "c" },
        pauseEventProcessor: { processorName: "winner" },
      }
      expect(parseInstruction(message)).toEqual({ kind: "pause-processor", processorName: "winner" })
    })
  })
})
