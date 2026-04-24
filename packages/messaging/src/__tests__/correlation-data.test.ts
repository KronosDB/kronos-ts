import { describe, expect, it } from "bun:test"
import { qn, emptyMetadata, generateIdentifier } from "@kronos-ts/common"
import type { CommandMessage } from "../message.js"
import {
  type CorrelationDataProvider,
  messageOriginProvider,
  simpleCorrelationDataProvider,
  correlationDataHandlerInterceptor,
  getActiveCorrelationData,
} from "../correlation-data.js"
import { createProcessingContext } from "../default-processing-context.js"
import { processingStateStorage, createInitialProcessingState } from "../processing-state.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function commandMsg(name: string, metadata: Record<string, unknown> = {}): CommandMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata,
    timestamp: Date.now(),
  }
}

/**
 * Run `fn` inside an active processingStateStorage.run() — required after Plan 03
 * because the handler interceptor now writes via the module-level `setResource`
 * (D-20 fail-fast contract). Same pattern Plans 02-01 / 02-02 introduced for
 * processing-context / replay-token / subscription-query / command-handling-module
 * test files.
 */
function inUoW<R>(fn: () => R | Promise<R>): Promise<R> {
  return processingStateStorage.run(createInitialProcessingState(emptyMetadata()), async () => fn())
}

// ---------------------------------------------------------------------------
// messageOriginProvider
// ---------------------------------------------------------------------------

describe("messageOriginProvider", () => {
  const provider = messageOriginProvider()

  it("sets correlationId to message identifier when no existing correlationId", () => {
    const msg = commandMsg("DoSomething")
    const data = provider.correlationDataFor(msg)
    expect(data.correlationId).toBe(msg.identifier)
  })

  it("preserves existing correlationId from metadata", () => {
    const msg = commandMsg("DoSomething", { correlationId: "original-chain" })
    const data = provider.correlationDataFor(msg)
    expect(data.correlationId).toBe("original-chain")
  })

  it("sets causationId to current message identifier", () => {
    const msg = commandMsg("DoSomething", { correlationId: "original-chain" })
    const data = provider.correlationDataFor(msg)
    expect(data.causationId).toBe(msg.identifier)
  })

  it("supports custom key names", () => {
    const provider = messageOriginProvider("myCorrelation", "myCausation")
    const msg = commandMsg("DoSomething")
    const data = provider.correlationDataFor(msg)
    expect(data.myCorrelation).toBe(msg.identifier)
    expect(data.myCausation).toBe(msg.identifier)
  })
})

// ---------------------------------------------------------------------------
// simpleCorrelationDataProvider
// ---------------------------------------------------------------------------

describe("simpleCorrelationDataProvider", () => {
  it("copies specified metadata keys", () => {
    const provider = simpleCorrelationDataProvider("tenantId", "userId")
    const msg = commandMsg("DoSomething", { tenantId: "t-1", userId: "u-1", other: "ignored" })
    const data = provider.correlationDataFor(msg)
    expect(data).toEqual({ tenantId: "t-1", userId: "u-1" })
  })

  it("silently ignores missing keys", () => {
    const provider = simpleCorrelationDataProvider("tenantId", "missing")
    const msg = commandMsg("DoSomething", { tenantId: "t-1" })
    const data = provider.correlationDataFor(msg)
    expect(data).toEqual({ tenantId: "t-1" })
  })

  it("returns empty map when no keys match", () => {
    const provider = simpleCorrelationDataProvider("nothing")
    const msg = commandMsg("DoSomething")
    const data = provider.correlationDataFor(msg)
    expect(data).toEqual({})
  })
})

// ---------------------------------------------------------------------------
// Handler interceptor + ProcessingContext
// ---------------------------------------------------------------------------

describe("correlationDataHandlerInterceptor", () => {
  it("stores correlation data in ProcessingContext", async () => {
    await inUoW(async () => {
      // given
      const providers: CorrelationDataProvider[] = [messageOriginProvider()]
      const handlerInterceptor = correlationDataHandlerInterceptor(providers)
      const msg = commandMsg("IncomingCommand")
      const ctx = createProcessingContext(emptyMetadata())

      // when
      await handlerInterceptor(msg, ctx, async () => undefined)

      // then — correlation data stored in ALS-backed state, visible via ctx.get shim
      const data = getActiveCorrelationData(ctx)
      expect(data).toBeDefined()
      expect(data!.correlationId).toBe(msg.identifier)
      expect(data!.causationId).toBe(msg.identifier)
    })
  })

  it("catches and logs provider exceptions", async () => {
    await inUoW(async () => {
      // given
      const failingProvider: CorrelationDataProvider = {
        correlationDataFor: () => { throw new Error("Provider failure") },
      }
      const workingProvider = simpleCorrelationDataProvider("tenantId")
      const handlerInterceptor = correlationDataHandlerInterceptor([failingProvider, workingProvider])
      const msg = commandMsg("DoSomething", { tenantId: "t-1" })
      const ctx = createProcessingContext(emptyMetadata())
      let handlerCalled = false

      // when
      await handlerInterceptor(msg, ctx, async () => {
        handlerCalled = true
        return undefined
      })

      // then — handler still called, working provider's data present
      expect(handlerCalled).toBe(true)
      const data = getActiveCorrelationData(ctx)
      expect(data?.tenantId).toBe("t-1")
    })
  })

  it("later providers override earlier ones on key conflicts", async () => {
    await inUoW(async () => {
      // given
      const provider1: CorrelationDataProvider = {
        correlationDataFor: () => ({ key: "first", unique1: "a" }),
      }
      const provider2: CorrelationDataProvider = {
        correlationDataFor: () => ({ key: "second", unique2: "b" }),
      }
      const handlerInterceptor = correlationDataHandlerInterceptor([provider1, provider2])
      const msg = commandMsg("DoSomething")
      const ctx = createProcessingContext(emptyMetadata())

      // when
      await handlerInterceptor(msg, ctx, async () => undefined)

      // then
      const data = getActiveCorrelationData(ctx)
      expect(data?.key).toBe("second")
      expect(data?.unique1).toBe("a")
      expect(data?.unique2).toBe("b")
    })
  })
})
