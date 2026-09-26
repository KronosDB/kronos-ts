/**
 * `loggingHandler(next, logger)` — supplies `ctx.log`, a logger already
 * carrying this invocation's message identity. Same shape as `postgresHandler`:
 * a plain function over a plain function, and the two things worth pinning
 * are what it puts on `log` and that it changes nothing else about `next`.
 */
import { describe, expect, it } from "bun:test"
import { qn, type Message } from "../../messaging/messages.js"
import type { LogFields, LogLevel, Logger } from "../logger.js"
import { loggingHandler, messageFields } from "../logging-handler.js"

function recordingLogger(): { logger: Logger; recorded: Array<{ level: LogLevel; message: string; fields?: LogFields }> } {
  const recorded: Array<{ level: LogLevel; message: string; fields?: LogFields }> = []
  function make(carried: LogFields): Logger {
    return {
      debug: (message, fields) => recorded.push({ level: "debug", message, fields: { ...carried, ...fields } }),
      info: (message, fields) => recorded.push({ level: "info", message, fields: { ...carried, ...fields } }),
      warn: (message, fields) => recorded.push({ level: "warn", message, fields: { ...carried, ...fields } }),
      error: (message, fields) => recorded.push({ level: "error", message, fields: { ...carried, ...fields } }),
      with: (fields) => make({ ...carried, ...fields }),
    }
  }
  return { logger: make({}), recorded }
}

function messageWith(metadata: Message["metadata"]): Message {
  return {
    kind: "command",
    identifier: "msg-1",
    name: qn("billing", "RecordCharge"),
    payload: { accountId: "a-1" },
    metadata,
  }
}

describe("messageFields", () => {
  it("carries the message's identity plus correlation ids when present", () => {
    const message = messageWith({ correlationId: "corr-1", causationId: "cause-1" })
    expect(messageFields(message)).toEqual({
      "message.name": "billing.RecordCharge",
      "message.id": "msg-1",
      "message.kind": "command",
      correlationId: "corr-1",
      causationId: "cause-1",
    })
  })

  it("includes trace_id/span_id when traceparent is a valid W3C header", () => {
    const message = messageWith({
      traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    })
    const fields = messageFields(message)
    expect(fields.trace_id).toBe("0af7651916cd43dd8448eb211c80319c")
    expect(fields.span_id).toBe("b7ad6b7169203331")
  })

  it("omits trace ids when traceparent is absent", () => {
    const fields = messageFields(messageWith({}))
    expect(fields.trace_id).toBeUndefined()
    expect(fields.span_id).toBeUndefined()
  })

  it("omits trace ids when traceparent is malformed", () => {
    const fields = messageFields(messageWith({ traceparent: "not-a-traceparent" }))
    expect(fields.trace_id).toBeUndefined()
    expect(fields.span_id).toBeUndefined()
  })
})

describe("loggingHandler", () => {
  it("supplies ctx.log carrying the message fields to every leveled call", () => {
    const { logger, recorded } = recordingLogger()
    const message = messageWith({ correlationId: "corr-1", causationId: "cause-1" })

    const wrapped = loggingHandler((_m: Message, ctx: { log: Logger }) => {
      ctx.log.info("handling", { extra: 1 })
      return "done"
    }, logger)

    const result = wrapped(message, {})

    expect(result).toBe("done")
    expect(recorded).toEqual([
      {
        level: "info",
        message: "handling",
        fields: {
          "message.name": "billing.RecordCharge",
          "message.id": "msg-1",
          "message.kind": "command",
          correlationId: "corr-1",
          causationId: "cause-1",
          extra: 1,
        },
      },
    ])
  })

  it("does not change a synchronous handler's return value or sync-ness", () => {
    const { logger } = recordingLogger()
    const wrapped = loggingHandler((_m: Message, _ctx: { log: Logger }) => 42, logger)
    const result = wrapped(messageWith({}), {})
    expect(result).toBe(42)
    expect(result).not.toBeInstanceOf(Promise)
  })

  it("does not change an asynchronous handler's return value or async-ness", async () => {
    const { logger } = recordingLogger()
    const wrapped = loggingHandler(async (_m: Message, _ctx: { log: Logger }) => "async-done", logger)
    const result = wrapped(messageWith({}), {})
    expect(result).toBeInstanceOf(Promise)
    expect(await result).toBe("async-done")
  })
})
