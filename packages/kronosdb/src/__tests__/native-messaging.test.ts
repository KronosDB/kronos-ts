import { describe, expect, it } from "bun:test"
import {
  NO_INSTRUCTIONS,
  creditWindow,
  deserializeResult,
  instructionBytes,
  metadataFromEntries,
  readMessage,
  readMetadata,
  readResult,
  requestTimeout,
  writeMessage,
  writeMetadata,
  writeResult,
} from "../native-messaging.js"
import {
  decodeCommand,
  decodeCommandResult,
  decodeProcessingInstruction,
  encodeCommand,
  encodeCommandResult,
  ProcessingKey,
  reader,
  writeU16,
  writer,
  written,
  type Command,
  type CommandResult,
} from "../protocol/kronosdb-protocol.js"

const bytes = (text: string) => new TextEncoder().encode(text)
const text = (data: Uint8Array) => new TextDecoder().decode(data)

function encode(write: (w: ReturnType<typeof writer>) => void): Uint8Array {
  const w = writer(8)
  write(w)
  return written(w).slice()
}

describe("messages on the native wire", () => {
  it("writes a message exactly as the generated codec reads it", () => {
    const body = encode((w) =>
      writeMessage(
        w,
        {
          messageId: "m-1",
          name: "orders.Place",
          timestamp: 1_761_300_000_123,
          payload: { type: "orders.Place", revision: "2", data: bytes('{"id":1}') },
          metadata: { trace: "t-1", attempt: 3, ratio: 0.5, ok: true, nested: { a: [1, 2] }, big: 5n, skipped: undefined, nothing: null },
        },
        instructionBytes({ routingKey: "order-9", priority: 2, timeoutMs: 1500 }),
        "client-7",
        "orders-service",
      ),
    )
    const command = decodeCommand(reader(body))
    expect(command).toMatchObject({
      messageId: "m-1",
      name: "orders.Place",
      timestamp: 1_761_300_000_123,
      payload: { typeName: "orders.Place", revision: "2" },
      clientId: "client-7",
      componentName: "orders-service",
    })
    expect(text(command.payload.data)).toBe('{"id":1}')
    const byKey = new Map(command.metadata.map((m) => [m.key, m.value]))
    expect(byKey.get("trace")).toEqual({ kind: "text", value: "t-1" })
    expect(byKey.get("attempt")).toEqual({ kind: "number", value: 3n })
    expect(byKey.get("ratio")).toEqual({ kind: "double", value: 0.5 })
    expect(byKey.get("ok")).toEqual({ kind: "boolean", value: true })
    expect(byKey.get("nested")).toEqual({ kind: "text", value: '{"a":[1,2]}' })
    expect(byKey.get("big")).toEqual({ kind: "number", value: 5n })
    expect(byKey.has("skipped")).toBe(false)
    expect(byKey.has("nothing")).toBe(false)
    expect(command.instructions).toEqual([
      { key: ProcessingKey.routing_key, value: { kind: "text", value: "order-9" } },
      { key: ProcessingKey.priority, value: { kind: "number", value: 2n } },
      { key: ProcessingKey.timeout, value: { kind: "number", value: 1500n } },
    ])
  })

  it("reads a command the generated codec wrote, up to what a handler needs", () => {
    const command: Command = {
      messageId: "c-1",
      name: "orders.Ship",
      timestamp: -5,
      payload: { typeName: "orders.Ship", revision: "", data: bytes("payload") },
      metadata: [
        { key: "s", value: { kind: "text", value: "héllo" } },
        { key: "n", value: { kind: "number", value: -42n } },
        { key: "d", value: { kind: "double", value: 2.25 } },
        { key: "b", value: { kind: "boolean", value: false } },
        { key: "o", value: { kind: "object", value: { typeName: "x", revision: "1", data: bytes("ignored") } } },
        { key: "__proto__", value: { kind: "text", value: "own" } },
      ],
      instructions: [{ key: ProcessingKey.routing_key, value: { kind: "text", value: "k" } }],
      clientId: "dispatcher",
      componentName: "svc",
    }
    const body = encode((w) => encodeCommand(w, command))
    const message = readMessage(reader(body))
    expect(message.messageId).toBe("c-1")
    expect(message.name).toBe("orders.Ship")
    expect(message.timestamp).toBe(-5)
    expect(message.payload.type).toBe("orders.Ship")
    expect(text(message.payload.data)).toBe("payload")
    expect(message.metadata.s).toBe("héllo")
    expect(message.metadata.n).toBe(-42)
    expect(message.metadata.d).toBe(2.25)
    expect(message.metadata.b).toBe(false)
    expect("o" in message.metadata).toBe(false)
    // A key named __proto__ is data, not a prototype.
    expect(Object.getPrototypeOf(message.metadata)).toBe(Object.prototype)
    expect(Object.getOwnPropertyDescriptor(message.metadata, "__proto__")?.value).toBe("own")
    expect(metadataFromEntries(command.metadata)).toEqual(message.metadata)
  })

  it("carries numbers the way a signed 64-bit integer does", () => {
    const roundTrip = (value: number) => {
      const body = encode((w) => writeMetadata(w, { v: value }))
      return readMetadata(reader(body)).v
    }
    for (const n of [0, 1, -1, 2 ** 31, -(2 ** 31) - 1, 2 ** 32, -(2 ** 32), Number.MAX_SAFE_INTEGER, Number.MIN_SAFE_INTEGER]) {
      expect(roundTrip(n)).toBe(n)
    }
    expect(() => encode((w) => writeMetadata(w, { v: 2 ** 70 }))).toThrow(/i64/)
    expect(() => encode((w) => writeMetadata(w, { v: 2n ** 70n }))).toThrow(/i64/)
  })

  it("encodes instructions as the codec decodes them, and none as an empty list", () => {
    expect(NO_INSTRUCTIONS).toEqual(new Uint8Array([0, 0]))
    const bytesOf = instructionBytes({ timeoutMs: 30_000, nrOfResults: 1 })
    const r = reader(bytesOf)
    const count = r.view.getUint16(0, true)
    r.pos = 2
    expect(count).toBe(2)
    expect([decodeProcessingInstruction(r), decodeProcessingInstruction(r)]).toEqual([
      { key: ProcessingKey.timeout, value: { kind: "number", value: 30_000n } },
      { key: ProcessingKey.nr_of_results, value: { kind: "number", value: 1n } },
    ])
    expect(r.pos).toBe(bytesOf.length)
  })

  it("reads a result's error code and message, or its payload, and nothing else", () => {
    const result = (over: Partial<CommandResult>): Uint8Array =>
      encode((w) =>
        encodeCommandResult(w, {
          messageId: "r-1",
          requestId: "c-1",
          errorCode: "",
          metadata: [{ key: "k", value: { kind: "text", value: "v" } }],
          instructions: [],
          ...over,
        }),
      )

    expect(readResult(reader(result({ payload: { typeName: "t", revision: "3", data: bytes("ok") } })))).toMatchObject({
      errorCode: "",
      payload: { type: "t", revision: "3" },
    })
    expect(readResult(reader(result({}))).payload).toBeUndefined()
    const failed = readResult(
      reader(result({ errorCode: "APP-1", error: { message: "no stock", location: "svc", details: ["a"], errorCode: "APP-1" } })),
    )
    expect(failed).toEqual({ errorCode: "APP-1", errorMessage: "no stock", payload: undefined })
    // An error without detail still has a code.
    expect(readResult(reader(result({ errorCode: "KRONOSDB-4005" }))).errorCode).toBe("KRONOSDB-4005")
    // An error detail on a success is skipped, not mistaken for the payload.
    expect(
      readResult(
        reader(
          result({
            error: { message: "m", location: "l", details: ["x", "y"], errorCode: "" },
            payload: { typeName: "t", revision: "", data: bytes("after") },
          }),
        ),
      ).payload?.data,
    ).toEqual(bytes("after"))
  })

  it("writes a result as the codec reads it", () => {
    const ok = decodeCommandResult(
      reader(encode((w) => writeResult(w, { requestId: "c-1", errorCode: "", errorMessage: "", location: "svc", payload: { type: "t", revision: "", data: bytes("v") } }))),
    )
    expect(ok).toMatchObject({ requestId: "c-1", errorCode: "", error: undefined, payload: { typeName: "t" } })
    const failed = decodeCommandResult(
      reader(encode((w) => writeResult(w, { requestId: "c-2", errorCode: "KRONOS-4002", errorMessage: "boom", location: "svc", payload: undefined }))),
    )
    expect(failed).toMatchObject({ requestId: "c-2", errorCode: "KRONOS-4002", error: { message: "boom", location: "svc", errorCode: "KRONOS-4002" }, payload: undefined })
  })

  it("refuses what the frame cannot hold", () => {
    expect(() => encode((w) => writeMetadata(w, { ["k".repeat(70_000)]: "v" }))).toThrow()
    const truncated = encode((w) => {
      writeU16(w, 3)
    })
    expect(() => readMetadata(reader(truncated))).toThrow(/truncated/)
  })

  it("only holds metadata entries the wire can name in a u16", () => {
    const many: Record<string, string> = {}
    for (let i = 0; i < 65_536; i++) many[`k${i}`] = "v"
    expect(() => encode((w) => writeMetadata(w, many))).toThrow(/u16/)
  })
})

describe("what both buses share", () => {
  it("validates the credit window", () => {
    expect(creditWindow(undefined)).toEqual({ permits: 5000, threshold: 2500 })
    expect(creditWindow({ permits: 1, refillThreshold: 0 })).toEqual({ permits: 1, threshold: 0 })
    expect(() => creditWindow({ permits: 0 })).toThrow(RangeError)
    expect(() => creditWindow({ permits: 4, refillThreshold: 4 })).toThrow(/refillThreshold/)
    expect(() => creditWindow({ permits: 2 ** 32 })).toThrow(/Credit frame/)
  })

  it("validates the request deadline", () => {
    expect(requestTimeout(undefined)).toBe(30000)
    expect(() => requestTimeout(0)).toThrow(RangeError)
    expect(() => requestTimeout(2 ** 31)).toThrow(/timer range/)
  })

  it("hands the serializer a payload only when there is data", () => {
    const seen: unknown[] = []
    const serializer = { serialize: () => ({ type: "", revision: "", data: new Uint8Array() }), deserialize: (v: unknown) => (seen.push(v), "value"), canConvert: () => true }
    expect(deserializeResult(serializer as never, undefined)).toBeUndefined()
    expect(deserializeResult(serializer as never, { type: "t", revision: "", data: new Uint8Array() })).toBeUndefined()
    expect(deserializeResult(serializer as never, { type: "t", revision: "", data: bytes("x") })).toBe("value")
    expect(seen).toHaveLength(1)
  })
})
