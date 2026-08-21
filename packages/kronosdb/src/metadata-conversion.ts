import type { Metadata } from "@kronos-ts/core"

/**
 * KronosDB MetadataValue type — matches the proto oneof.
 *
 * Since we don't have generated types yet, we define the shape
 * inline. Once proto codegen runs, these can be replaced with imports.
 */
export type MetadataValue = {
  textValue?: string
  numberValue?: bigint
  booleanValue?: boolean
  doubleValue?: number
  bytesValue?: { type: string; revision: string; data: Uint8Array }
}

/**
 * Convert framework metadata to KronosDB proto metadata format.
 * Used for command and query metadata (which use MetadataValue).
 */
export function metadataToProto(metadata: Metadata): Record<string, MetadataValue> {
  const result: Record<string, MetadataValue> = {}
  for (const [key, value] of Object.entries(metadata)) {
    if (typeof value === "string") {
      result[key] = { textValue: value }
    } else if (typeof value === "number") {
      if (Number.isInteger(value)) {
        result[key] = { numberValue: BigInt(value) }
      } else {
        result[key] = { doubleValue: value }
      }
    } else if (typeof value === "boolean") {
      result[key] = { booleanValue: value }
    } else if (typeof value === "bigint") {
      result[key] = { numberValue: value }
    } else if (value !== undefined && value !== null) {
      result[key] = { textValue: JSON.stringify(value) }
    }
  }
  return result
}

/**
 * Convert KronosDB proto metadata format to framework metadata.
 */
export function metadataFromProto(protoMeta: Record<string, MetadataValue>): Metadata {
  const result: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(protoMeta)) {
    if (value.textValue !== undefined) {
      result[key] = value.textValue
    } else if (value.numberValue !== undefined) {
      result[key] = Number(value.numberValue)
    } else if (value.booleanValue !== undefined) {
      result[key] = value.booleanValue
    } else if (value.doubleValue !== undefined) {
      result[key] = value.doubleValue
    }
  }
  return result
}

/**
 * Convert framework metadata to simple string map.
 * Used for event and snapshot metadata (which use map<string, string>).
 */
export function metadataToStringMap(metadata: Metadata): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [key, value] of Object.entries(metadata)) {
    if (value !== undefined && value !== null) {
      result[key] = String(value)
    }
  }
  return result
}

/**
 * Convert simple string map to framework metadata.
 */
export function metadataFromStringMap(map: Record<string, string>): Metadata {
  const result: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(map)) {
    result[key] = value
  }
  return result
}
