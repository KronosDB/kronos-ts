import type { Metadata } from "@kronos-ts/common"
import type { MetaDataValue } from "./generated/common.js"

/**
 * Convert framework metadata to Axon Server proto metadata format.
 */
export function metadataToProto(metadata: Metadata): Record<string, MetaDataValue> {
  const result: Record<string, MetaDataValue> = {}
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
 * Convert Axon Server proto metadata format to framework metadata.
 */
export function metadataFromProto(protoMeta: Record<string, MetaDataValue>): Metadata {
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
