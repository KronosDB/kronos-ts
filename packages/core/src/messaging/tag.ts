
/**
 * A key-value tag attached to events for query-based sourcing.
 * Tags are the primary mechanism for defining sourcing and append conditions
 * in a Dynamic Consistency Boundary (DCB) model.
 */
export type Tag = {
  readonly key: string
  readonly value: string
}

/**
 * Creates a tag with the given key and value.
 */
export function tag(key: string, value: string): Tag {
  if (!key) throw new Error("Tag key must not be empty")
  if (!value) throw new Error("Tag value must not be empty")
  return { key, value }
}

/**
 * Converts a record of key-value pairs to an array of Tags.
 *
 * ```typescript
 * tagsFromRecord({ courseId: "cs-101", studentId: "stu-1" })
 * // → [{ key: "courseId", value: "cs-101" }, { key: "studentId", value: "stu-1" }]
 * ```
 */
export function tagsFromRecord(record: Record<string, string>): Tag[] {
  return Object.entries(record).map(([key, value]) => tag(key, value))
}

