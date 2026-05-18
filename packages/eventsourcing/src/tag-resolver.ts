import type { Tag } from "@kronos-ts/common"
import type { EventMessage } from "@kronos-ts/messaging"

/**
 * Resolves tags from an event message. Tags are metadata markers attached
 * to events for filtering, categorization, and criteria-based sourcing.
 *
 * By default, tags are derived from the event descriptor's `tags` function
 * at event creation time. The TagResolver runs before storage and can enrich
 * events with additional tags from metadata, context, etc.
 */
export interface TagResolver {
  resolve(event: EventMessage): Tag[]
}

/**
 * Default tag resolver — passes through tags already on the event.
 *
 * Events are created with descriptor-derived tags. This resolver simply
 * returns those existing tags unchanged.
 */
export function descriptorBasedTagResolver(): TagResolver {
  return {
    resolve(event: EventMessage): Tag[] {
      return [...event.tags]
    },
  }
}

/**
 * Resolves additional tags from event metadata. For each configured key,
 * if the metadata contains that key, a tag is created.
 */
export function metadataBasedTagResolver(...metadataKeys: string[]): TagResolver {
  return {
    resolve(event: EventMessage): Tag[] {
      const tags: Tag[] = []
      for (const key of metadataKeys) {
        const value = event.metadata[key]
        if (value != null) {
          tags.push({ key, value: String(value) })
        }
      }
      return tags
    },
  }
}

/**
 * Combines multiple tag resolvers. Tags from all resolvers are merged.
 */
export function multiTagResolver(...resolvers: TagResolver[]): TagResolver {
  return {
    resolve(event: EventMessage): Tag[] {
      const tags: Tag[] = []
      for (const resolver of resolvers) {
        tags.push(...resolver.resolve(event))
      }
      return tags
    },
  }
}
