import { describe, expect, it } from "bun:test"
import { qn } from "../../primitives/qualified-name.js"
import { generateIdentifier } from "../../primitives/identifier.js"
import { emptyMetadata } from "../../primitives/metadata.js"
import { tag } from "../../primitives/tag.js"
import type { EventMessage } from "../../messages/message.js"
import { descriptorBasedTagResolver, metadataBasedTagResolver, multiTagResolver } from "../tag-resolver.js"

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function eventMsg(
  name: string,
  tags: Array<{ key: string; value: string }> = [],
  metadata: Record<string, unknown> = {},
): EventMessage {
  return {
    identifier: generateIdentifier(),
    name: qn("test", name),
    payload: {},
    metadata,
    timestamp: Date.now(),
    version: "1.0",
    tags,
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("TagResolver", () => {
  describe("descriptorBasedTagResolver", () => {
    it("passes through existing descriptor-derived tags", () => {
      // given
      const resolver = descriptorBasedTagResolver()
      const ev = eventMsg("Created", [tag("entityId", "e-1"), tag("type", "course")])

      // when
      const resolved = resolver(ev)

      // then
      expect(resolved).toEqual([
        { key: "entityId", value: "e-1" },
        { key: "type", value: "course" },
      ])
    })

    it("returns empty for events without tags", () => {
      // given
      const resolver = descriptorBasedTagResolver()
      const ev = eventMsg("Created")

      // when / then
      expect(resolver(ev)).toEqual([])
    })
  })

  describe("metadataBasedTagResolver", () => {
    it("extracts tags from metadata keys", () => {
      // given
      const resolver = metadataBasedTagResolver("tenantId", "userId")
      const ev = eventMsg("Created", [], { tenantId: "t-1", userId: "u-42", other: "ignored" })

      // when
      const resolved = resolver(ev)

      // then
      expect(resolved).toEqual([
        { key: "tenantId", value: "t-1" },
        { key: "userId", value: "u-42" },
      ])
    })

    it("skips missing metadata keys", () => {
      // given
      const resolver = metadataBasedTagResolver("tenantId", "missing")
      const ev = eventMsg("Created", [], { tenantId: "t-1" })

      // when / then
      expect(resolver(ev)).toEqual([{ key: "tenantId", value: "t-1" }])
    })
  })

  describe("multiTagResolver", () => {
    it("combines tags from multiple resolvers", () => {
      // given
      const resolver = multiTagResolver(
        descriptorBasedTagResolver(),
        metadataBasedTagResolver("tenantId"),
      )
      const ev = eventMsg("Created", [tag("entityId", "e-1")], { tenantId: "t-1" })

      // when
      const resolved = resolver(ev)

      // then
      expect(resolved).toEqual([
        { key: "entityId", value: "e-1" },
        { key: "tenantId", value: "t-1" },
      ])
    })
  })
})
