---
"@kronos-ts/messaging": patch
---

Label persisted tokens with PascalCase `token_type` values (`GlobalSequenceToken` / `GapAwareToken`).

The token serializer wrote the kebab-case token kind into the `token_type` column, leaving `gap-aware` sitting next to legacy `GlobalSequenceToken` rows. The value is informational — deserialization keys off the `gapKey` in the body, not the type string — so this only affects the readability of the token table.
